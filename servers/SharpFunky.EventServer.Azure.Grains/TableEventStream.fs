namespace SharpFunky.EventServer.Azure.Grains

open System 
open Orleans 
open FSharp.Control.Tasks
open SharpFunky
open SharpFunky.EventServer.Interfaces
open System.Threading.Tasks
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

module internal Constants =
    [<Literal>]
    let StatusRowKey = "A_STATUS"
    [<Literal>]
    let RowKeyPrefix = "B_"
    [<Literal>]
    let MetaPrefix = "Meta_"
    [<Literal>]
    let DataPrefix = "Data_"
    [<Literal>]
    let NextSequenceKey = "NextSequence"
    [<Literal>]
    let PartitionKey = "PartitionKey"
    [<Literal>]
    let RowKey = "RowKey"
    [<Literal>]
    let BinaryChunkSize = 65536
    [<Literal>]
    let StringChunkSize = 32768
    [<Literal>]
    let MaxTableQueryCount = 1000
    [<Literal>]
    let MaxTableBatchSize = 100
    

    let forLong name value (entity: DynamicTableEntity) =
        let prop = value |> Nullable |> EntityProperty.GeneratePropertyForLong
        entity.Properties.Add(name, prop)
        entity

    let forString name value (entity: DynamicTableEntity) =
        let prop = value |> EntityProperty.GeneratePropertyForString
        entity.Properties.Add(name, prop)
        entity

    let forByteArray name value (entity: DynamicTableEntity) =
        let prop = value |> EntityProperty.GeneratePropertyForByteArray
        entity.Properties.Add(name, prop)
        entity

    let sequenceToRowKey (sequence: int64) = sprintf "%s%020d" RowKeyPrefix sequence
    let sequenceOfRowKey rowKey =
        String.substringFrom RowKeyPrefix.Length rowKey
        |> String.trimStartWith '0'
        |> fun s -> if s = "" then "0" else s
        |> Int64.parse

    let statusOfEntity (entity: DynamicTableEntity) =
        let nextSequence = entity.Properties.[NextSequenceKey].Int64Value.GetValueOrDefault(0L)
        { nextSequence = nextSequence }

    let statusToEntity partition (status: EventStreamStatus) =
        let entity = DynamicTableEntity(partition, StatusRowKey)
        entity.ETag <- "*"
        entity
        |> forLong NextSequenceKey status.nextSequence

    let setMeta meta (entity: DynamicTableEntity) =
        (entity, Map.toSeq meta)
        ||> Seq.fold (fun e (key, value) ->
            forString (sprintf "%s%s" MetaPrefix key) value e)

    let setData (bytes: byte[]) (entity: DynamicTableEntity) =
        (entity, seq { 0 .. bytes.Length / BinaryChunkSize })
        ||> Seq.fold (fun e index ->
            let startIndex = index * BinaryChunkSize
            let size = min BinaryChunkSize (bytes.Length - startIndex)
            let chunk = Array.zeroCreate size
            do Array.Copy(bytes, startIndex, chunk, 0, size)
            forByteArray (sprintf "%s%d" DataPrefix index) chunk e)

    let getMeta (entity: DynamicTableEntity) =
        entity.Properties
        |> Seq.filter (fun kvp -> String.startsWith MetaPrefix kvp.Key && kvp.Value.PropertyType = EdmType.String)
        |> Seq.map (fun kvp -> (String.substringFrom MetaPrefix.Length kvp.Key), kvp.Value.StringValue)
        |> Map.ofSeq

    let getData (entity: DynamicTableEntity) =
        entity.Properties
        |> Seq.filter (fun kvp -> String.startsWith DataPrefix kvp.Key && kvp.Value.PropertyType = EdmType.Binary)
        |> Seq.map (fun kvp -> Int32.parse (String.substringFrom DataPrefix.Length kvp.Key), kvp.Value.BinaryValue)
        |> Seq.sortBy fst
        |> Seq.map snd
        |> Seq.toList
        |> fun items ->
            let size = items |> List.sumBy (fun bytes -> bytes.Length)
            let arr = Array.zeroCreate size
            (0, items)
            ||> Seq.fold (fun index bytes ->
                Array.Copy(bytes, 0, arr, index, bytes.Length)
                index + bytes.Length)
            |> ignore
            arr

    let eventOfEntity (entity: DynamicTableEntity) =
        {
            sequence = sequenceOfRowKey entity.RowKey
            event = {
                meta = getMeta entity
                data = getData entity
            }
        }


    let eventToEntity partition sequence (event: EventData) =
        let rowKey = sequenceToRowKey sequence
        let entity = DynamicTableEntity(partition, rowKey)
        entity.ETag <- "*"
        entity
        |> setMeta event.meta
        |> setData event.data

    let loadStatus partition (table: CloudTable) = task {
        let retrieve = TableOperation.Retrieve(partition, StatusRowKey)
        let! result = table.ExecuteAsync(retrieve)
        let status = 
            match result.Result with
            | :? DynamicTableEntity as entity ->
                statusOfEntity entity
            | _ -> 
                { nextSequence = 0L }
        return status
    }


open Constants

// EventStreamReader

type EventStreamReaderInitializer = {
    readFrom: ReadFromSequence
    connectionString: string
    tableName: string
    partition: string
    streamGrainId: string
}

type IEventStreamReaderGrainInternal =
    inherit IEventStreamReaderGrain

    abstract initialize: request: EventStreamReaderInitializer -> Task<unit>
    abstract isInitialized: unit -> Task<bool>
    abstract notifyNewData: unit -> Task<unit>

type TableEventStreamReaderGrain
    (
        grainFactory: IGrainFactory
    ) =
    inherit Grain()

    let subs = ObserverSubscriptionManager<IEventStreamReaderGrainObserver>()
    let mutable initialized = false
    let mutable table: CloudTable = Unchecked.defaultof<_>
    let mutable nextSequence = 0L
    let mutable partition: string = ""
    let mutable remainingQuota = 0
    let mutable couldBeMoreData = true
    let mutable streamObserver = Unchecked.defaultof<IEventStreamGrainObserver>

    let rec fullfillQuota() = task {
        if not initialized then return()
        if remainingQuota <= 0 then return()
        if subs.Count = 0 then return()
        if not couldBeMoreData then return()
        
        let query =
            let minRowKey = sequenceToRowKey nextSequence
            let whereClause =
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition(PartitionKey, QueryComparisons.Equal, partition),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition(RowKey, QueryComparisons.GreaterThanOrEqual, minRowKey)
                )
            let query = TableQuery().Where(whereClause)
            if remainingQuota < MaxTableQueryCount then
                query.Take(Nullable remainingQuota)
            else
                query
        let! segment = table.ExecuteQuerySegmentedAsync(query, null)
        let eventCount = segment.Results.Count
        let events = segment |> Seq.map (eventOfEntity) |> List.ofSeq
        remainingQuota <- remainingQuota - eventCount
        couldBeMoreData <- isNull segment.ContinuationToken |> not
        subs.Notify (fun sub -> sub.eventsAvailable events)
        if couldBeMoreData then
            return! fullfillQuota()
        else
            return ()
    }

    interface IEventStreamReaderGrainInternal with
        member this.notifyNewData() = task {
            couldBeMoreData <- true
            do! fullfillQuota()
        }

        member this.isInitialized () = task {
            return initialized
        }

        member this.initialize(request) = task {
            if initialized then
                return invalidOp "Cannot be initialized twice"
            partition <- request.partition
            nextSequence <-
                match request.readFrom with
                | ReadFromStart -> 0L

            let account = CloudStorageAccount.Parse(request.connectionString)
            let client = account.CreateCloudTableClient()
            table <- client.GetTableReference(request.tableName)
            initialized <- true

            let streamGrain = grainFactory.GetGrain<IEventStreamGrain>(request.streamGrainId)
            let myGuid = this.GetPrimaryKey()
            let observer = {
                new IEventStreamGrainObserver with
                    member __.eventsCommitted() =
                        let mySelf = grainFactory.GetGrain<IEventStreamReaderGrainInternal>(myGuid)
                        mySelf.notifyNewData() |> ignore
            }
            let! observer = grainFactory.CreateObjectReference(observer)
            streamObserver <- observer
            do! streamGrain.subscribe(streamObserver)

            do! fullfillQuota()
        }

        member this.subscribe(observer) = task {
            if subs.IsSubscribed observer |> not then
                subs.Subscribe observer
                do! fullfillQuota()
        }

        member this.unsubscribe(observer)= task {
            if subs.IsSubscribed observer then
                subs.Unsubscribe observer
        }

        member this.addReadQuota(quota)= task {
            if quota <= 0 then
                return invalidArg "quota" "must be positive"
            remainingQuota <- remainingQuota + quota
            do! fullfillQuota()
        }

// EventStream

type EventStreamInitializer = {
    connectionString: string
    tableName: string
    partition: string
}

type IEventStreamGrainInitializer =
    inherit IEventStreamGrain

    abstract initialize: request: EventStreamInitializer -> Task<unit>
    abstract isInitialized: unit -> Task<bool>

type TableEventStreamGrain(grainFactory: IGrainFactory) =
    inherit Grain()

    let mutable tableName = ""
    let mutable connectionString = ""
    let mutable table: CloudTable = Unchecked.defaultof<_>
    let mutable initialized = false
    let mutable nextSequence = 0L
    let mutable partition = ""
    let subs = ObserverSubscriptionManager()

    let fetchStatus() = task {
        let! status = loadStatus partition table
        nextSequence <- status.nextSequence
    }

    interface IEventStreamGrainInitializer with
        member this.isInitialized () = task {
            return initialized
        }

        member this.initialize request = task {
            if initialized then
                return invalidOp "Cannot be initialized twice"
            tableName <- request.tableName
            connectionString <- request.connectionString
            partition <- request.partition

            let account = CloudStorageAccount.Parse(request.connectionString)
            let client = account.CreateCloudTableClient()
            table <- client.GetTableReference(request.tableName)
            initialized <- true

            do! fetchStatus()
        }

        member this.subscribe(observer) = task {
            if subs.IsSubscribed observer |> not then
                subs.Subscribe observer
        }

        member this.unsubscribe(observer) = task {
            if subs.IsSubscribed observer then
                subs.Unsubscribe observer
        }

        member this.getStatus() = task {
            return { nextSequence = nextSequence }
        }

        member this.commitEvents(events) = task {
            if events |> List.length <= 0 then
                return invalidOp "Must commit at least one event"
            if events |> List.length > MaxTableBatchSize - 1 then
                return invalidOp "Cannot commit more than 99 events"

            let batch = TableBatchOperation()

            let mutable sequence = nextSequence
            for event in events do
                do batch.Insert(eventToEntity partition sequence event)
                do sequence <- sequence + 1L

            let newStatus = { nextSequence = sequence }
            do batch.InsertOrReplace(statusToEntity partition newStatus)

            let! result = table.ExecuteBatchAsync(batch)

            nextSequence <- newStatus.nextSequence

            subs.Notify(fun sub -> sub.eventsCommitted())

            return ()
        }

        member this.createReader(readFrom) = task {
            let guid = Guid.NewGuid()
            let reader = grainFactory.GetGrain<IEventStreamReaderGrainInternal>(guid)
            do! reader.initialize({
                readFrom = readFrom
                connectionString = connectionString
                tableName = tableName
                partition = partition
                streamGrainId = this.GetPrimaryKeyString()
            })
            return reader :> IEventStreamReaderGrain
        }

// EventStreamNamespace

type EventStreamNamespaceInitializer = {
    connectionString: string
    tableName: string
}

type IEventStreamNamespaceGrainInitializer =
    inherit IEventStreamNamespaceGrain

    abstract initialize: request: EventStreamNamespaceInitializer -> Task<unit>
    abstract isInitialized: unit -> Task<bool>

type TableEventStreamNamespaceGrain(grainFactory: IGrainFactory) =
    inherit Grain()

    let mutable tableName = ""
    let mutable connectionString = ""
    let mutable initialized = false

    interface IEventStreamNamespaceGrainInitializer with
        member this.isInitialized () = task {
            return initialized
        }

        member this.initialize request = task {
            if initialized then
                return invalidOp "Cannot be initialized twice"
            tableName <- request.tableName
            connectionString <- request.connectionString

            let account = CloudStorageAccount.Parse(request.connectionString)
            let client = account.CreateCloudTableClient()
            let table = client.GetTableReference(request.tableName)
            let! created = table.CreateIfNotExistsAsync()
            initialized <- true
        }

        member this.getStream(streamId) = task {
            let grainId = sprintf "%s/%s" (this.GetPrimaryKeyString()) streamId
            let stream = grainFactory.GetGrain<IEventStreamGrainInitializer>(grainId)
            match! stream.isInitialized() with
            | false -> 
                do! stream.initialize({
                    connectionString = connectionString
                    tableName = tableName
                    partition = streamId
                })
            | true -> do()
            return stream :> IEventStreamGrain
        }

// EventStreamService

type TableEventStreamServiceOptions = {
    connectionString: string
}

type ITableEventStreamServiceOptions =
    abstract loadOptions: configName: string -> TableEventStreamServiceOptions

type TableEventStreamServiceGrain
    (
        grainFactory: IGrainFactory,
        options: ITableEventStreamServiceOptions
    ) =
    inherit Grain()

    let mutable connectionString = ""

    override this.OnActivateAsync() =
        task {
            let configName = this.GetPrimaryKeyString()
            let opts = options.loadOptions(configName)
            connectionString <- opts.connectionString
        } :> Task

    interface IEventStreamServiceGrain with
        member this.getNamespace(nameSpace) = task {
            let grainId = sprintf "%s/%s" (this.GetPrimaryKeyString()) nameSpace
            let namespaceGrain = grainFactory.GetGrain<IEventStreamNamespaceGrainInitializer>(grainId)
            match! namespaceGrain.isInitialized() with
            | false -> 
                do! namespaceGrain.initialize({
                    connectionString = connectionString
                    tableName = nameSpace
                })
            | true -> do()
            return namespaceGrain :> IEventStreamNamespaceGrain
        }
