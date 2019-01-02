module SharpFunky.Storage.EventStore.AzureTables

open System
open System.Threading.Tasks
open SharpFunky
open SharpFunky.Storage
open SharpFunky.AzureStorage.Tables
open Microsoft.WindowsAzure.Storage.Table
open FSharp.Control.Tasks.V2
open SharpFunky.Storage
open System.Diagnostics
open SharpFunky.Services
open SharpFunky

type Options = {
    table: CloudTable
    partitionKey: string
}

[<RequireQualifiedAccess>]
module Options = 
    let from table partitionKey = 
        {
            table = table
            partitionKey = partitionKey
        }
    let table = Lens.cons' (fun opts -> opts.table) (fun value opts -> { opts with table = value })
    let partitionKey = Lens.cons' (fun opts -> opts.partitionKey) (fun value opts -> { opts with partitionKey = value })


let createEventStream (opts: Options) =
    let eventDataTypeName = "DataType"
    let eventDataTypeBinary = "binary"
    let eventDataTypeString = "string"
    let eventDataPrefix = "Data_"
    let eventMetaPrefix = "Meta_"
    let statusRowKey = "A_Status"
    let eventRowKey sequence = sprintf "B_%020i" sequence
    let getEventRowKey rowKey =
        rowKey
        |> String.substringFrom 2
        |> String.trimStartWith '0'
        |> fun s -> if s = "" then "0" else s
        |> Int64.parse
    
    let isFrozen = DynamicTableEntity.booleanProperty "IsFrozen"
    let nextSequence = DynamicTableEntity.int64Property "NextSequence"

    let statusToEntity (status: EventStreamStatus) =
        OptLens.setSome isFrozen status.isFrozen
        >> OptLens.setSome nextSequence status.nextSequence
    let createStatusToEntity status =
        let entity = DynamicTableEntity(opts.partitionKey, statusRowKey)
        entity.ETag <- "*"
        entity |> statusToEntity status

    let statusFromEntity (entity: DynamicTableEntity) =
        EventStreamStatus.empty
        |> Lens.setOpt EventStreamStatus.isFrozen (OptLens.getOpt isFrozen entity)
        |> Lens.setOpt EventStreamStatus.nextSequence (OptLens.getOpt nextSequence entity)

    let eventMetaValueToEntity name (metaValue: MetaValue) (entity: DynamicTableEntity) =
        let name = sprintf "%s%s" eventMetaPrefix name
        match metaValue with
        | MetaNull -> []
        | MetaString v ->
            [ sprintf "S:%s" v
                |> EntityProperty.GeneratePropertyForString ]
        | MetaStrings vs ->
            [ String.Join("|", vs |> List.toArray)
                |> sprintf "L:%s"
                |> EntityProperty.GeneratePropertyForString ]
        | MetaLong v ->
            [ EntityProperty.GeneratePropertyForLong (Nullable v) ]
        |> List.map (fun prop -> name, prop)
        |> fun props -> entity |> DynamicTableEntity.addProperties props
    let eventMetaDataToEntity (meta: MetaData) (entity: DynamicTableEntity) =
        do meta
            |> Map.toSeq
            |> Seq.iter (fun (name, value) ->
                entity |> eventMetaValueToEntity name value |> ignore)
        entity
    let eventContentToEntity (content: EventContent) (entity: DynamicTableEntity) =
        match content with
        | EmptyEvent -> entity
        | BinaryEvent bytes ->
            entity
            |> DynamicTableEntity.addProperty eventDataTypeName 
                (EntityProperty.GeneratePropertyForString eventDataTypeBinary)
            |> DynamicTableEntity.encodeLargeBinary eventDataPrefix bytes
        | StringEvent text ->
            entity
            |> DynamicTableEntity.addProperty eventDataTypeName 
                (EntityProperty.GeneratePropertyForString eventDataTypeString)
            |> DynamicTableEntity.encodeLargeString eventDataPrefix text
    let eventDataToEntity (event: EventData) =
        eventMetaDataToEntity event.meta
        >> eventContentToEntity event.data
    let eventToEntity (event: PersistedEvent) =
        let rowKey = eventRowKey event.sequence
        let entity = DynamicTableEntity(opts.partitionKey, rowKey)
        entity.ETag <- "*"
        entity
        |> eventDataToEntity event.event

    let filterEventMetaValue (prop: EntityProperty) =
        match prop.PropertyType with
        | EdmType.String ->
            match prop.StringValue with
            | null -> MetaNull
            | str -> 
                if str.StartsWith "S:" then str.Substring(2) |> MetaString 
                elif str.StartsWith("L:") then str.Substring(2).Split('|') |> List.ofArray |> MetaStrings
                else MetaNull
        | EdmType.Int64 ->
            match Option.ofNullable prop.Int64Value with
            | Some v -> MetaLong v
            | _ -> MetaNull
        | _ -> MetaNull
    let eventMetaDataFromEntity (entity: DynamicTableEntity): MetaData =
        entity
        |> DynamicTableEntity.getProperties
        |> DynamicTableEntity.filterPrefixedBy eventMetaPrefix
        |> Seq.map (fun (name, prop) -> name, filterEventMetaValue prop)
        |> Map.ofSeq
    let eventContentFromEntity (entity: DynamicTableEntity) =
        match entity |> OptLens.getOpt (DynamicTableEntity.stringProperty eventDataTypeName) with
        | Some dt when dt = eventDataTypeBinary ->
            entity
            |> DynamicTableEntity.decodeLargeBinary eventDataPrefix
            |> BinaryEvent
        | Some dt when dt = eventDataTypeString ->
            entity
            |> DynamicTableEntity.decodeLargeString eventDataPrefix
            |> StringEvent
        | None -> EmptyEvent
        | Some dt ->
            invalidOp (sprintf "Unknown event content data type: '%s'" dt)
    let eventDataFromEntity (entity: DynamicTableEntity) =
        EventData.empty
        |> Lens.set EventData.meta (eventMetaDataFromEntity entity)
        |> Lens.set EventData.data (eventContentFromEntity entity)
    let eventFromEntity (entity: DynamicTableEntity) =
        PersistedEvent.empty
        |> Lens.set PersistedEvent.streamId entity.PartitionKey
        |> Lens.set PersistedEvent.sequence (getEventRowKey entity.RowKey)
        |> Lens.set PersistedEvent.event (eventDataFromEntity entity)

    let getStatusOrDefaultTask () = task {
        match! opts.table |> executeRetrieve opts.partitionKey statusRowKey with
        | Some entity -> return statusFromEntity entity, entity.ETag
        | None -> return EventStreamStatus.empty, "*"
    }

    let freezeTask () = task {
        let! status, etag = getStatusOrDefaultTask()
        match status.isFrozen with
        | false ->
            let statusEntity =
                { status with isFrozen = true }
                |> createStatusToEntity
            statusEntity.ETag <- etag
            // let! _ = replace statusEntity |> execute
            let! result = opts.table |> execute (replace statusEntity)
            return ()
        | true -> return ()
    }

    let status () =
        task { 
            let! st, _ = getStatusOrDefaultTask()
            return st
        } |> AsyncResult.ofTask

    let freeze () = freezeTask() |> AsyncResult.ofTask

    let read (request: ReadEventsRequest) =
        task {
            let fromSequence = request.fromSequence |> Option.defaultValue 0L
            let fromSequenceKey = eventRowKey fromSequence
            let takeCount = request.limit |> Option.defaultValue 1000
            let query =
                let where = Query.EntityKey.ge opts.partitionKey fromSequenceKey
                TableQuery().Where(where).Take(Nullable takeCount)
            let! segment = executeQuerySegmented null query opts.table
            let events = segment |> Seq.map eventFromEntity |> Seq.toList
            let nextSequence =
                match events with
                | [] -> fromSequence
                | evs -> (evs |> Seq.map (fun e -> e.sequence) |> Seq.max) + 1L
            return ReadEventsResponse.empty
                |> Lens.set ReadEventsResponse.events events
                |> Lens.set ReadEventsResponse.nextSequence nextSequence
        } |> AsyncResult.ofTask

    let write (request: WriteEventsRequest) =
        task {
            if request.events = [] then return invalidOp "Cannot write empty collection of events"
            let eventCount = List.length request.events
            if eventCount > 99 then return invalidOp "Cannot write more than 99 events due to restrictions on Azure Storage Tables"
            let! status, etag = getStatusOrDefaultTask ()
            if status.isFrozen then return invalidOp "Event stream is frozen"
            let batch = TableBatchOperation()
            request.events
            |> Seq.iteri (fun index ev ->
                PersistedEvent.empty
                |> Lens.set PersistedEvent.event ev
                |> Lens.set PersistedEvent.sequence (status.nextSequence + int64 index)
                |> eventToEntity
                |> insertEcho true
                |> batch.Add)
            let nextSequence = status.nextSequence + int64 eventCount
            let statusEntity =
                { status with nextSequence = nextSequence }
                |> createStatusToEntity
            statusEntity.ETag <- etag
            statusEntity |> replace |> batch.Add
            let! _ = opts.table |> executeBatch batch
            return { nextSequence = nextSequence }
            
        } |> AsyncResult.ofTask

    { new IEventStream with
        member this.status () = status ()
        member this.read request = read request
        member this.freeze () = freeze ()
        member this.write request = write request }

let createEventStreamFromPartitions table =
    let create partitionKey =
        let opts: Options = {
            table = table
            partitionKey = partitionKey
        }
        createEventStream opts

    { new IEventStreamFactory with
        member this.create partitionKey = create partitionKey }

let createEventStreamFromTable (tableClient: CloudTableClient) =
    let create tableName = async {
        let table = tableClient.GetTableReference(tableName)
        do! table.CreateIfNotExistsAsync() |> Async.ofTaskVoid
        return createEventStreamFromPartitions table
    }

    { new IKeyServiceFactoryAsync<_, _> with
        member this.create tableName = create tableName }
