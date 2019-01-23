module SharpFunky.EventStorage.StatelessEventStream.AzureTables

//open System
//open System.Threading.Tasks
//open SharpFunky
//open SharpFunky.EventStorage
//open SharpFunky.EventStorage.Stateless
//open SharpFunky.Services
//open SharpFunky.AzureStorage.Tables
//open Microsoft.WindowsAzure.Storage.Table
//open FSharp.Control.Tasks.V2
//open SharpFunky.Storage
//open System.Diagnostics

//type Options = {
//    table: CloudTable
//    partitionKey: string
//}

//[<RequireQualifiedAccess>]
//module Options = 
//    let from table partitionKey = 
//        {
//            table = table
//            partitionKey = partitionKey
//        }
//    let table = Lens.cons' (fun opts -> opts.table) (fun value opts -> { opts with table = value })
//    let partitionKey = Lens.cons' (fun opts -> opts.partitionKey) (fun value opts -> { opts with partitionKey = value })


//let createStatelessEventStream (opts: Options) =
//    let eventDataTypeName = "DataType"
//    let eventDataTypeBinary = "binary"
//    let eventDataTypeString = "string"
//    let eventDataPrefix = "Data_"
//    let eventMetaPrefix = "Meta_"
//    let statusRowKey = "A_Status"
//    let eventRowKey sequence = sprintf "B_%020i" sequence
//    let getEventRowKey rowKey =
//        rowKey
//        |> String.substringFrom 2
//        |> String.trimStartWith '0'
//        |> fun s -> if s = "" then "0" else s
//        |> Int64.parse

//    let metaValueToEntity name (metaValue: MetaValue) (entity: DynamicTableEntity) =
//        let name = sprintf "%s%s" eventMetaPrefix name
//        match metaValue with
//        | MetaNull -> []
//        | MetaString v ->
//            [ sprintf "S:%s" v |> EntityProperty.forString ]
//        | MetaBool v ->
//            [ EntityProperty.forBool v ]
//        | MetaStrings vs ->
//            [ String.Join("|", vs |> List.toArray)
//                |> sprintf "L:%s"
//                |> EntityProperty.forString ]
//        | MetaLong v ->
//            [ EntityProperty.forInt64 v ]
//        |> List.map (fun prop -> name, prop)
//        |> fun props -> entity |> DynamicTableEntity.addProperties props
//    let metaDataToEntity (meta: MetaData) (entity: DynamicTableEntity) =
//        do meta
//            |> Map.toSeq
//            |> Seq.iter (fun (name, value) ->
//                entity |> metaValueToEntity name value |> ignore)
//        entity
//    let metaValueFromProperty (prop: EntityProperty) =
//        match prop with
//        | StringProperty str ->
//            if str.StartsWith "S:" then str.Substring(2) |> MetaString 
//            elif str.StartsWith("L:") then str.Substring(2).Split('|') |> List.ofArray |> MetaStrings
//            else invalidOp (sprintf "Unknown string property prefix: '%s'" (str.Substring(2)))
//        | BoolProperty v -> MetaBool v
//        | Int64Property v -> MetaLong v
//        | _ -> invalidOp ("Unknown property value")
//    let metaDataFromEntity (entity: DynamicTableEntity): MetaData =
//        entity
//        |> DynamicTableEntity.getProperties
//        |> DynamicTableEntity.filterPrefixedBy eventMetaPrefix
//        |> Seq.map (fun (name, prop) -> name, metaValueFromProperty prop)
//        |> Map.ofSeq

//    let statusNextSequence = DynamicTableEntity.int64 "NextSequence"

//    let statusToEntity (status: StatelessEventStreamStatus) entity =
//        entity
//        |> OptLens.setSome statusNextSequence status.nextSequence
//        |> metaDataToEntity status.meta
//    let createStatusToEntity status =
//        DynamicTableEntity(opts.partitionKey, statusRowKey)
//        |> Lens.set DynamicTableEntity.etag "*"
//        |> statusToEntity status

//    let statusFromEntity (entity: DynamicTableEntity) =
//        StatelessEventStreamStatus.empty
//        |> Lens.setOpt StatelessEventStreamStatus.nextSequence (OptLens.getOpt statusNextSequence entity)
//        |> Lens.set StatelessEventStreamStatus.meta (metaDataFromEntity entity)

//    let eventContentToEntity (content: EventContent) (entity: DynamicTableEntity) =
//        match content with
//        | EmptyEvent -> entity
//        | BinaryEvent bytes ->
//            entity
//            |> DynamicTableEntity.addProperty eventDataTypeName 
//                (EntityProperty.GeneratePropertyForString eventDataTypeBinary)
//            |> DynamicTableEntity.encodeLargeBinary eventDataPrefix bytes
//        | StringEvent text ->
//            entity
//            |> DynamicTableEntity.addProperty eventDataTypeName 
//                (EntityProperty.GeneratePropertyForString eventDataTypeString)
//            |> DynamicTableEntity.encodeLargeString eventDataPrefix text
//    let eventDataToEntity (event: EventData) =
//        metaDataToEntity event.meta
//        >> eventContentToEntity event.data
//    let eventToEntity (event: PersistedEvent) =
//        let rowKey = eventRowKey event.sequence
//        let entity = DynamicTableEntity(opts.partitionKey, rowKey)
//        entity.ETag <- "*"
//        entity
//        |> eventDataToEntity event.event

//    let eventContentFromEntity (entity: DynamicTableEntity) =
//        match entity |> OptLens.getOpt (DynamicTableEntity.string eventDataTypeName) with
//        | Some dt when dt = eventDataTypeBinary ->
//            entity
//            |> DynamicTableEntity.decodeLargeBinary eventDataPrefix
//            |> BinaryEvent
//        | Some dt when dt = eventDataTypeString ->
//            entity
//            |> DynamicTableEntity.decodeLargeString eventDataPrefix
//            |> StringEvent
//        | None -> EmptyEvent
//        | Some dt ->
//            invalidOp (sprintf "Unknown event content data type: '%s'" dt)
//    let eventDataFromEntity (entity: DynamicTableEntity) =
//        EventData.empty
//        |> Lens.set EventData.meta (metaDataFromEntity entity)
//        |> Lens.set EventData.data (eventContentFromEntity entity)
//    let eventFromEntity (entity: DynamicTableEntity) =
//        PersistedEvent.empty
//        |> Lens.set PersistedEvent.streamId entity.PartitionKey
//        |> Lens.set PersistedEvent.sequence (getEventRowKey entity.RowKey)
//        |> Lens.set PersistedEvent.event (eventDataFromEntity entity)

//    let getStatusOrDefaultTask () = task {
//        match! opts.table |> executeRetrieve opts.partitionKey statusRowKey with
//        | Some entity -> return statusFromEntity entity, entity.ETag
//        | None -> return StatelessEventStreamStatus.empty, "*"
//    }

//    let status () =
//        task { 
//            let! st, _ = getStatusOrDefaultTask()
//            return st
//        } |> Async.ofTask

//    let read (request: StatelessReadEventsRequest) =
//        task {
//            let fromSequence = request.fromSequence |> Option.defaultValue 0L
//            let fromSequenceKey = eventRowKey fromSequence
//            let takeCount = request.limit |> Option.defaultValue 1000
//            let query =
//                let where = Query.EntityKey.ge opts.partitionKey fromSequenceKey
//                TableQuery().Where(where).Take(Nullable takeCount)
//            let! segment = executeQuerySegmented null query opts.table
//            let hasMore = isNull segment.ContinuationToken |> not
//            let events = segment |> Seq.map eventFromEntity |> Seq.toList
//            let nextSequence =
//                match events with
//                | [] -> fromSequence
//                | evs -> (evs |> Seq.map (fun e -> e.sequence) |> Seq.max) + 1L
//            return StatelessReadEventsResponse.create events nextSequence hasMore
//        } |> Async.ofTask

//    let write (request: StatelessWriteEventsRequest) =
//        task {
//            if request.events = [] then return invalidOp "Cannot write empty collection of events"
//            let eventCount = List.length request.events
//            if eventCount > 99 then return invalidOp "Cannot write more than 99 events due to restrictions on Azure Storage Tables"
//            let batch = TableBatchOperation()
//            request.events
//            |> Seq.iteri (fun index ev ->
//                PersistedEvent.empty
//                |> Lens.set PersistedEvent.event ev
//                |> Lens.set PersistedEvent.sequence (request.startSequence + int64 index)
//                |> eventToEntity
//                |> insertEcho true
//                |> batch.Add)
//            let nextSequence = request.startSequence + int64 eventCount
//            { 
//                StatelessEventStreamStatus.nextSequence = nextSequence
//                StatelessEventStreamStatus.meta = request.meta }
//                |> createStatusToEntity
//                |> Lens.set DynamicTableEntity.etag "*"
//                |> insertOrReplace
//                |> batch.Add
//            let! _ = opts.table |> executeBatch batch
//            return ()
            
//        } |> Async.ofTask

//    { new IStatelessEventStream with
//        member this.status () = status ()
//        member this.read request = read request
//        member this.write request = write request }

//let createStatelessEventStreamFromPartitions table =
//    let create partitionKey =
//        let opts: Options = {
//            table = table
//            partitionKey = partitionKey
//        }
//        createStatelessEventStream opts

//    { new IStatelessEventStreamFactory with
//        member this.create partitionKey = create partitionKey }

//let createStatelessEventStreamFromTable (tableClient: CloudTableClient) =
//    let create tableName = async {
//        let table = tableClient.GetTableReference(tableName)
//        do! table.CreateIfNotExistsAsync() |> Async.ofTaskVoid
//        return createStatelessEventStreamFromPartitions table
//    }

//    { new IKeyServiceFactoryAsync<_, _> with
//        member this.create tableName = create tableName }
