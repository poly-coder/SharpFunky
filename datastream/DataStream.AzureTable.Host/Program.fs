open System
open Microsoft.Extensions.Configuration
open DataStream.Protocols.BinaryDataStream
open Grpc.Core
open SharpFunky.AzureStorage
open SharpFunky.AzureStorage.Tables
open FSharp.Control.Tasks.V2
open SharpFunky
open Google.Protobuf.Collections
open Microsoft.WindowsAzure.Storage.Table.Protocol
open Google.Protobuf

type ServerConfig() =
    member val Host = "localhost" with get, set
    member val Port = 50200 with get, set

type DataStreamConfig() =
    member val StorageConnectionString = "UseDevelopmentStorage=true" with get, set
    member val TableName = "datastream" with get, set

type DataStoreServiceImpl(config: DataStreamConfig) =
    inherit BinaryDataStoreService.BinaryDataStoreServiceBase()

    [<Literal>]
    let StatusRowKey = "A_STATUS"
    [<Literal>]
    let ItemRowKeyPrefix = "B_"
    [<Literal>]
    let MetaPrefix = "Meta_"
    [<Literal>]
    let ContentPrefix = "Data_"

    let account = Account.parse config.StorageConnectionString
    let client = account.CreateCloudTableClient()
    let table = client.GetTableReference(config.TableName)

    let seqToRowKey sequence = sprintf "%s%020d" ItemRowKeyPrefix sequence
    let seqFromRowKey rowKey =
        rowKey
        |> String.substringFrom ItemRowKeyPrefix.Length
        |> String.trimStartWith '0'
        |> function "" -> "0" | s -> s
        |> UInt64.Parse

    let mapFromProtobuf (mapField: MapField<_, _>) =
        mapField
        |> Seq.map (fun pair -> pair.Key, pair.Value)
        |> Map.ofSeq

    let mapToProtobuf (mapField: MapField<_, _>) map =
        map
        |> Map.toSeq
        |> Seq.iter (fun (key, value) -> mapField.Add(key, value))

    let dataItemToEntity streamId (item: DataItem) =
        Entity.create streamId (seqToRowKey item.Sequence)
        |> Entity.encodeLargeBinary ContentPrefix (item.Content.ToByteArray())
        |> Entity.encodeStringMap MetaPrefix (mapFromProtobuf item.Metadata)

    let dataItemFromEntity entity =
        DataItem()
        |> tee (fun item ->
            item.Sequence <- seqFromRowKey <| Lens.get Entity.rowKey entity
            item.Content <- Entity.decodeLargeBinary ContentPrefix entity |> ByteString.CopyFrom
            mapToProtobuf item.Metadata (Entity.decodeStringMap MetaPrefix entity)
        )

    let getErrorCode =
        let errors =
            [ TableErrorCodeStrings.EntityTooLarge, ErrorCode.EntityTooLargeError
              TableErrorCodeStrings.DuplicateKeyPropertySpecified, ErrorCode.DuplicateSequenceError
              TableErrorCodeStrings.EntityAlreadyExists, ErrorCode.DuplicateSequenceError
              TableErrorCodeStrings.PropertyNameInvalid, ErrorCode.MetadataError
              TableErrorCodeStrings.PropertyNameTooLong, ErrorCode.MetadataError
              TableErrorCodeStrings.PropertyValueTooLarge, ErrorCode.MetadataError
              TableErrorCodeStrings.TooManyProperties, ErrorCode.MetadataError
              TableErrorCodeStrings.TableBeingDeleted, ErrorCode.DatabaseNotFoundError
              TableErrorCodeStrings.TableNotFound, ErrorCode.DatabaseNotFoundError
              TableErrorCodeStrings.TableServerOutOfMemory, ErrorCode.DatabaseFullError
              TableErrorCodeStrings.UpdateConditionNotSatisfied, ErrorCode.ConcurrencyError
            ]
            |> Map.ofSeq
        fun exn ->
            getErrorCodes exn
            |> Seq.bindOpt (fun err -> Map.tryFind err errors)
            |> Seq.tryHead
            |> Option.defaultValue ErrorCode.UnknownError

    let emptyStatusResponse streamId =
        DataStreamStatus()
        |> tee (fun response ->
            response.StreamId <- streamId
            response.Success <- true
            response.Exists <- false
        )

    let statusResponse streamId etag metadata =
        DataStreamStatus()
        |> tee (fun response ->
            response.StreamId <- streamId
            response.Success <- true
            response.Exists <- true
            response.Etag <- etag
            mapToProtobuf response.Metadata metadata
        )

    let exceptionStatusResponse streamId exn =
        DataStreamStatus()
        |> tee (fun response ->
            response.StreamId <- streamId
            response.Success <- false
            response.ErrorCode <- getErrorCode exn
        )

    let readResponse streamId nextSequence reachedEnd items  =
        ReadResponse()
        |> tee (fun response ->
            response.StreamId <- streamId
            response.Success <- true
            response.NextSequence <- nextSequence
            response.ReachedEnd <- reachedEnd
            response.Items.Add(items: _ seq)
        )

    let exceptionReadResponse streamId exn =
        ReadResponse()
        |> tee (fun response ->
            response.StreamId <- streamId
            response.Success <- false
            response.ErrorCode <- getErrorCode exn
        )

    override this.GetStatus(request, context) = task {
        try
            let! result =
                Cancellable.executeRetrieve
                    context.CancellationToken Unchecked.defaultof<_> Unchecked.defaultof<_>
                    request.StreamId StatusRowKey table

            match result with
            | Some entity ->
                let metadata = Entity.decodeStringMap MetaPrefix entity
                return statusResponse request.StreamId entity.ETag metadata

            | None ->
                return emptyStatusResponse request.StreamId
        with exn ->
            return exceptionStatusResponse request.StreamId exn
    }

    override this.SaveStatus(request, context) = task {
        try
            let metadata = mapFromProtobuf request.Metadata
            let! insertResult =
                Entity.create request.StreamId StatusRowKey
                |> Entity.encodeStringMap MetaPrefix metadata
                |> Lens.set Entity.etag request.Etag
                |> insertOrReplace
                |> fun op -> execute op table
            let etag = insertResult.Etag
            return statusResponse request.StreamId etag metadata
        with exn ->
            return exceptionStatusResponse request.StreamId exn
    }

    override this.Append(request, context) = task {
        try
            if request.Items.Count <= 0 then
                return invalidArg "count" "Should append at least one item"
            elif request.Items.Count > 99 then
                return invalidArg "count" "Cannot append more than 99 items"
            let metadata = mapFromProtobuf request.Metadata
            let! batchResult =
                seq {
                    yield! request.Items
                        |> Seq.map (dataItemToEntity request.StreamId)
                        |> Seq.map insert
                    yield Entity.create request.StreamId StatusRowKey
                        |> Entity.encodeStringMap MetaPrefix metadata
                        |> Lens.set Entity.etag request.Etag
                        |> insertOrReplace
                }
                |> createBatch
                |> fun batch -> 
                    Cancellable.executeBatch
                        context.CancellationToken Unchecked.defaultof<_> Unchecked.defaultof<_>
                        batch table
            let etag =
                batchResult
                |> Seq.last
                |> fun r -> r.Etag
            return statusResponse request.StreamId etag metadata
        with exn ->
            return exceptionStatusResponse request.StreamId exn
    }

    override this.Read(request, context) = task {
        try
            let query =
                let filter =
                    let isStream = Query.PartitionKey.eq request.StreamId
                    let fromSequence = Query.RowKey.ge (seqToRowKey request.FromSequence)
                    isStream &&&& fromSequence
                let limit = min 1 (max request.Limit 1000)
                Query.create()
                |> Query.where filter
                |> Query.take limit

            let! segment =
                Cancellable.executeQuerySegmented
                    context.CancellationToken Unchecked.defaultof<_> Unchecked.defaultof<_>
                    null query table
            let reachedEnd = isNull segment.ContinuationToken
            let items = Seq.map dataItemFromEntity segment.Results |> Seq.toList
            let nextSequence =
                match items with
                | [] -> request.FromSequence
                | _ -> items |> Seq.map (fun it -> it.Sequence) |> Seq.max |> (+) 1UL

            return readResponse request.StreamId nextSequence reachedEnd items
        with exn ->
            return exceptionReadResponse request.StreamId exn
    }

let loadConfiguration argv =
    ConfigurationBuilder()
        .AddJsonFile("appsettings.json", optional = false, reloadOnChange = false)
        .AddJsonFile("appsettings.production.json", optional = true, reloadOnChange = false)
        .AddEnvironmentVariables()
        .AddCommandLine(argv: string[])
        .Build()

[<EntryPoint>]
let main argv =
    let configuration = loadConfiguration argv
    let dataStreamConfig = configuration.GetSection("DataStream").Get<DataStreamConfig>()
    let serverConfig = configuration.GetSection("Server").Get<ServerConfig>()
    let server = new Server()
    do ServerPort(serverConfig.Host, serverConfig.Port, ServerCredentials.Insecure)
        |> server.Ports.Add
        |> ignore
    do dataStreamConfig
        |> DataStoreServiceImpl
        |> BinaryDataStoreService.BindService
        |> server.Services.Add
    do printfn "Connecting ..."
    do server.Start()
    do printfn "Connected!"
    do printfn "Press enter to stop server..."
    do Console.ReadLine() |> ignore
    do server.ShutdownAsync().GetAwaiter().GetResult()
    0 // return an integer exit code
