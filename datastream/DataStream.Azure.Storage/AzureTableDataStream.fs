namespace DataStream.Azure.Storage

open System
open System.IO
open System.Net
open SharpFunky
open SharpFunky.AzureStorage
open SharpFunky.AzureStorage.Tables
open DataStream
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.WindowsAzure.Storage.Table.Protocol

type AzureTableDataStreamOptions() =
    member val StorageConnectionString = "UseDevelopmentStorage=True" with get, set
    member val TableName = "datastream" with get, set


type AzureTableDataStream(options: AzureTableDataStreamOptions) =

    let account = Account.parse options.StorageConnectionString
    do NameValidator.ValidateTableName(options.TableName)

    [<Literal>]
    let StatusRowKey = "A_STATUS"
    [<Literal>]
    let ItemRowKeyPrefix = "B_"
    [<Literal>]
    let MetaPrefix = "Meta_"
    [<Literal>]
    let ContentPrefix = "Data_"

    let account = Account.parse options.StorageConnectionString
    let client = account.CreateCloudTableClient()
    let table = client.GetTableReference(options.TableName)

    let seqToRowKey sequence = sprintf "%s%020d" ItemRowKeyPrefix sequence
    let seqFromRowKey rowKey =
        rowKey
        |> String.substringFrom ItemRowKeyPrefix.Length
        |> String.trimStartWith '0'
        |> function "" -> "0" | s -> s
        |> UInt64.Parse

    let dataItemToEntity streamId item =
        Entity.create streamId (seqToRowKey item.sequence)
        |> Entity.encodeLargeBinary ContentPrefix item.data
        |> Entity.encodeStringMap MetaPrefix item.metadata

    let dataItemFromEntity entity = {
        sequence = seqFromRowKey <| Lens.get Entity.rowKey entity
        data = Entity.decodeLargeBinary ContentPrefix entity
        metadata = Entity.decodeStringMap MetaPrefix entity
    }

    let getErrorCode =
        let errors =
            [ TableErrorCodeStrings.EntityTooLarge, DataStreamErrorCode.EntityTooLargeError
              TableErrorCodeStrings.DuplicateKeyPropertySpecified, DataStreamErrorCode.DuplicateSequenceError
              TableErrorCodeStrings.EntityAlreadyExists, DataStreamErrorCode.DuplicateSequenceError
              TableErrorCodeStrings.PropertyNameInvalid, DataStreamErrorCode.MetadataError
              TableErrorCodeStrings.PropertyNameTooLong, DataStreamErrorCode.MetadataError
              TableErrorCodeStrings.PropertyValueTooLarge, DataStreamErrorCode.MetadataError
              TableErrorCodeStrings.TooManyProperties, DataStreamErrorCode.MetadataError
              TableErrorCodeStrings.TableBeingDeleted, DataStreamErrorCode.DatabaseNotFoundError
              TableErrorCodeStrings.TableNotFound, DataStreamErrorCode.DatabaseNotFoundError
              TableErrorCodeStrings.TableServerOutOfMemory, DataStreamErrorCode.DatabaseFullError
              TableErrorCodeStrings.UpdateConditionNotSatisfied, DataStreamErrorCode.ConcurrencyError
            ]
            |> Map.ofSeq
        fun exn ->
            getErrorCodes exn
            |> Seq.bindOpt (fun err -> Map.tryFind err errors)
            |> Seq.tryHead
            |> Option.defaultValue DataStreamErrorCode.UnknownError

    let rethrowException exn =
        getErrorCode exn
        |> function
            | DataStreamErrorCode.UnknownError ->
                raise exn
            | code -> raise <| DataStreamException code

    let emptyStatusResponse = {
        exists = false
        metadata = Map.empty
        etag = ""
    }

    let statusResponse etag metadata = {
        exists = true
        metadata = metadata
        etag = etag
    }

    let protectedOperation createIfNotExistsTable fn =
        let rec loop() = async {
            try return! fn()
            with
            | exn when createIfNotExistsTable && isErrorCode TableErrorCodeStrings.TableNotFound exn ->
                let! created = table.CreateIfNotExistsAsync() |> Async.AwaitTask
                if created then return! loop()
                else return rethrowException exn
            | exn -> return rethrowException exn
        }
        loop()

    interface IDataStreamService<uint64, byte[], Map<string, string>> with
        member this.getStatus request =
            protectedOperation false (fun() -> async {
                let! result =
                    Cancellable.executeRetrieve
                        request.cancellationToken Unchecked.defaultof<_> Unchecked.defaultof<_>
                        request.streamId StatusRowKey table
                    |> Async.AwaitTask

                match result with
                | Some entity ->
                    return {
                        exists = true
                        metadata = Entity.decodeStringMap MetaPrefix entity
                        etag = entity.ETag
                    }

                | None ->
                    return emptyStatusResponse
            })

        member this.saveStatus request =
            protectedOperation false (fun() -> async {
                let! insertResult =
                    Entity.create request.streamId StatusRowKey
                    |> Entity.encodeStringMap MetaPrefix request.metadata
                    |> Lens.set Entity.etag request.etag
                    |> insertOrReplace
                    |> fun operation ->
                        Cancellable.execute
                            request.cancellationToken Unchecked.defaultof<_> Unchecked.defaultof<_>
                            operation table
                        |> Async.AwaitTask
                let etag = insertResult.Etag
                return statusResponse etag request.metadata
            })

        member this.append request =
            protectedOperation false (fun() -> async {
                let itemCount = List.length request.items
                if itemCount <= 0 then
                    return invalidArg "count" "Should append at least one item"
                elif itemCount > 99 then
                    return invalidArg "count" "Cannot append more than 99 items"
                let! batchResult =
                    seq {
                        yield! request.items
                            |> Seq.map (dataItemToEntity request.streamId)
                            |> Seq.map insert
                        yield Entity.create request.streamId StatusRowKey
                            |> Entity.encodeStringMap MetaPrefix request.metadata
                            |> Lens.set Entity.etag request.etag
                            |> insertOrReplace
                    }
                    |> createBatch
                    |> fun batch -> executeBatch batch table |> Async.AwaitTask
                let etag =
                    batchResult
                    |> Seq.last
                    |> fun r -> r.Etag
                return statusResponse etag request.metadata
            })

        member this.read request =
            protectedOperation false (fun() -> async {
                let query =
                    let filter =
                        let isStream = Query.PartitionKey.eq request.streamId
                        let fromSequence = Query.RowKey.ge (seqToRowKey request.fromSequence)
                        isStream &&&& fromSequence
                    let limit = min 1 (max request.limit 1000)
                    Query.create()
                    |> Query.where filter
                    |> Query.take limit

                let! segment = executeQuerySegmented null query table |> Async.AwaitTask
                let reachedEnd = isNull segment.ContinuationToken
                let items = Seq.map dataItemFromEntity segment.Results |> Seq.toList
                let nextSequence =
                    match items with
                    | [] -> request.fromSequence
                    | _ -> items |> Seq.map (fun it -> it.sequence) |> Seq.max |> (+) 1UL

                return {
                    nextSequence = nextSequence
                    items = items
                    reachedEnd = reachedEnd
                }
            })