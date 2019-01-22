open Microsoft.WindowsAzure.Storage

open Microsoft.WindowsAzure.Storage

namespace Topics.Azure.Storage

open System.IO
open System.Net
open SharpFunky
open Topics
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Microsoft.WindowsAzure.Storage.Table.Protocol
open System

type AzureTablePartitionBinaryDataStoreOptions = {
    storageConnectionString: string // = "UseDevelopmentStorage=True" with get, set
    tableName: string
    partitionKey: string
    statusRowKey: string
    rowKeyPrefix: string
    dataColumnPrefix: string
} with
    static member defVal = {
        storageConnectionString = "UseDevelopmentStorage=True"
        tableName = ""
        partitionKey = ""
        statusRowKey = "A_STATUS"
        rowKeyPrefix = "B_"
        dataColumnPrefix = "Data_"
    }

type AzureTablePartitionBinaryDataStore(options: AzureTablePartitionBinaryDataStoreOptions) =

    let account = CloudStorageAccount.Parse(options.storageConnectionString)
    do NameValidator.ValidateTableName(options.tableName)

    let rec isTableNotFound (exn: exn) =
        match exn with
        | :? StorageException as exn
            when exn.RequestInformation.HttpStatusCode = int HttpStatusCode.NotFound &&
                 exn.RequestInformation.ErrorCode = TableErrorCodeStrings.TableNotFound ->
            true
        | :? AggregateException as exn ->
            exn.InnerExceptions
            |> Seq.exists isTableNotFound
        | _ -> false

    let mutable nextSequence = 0UL
    let mutable statusETag = "*"

    let readStatus (table: CloudTable) = async {
        let! result =
            TableOperation.Retrieve(options.partitionKey, options.statusRowKey)
            |> table.ExecuteAsync
            |> Async.AwaitTask
        match result.Result with
        | :? DynamicTableEntity as entity ->
            let nextSequence = entity.Properties.Item("NextSequence").Int64Value.GetValueOrDefault(0L) |> uint64
            return nextSequence, entity.ETag
        | _ ->
            return 0UL, "*"
    }

    let table =
        async {
            try
                let client = account.CreateCloudTableClient()
                let table = client.GetTableReference(options.tableName)
                let! created = table.CreateIfNotExistsAsync() |> Async.AwaitTask
                let! nextSeq, etag = readStatus table
                nextSequence <- nextSeq
                statusETag <- etag

                return table
            with exn ->
                printfn "%O" exn
                return raise exn
        } |> Async.toPromise

    interface IDataStoreService<uint64, byte[]> with
        member this.getNextSequence () = async {
            let! nextSequence' = service.getNextSequence()
            return nextSequence' |> Converter.backward sequenceConverter
        }

        member this.append messages = async {
            let messages' = messages |> List.map (Converter.forward dataConverter)
            let! result' = service.append messages'
            return {
                firstAssignedSequence = result'.firstAssignedSequence |> Converter.backward sequenceConverter
                nextSequence = result'.nextSequence |> Converter.backward sequenceConverter
            }
        }

        member this.read request = async {
            let request' = {
                fromSequence = request.fromSequence |> Converter.forward sequenceConverter
            }
            let! response' = service.read request'
            return {
                reachedEnd = response'.reachedEnd
                nextSequence = response'.nextSequence |> Converter.backward sequenceConverter
                messages = response'.messages |> List.map (Converter.backward dataConverter)
            }
        }
