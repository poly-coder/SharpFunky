namespace Topics.Azure.Storage

open System.IO
open System.Net
open SharpFunky
open SharpFunky.AzureStorage
open SharpFunky.AzureStorage.Tables
open Topics
open Microsoft.WindowsAzure.Storage
//open Microsoft.WindowsAzure.Storage.Table
//open Microsoft.WindowsAzure.Storage.Table.Protocol
open System

type AzureTablePartitionBinaryDataStoreOptions = {
    storageConnectionString: string // = "UseDevelopmentStorage=True" with get, set
    tableName: string
    partitionKey: string
    statusRowKey: string
    rowKeyPrefix: string
    dataColumnPrefix: string
    nextSequenceColumnName: string
} with
    static member defVal = {
        storageConnectionString = "UseDevelopmentStorage=True"
        tableName = ""
        partitionKey = ""
        statusRowKey = "A_STATUS"
        rowKeyPrefix = "B_"
        dataColumnPrefix = "Data_"
        nextSequenceColumnName = "NextSequence"
    }

type AzureTablePartitionBinaryDataStore(options: AzureTablePartitionBinaryDataStoreOptions) =

    let account = Account.parse(options.storageConnectionString)
    do NameValidator.ValidateTableName(options.tableName)

    let isPartition = Query.PartitionKey.eq options.partitionKey
    let sequenceRowKey (sequence: uint64) =
        sprintf "%s%020d" options.rowKeyPrefix sequence
    let fromSequenceRowKey = sequenceRowKey >> Query.RowKey.ge
    let rowKeySequence rowKey =
        rowKey
        |> String.substringFrom options.rowKeyPrefix.Length
        |> String.trimStartWith '0'
        |> function "" -> "0" | s -> s
        |> UInt64.Parse

    let mutable nextSequence = 0UL
    let mutable statusETag = "*"

    let readStatus table = async {
        let! result =
            table
            |> executeRetrieve options.partitionKey options.statusRowKey
            |> Async.AwaitTask
        match result with
        | Some entity ->
            let nextSequence =
                entity
                |> OptLens.getOpt (Entity.int64 options.nextSequenceColumnName)
                |> Option.defaultValue 0L
                |> uint64
            return nextSequence, Lens.get Entity.etag entity
        | _ ->
            return 0UL, "*"
    }

    let statusToEntity (sequence: uint64) =
        let nextSequenceProp = int64 sequence |> EntityProperty.forInt64
        Entity.create options.partitionKey options.statusRowKey
        |> Lens.set Entity.etag statusETag
        |> Entity.addProperty options.nextSequenceColumnName nextSequenceProp

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
            return nextSequence
        }

        member this.append messages = async {
            let messageCount = List.length messages
            if messageCount <= 0 then
                invalidArg "messages" "Must append at least one message"
            elif messageCount > 99 then
                invalidArg "messages" "Cannot append more than 99 messages"
            let! table = table
            let batch = 
                seq {
                    yield! messages
                        |> Seq.mapi (fun index message ->
                            nextSequence + uint64 index
                            |> sequenceRowKey
                            |> Entity.create options.partitionKey
                            |> Lens.set Entity.etag "*"
                            |> Entity.encodeLargeBinary options.dataColumnPrefix message
                        )
                        |> Seq.map insert
                    yield nextSequence + uint64 messageCount
                        |> statusToEntity
                        |> insertOrReplace
                } |> createBatch
            let! batchResult = table |> executeBatch batch |> Async.AwaitTask
            
            let firstAssignedSequence = nextSequence
            do nextSequence <- nextSequence + uint64 messageCount
            do statusETag <- batchResult.Item(batchResult.Count - 1).Etag
            return {
                firstAssignedSequence = firstAssignedSequence
                nextSequence = nextSequence
            }
        }

        member this.read request = async {
            if request.limit < 1 then
                return invalidArg "limit" "limit must be a positive integer"
            let! table = table
            let query =
                Query.create()
                |> Query.where (fromSequenceRowKey request.fromSequence |> Query.and' isPartition)
                |> Query.take (request.limit |> max 1000)
            let! segment = executeSegment query table |> Async.AwaitTask
            let messages =
                segment
                |> Seq.map (Entity.decodeLargeBinary options.dataColumnPrefix)
                |> Seq.toList
            let nextSequence =
                segment.Results
                |> Seq.tryLast
                |> Option.map (fun e -> 1UL + rowKeySequence e.RowKey)
                |> Option.defaultValue nextSequence
                
            return {
                nextSequence = nextSequence
                messages = messages
                reachedEnd = isNull segment.ContinuationToken
            }
        }
