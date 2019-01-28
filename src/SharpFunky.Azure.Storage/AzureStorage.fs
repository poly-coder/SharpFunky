module SharpFunky.AzureStorage

open System
open System.Collections.Generic
open System.Threading.Tasks
open Microsoft.WindowsAzure.Storage
open FSharp.Control.Tasks.V2
open SharpFunky

[<RequireQualifiedAccess>]
module Account =
    let parse connectionString = CloudStorageAccount.Parse(connectionString)
    let tryParse connectionString = CloudStorageAccount.TryParse(connectionString) |> Option.ofTryOp

    let blobClient (account: CloudStorageAccount) = account.CreateCloudBlobClient()
    let fileClient (account: CloudStorageAccount) = account.CreateCloudFileClient()
    let queueClient (account: CloudStorageAccount) = account.CreateCloudQueueClient()
    let tableClient (account: CloudStorageAccount) = account.CreateCloudTableClient()


let getStatusAndErrorCodes exn =
    Exception.getInnerExceptions<StorageException> exn
    |> Seq.filter (fun e -> isNull e.RequestInformation |> not)
    |> Seq.map (fun e -> e.RequestInformation.HttpStatusCode, e.RequestInformation.ErrorCode)

let isErrorCode errorCode exn =
    getStatusAndErrorCodes exn
    |> Seq.exists (fun (status, code) -> code = errorCode)

let isStatusOrErrorCode statusCode errorCode exn =
    getStatusAndErrorCodes exn
    |> Seq.exists (fun (status, code) -> status = statusCode || code = errorCode)

let fromSegmented
    (initToken: 'continuationToken)
    (getNextToken: 'segment -> 'continuationToken)
    (getItems: 'segment -> 'result seq)
    (isLastTokenFn: 'continuationToken -> bool)
    (segmentedFn: 'continuationToken -> Task<'segment>) =
    task {
        let lst = List<_>()
        let rec loop token = task {
            let! segment = segmentedFn token
            lst.AddRange <| getItems segment
            let nextToken = getNextToken segment
            if isLastTokenFn nextToken then
                return ()
            else
                return! loop nextToken
        }
        do! loop initToken
        return lst |> List.ofSeq
    }

module Tables =
    open Microsoft.WindowsAzure.Storage.Table

    let getServiceProperties (client: CloudTableClient) =
        client.GetServicePropertiesAsync()

    let setServiceProperties properties (client: CloudTableClient) =
        client.SetServicePropertiesAsync(properties)

    let getServiceStats (client: CloudTableClient) =
        client.GetServiceStatsAsync()

    let getTableReference tableName (client: CloudTableClient) =
        client.GetTableReference(tableName)

    let listTablesSegmented currentToken (client: CloudTableClient) =
        client.ListTablesSegmentedAsync(currentToken)

    let listPrefixedTablesSegmented prefix currentToken (client: CloudTableClient) =
        client.ListTablesSegmentedAsync(prefix ,currentToken)
    

    let internal getTableContToken (s: TableResultSegment) = s.ContinuationToken
    let internal getTableResults (s: TableResultSegment) = s.Results |> Seq.map id

    let listTables client =
        fun token -> listTablesSegmented token client
        |> fromSegmented null getTableContToken getTableResults isNull

    let listPrefixedTables prefix client =
        fun token -> listPrefixedTablesSegmented prefix token client
        |> fromSegmented null getTableContToken getTableResults isNull


    let insert entity =
        TableOperation.Insert(entity)
    let insertEcho echo entity =
        TableOperation.Insert(entity, echo)

    let delete entity =
        TableOperation.Delete(entity)
    let deleteOf partitionKey rowKey =
        let entity = DynamicTableEntity(partitionKey, rowKey)
        delete entity

    let insertOrMerge entity =
        TableOperation.InsertOrMerge(entity)

    let insertOrReplace entity =
        TableOperation.InsertOrReplace(entity)

    let merge entity =
        TableOperation.Merge(entity)

    let replace entity =
        TableOperation.Replace(entity)

    let retrieve partitionKey rowKey =
        TableOperation.Retrieve(partitionKey, rowKey)

    let retrieveSelect columns partitionKey rowKey =
        TableOperation.Retrieve(partitionKey, rowKey, List<_>(columns: _ seq))

    let retrieveWith<'a> resolver partitionKey rowKey =
        TableOperation.Retrieve(partitionKey, rowKey, (resolver: EntityResolver<'a>))

    let retrieveSelectWith<'a> columns resolver partitionKey rowKey =
        TableOperation.Retrieve(partitionKey, rowKey, (resolver: EntityResolver<'a>), List<_>(columns: _ seq))
    

    let execute operation (table: CloudTable) =
        table.ExecuteAsync(operation)

    let executeBatch batch (table: CloudTable) =
        table.ExecuteBatchAsync(batch)

    let createBatch operations =
        TableBatchOperation()
        |> tee (fun batch -> Seq.iter (fun op -> batch.Add(op)) operations)

    let executeRetrieve partitionKey rowKey (table: CloudTable) = task {
        let! result = execute (retrieve partitionKey rowKey) table
        match result.Result with
        | :? DynamicTableEntity as x -> return Some x
        | _ -> return None
    }

    let executeQuerySegmented continuationToken (query: TableQuery) (table: CloudTable) =
        table.ExecuteQuerySegmentedAsync(query, continuationToken)
    let executeSegment query table = executeQuerySegmented null query table
    
    let internal getQueryContToken (s: TableQuerySegment) = s.ContinuationToken
    let internal getQueryResults (s: TableQuerySegment) = s.Results |> Seq.map id

    let executeQuery query table =
        fun token -> executeQuerySegmented token query table
        |> fromSegmented null getQueryContToken getQueryResults isNull

    [<RequireQualifiedAccess>]
    module Of =
        let executeQuerySegmented continuationToken (query: TableQuery<_>) (table: CloudTable) =
            table.ExecuteQuerySegmentedAsync(query, continuationToken)
        let executeSegment query table = executeQuerySegmented null query table

        let executeQuerySegmentedResolver (resolver: EntityResolver<_>) continuationToken (query: TableQuery<_>) (table: CloudTable) =
            table.ExecuteQuerySegmentedAsync(query, resolver, continuationToken)
        let executeSegmentResolver resolver query table = executeQuerySegmentedResolver resolver null query table

        let internal getQueryContToken (s: TableQuerySegment<_>) = s.ContinuationToken
        let internal getQueryResults (s: TableQuerySegment<_>) = s.Results |> Seq.map id

        let executeQuery query table =
            fun token -> executeQuerySegmented token query table
            |> fromSegmented null getQueryContToken getQueryResults isNull

        let executeQueryResolver resolver query table =
            fun token -> executeQuerySegmentedResolver resolver token query table
            |> fromSegmented null getQueryContToken getQueryResults isNull

    [<Literal>]
    let PartitionKeyName = "PartitionKey"
    [<Literal>]
    let RowKeyName = "RowKey"
    [<Literal>]
    let TimestampName = "Timestamp"
    [<Literal>]
    let ETagName = "ETag"
    let SystemKeys = [PartitionKeyName; RowKeyName; TimestampName; ETagName] |> Set.ofList

    [<RequireQualifiedAccess>]
    module Query =
        let create() = TableQuery()
        let copy (query: TableQuery) = query.Copy()
        let where filter (query: TableQuery) = query.Where(filter)
        let take limit (query: TableQuery) = query.Take(Nullable limit)
        let select columns (query: TableQuery) = query.Select(columns |> Seq.toArray)

        [<RequireQualifiedAccess>]
        module Of =
            let create() = TableQuery<_>()
            let copy (query: TableQuery<_>) = query.Copy()
            let where filter (query: TableQuery<_>) = query.Where(filter)
            let take limit (query: TableQuery<_>) = query.Take(Nullable limit)
            let select columns (query: TableQuery<_>) = query.Select(columns |> Seq.toArray)

        let and' cond1 cond2 = TableQuery.CombineFilters(cond1, TableOperators.And, cond2)
        let or' cond1 cond2 = TableQuery.CombineFilters(cond1, TableOperators.Or, cond2)
        let rec andMany conds =
            match conds with
            | [] -> ""
            | cond :: [] -> cond
            | cond1 :: rest -> and' cond1 (andMany rest)
        let rec orMany conds =
            match conds with
            | [] -> ""
            | cond :: [] -> cond
            | cond1 :: rest -> or' cond1 (orMany rest)

        module String =
            let compare operator columnName value =
                TableQuery.GenerateFilterCondition(columnName, operator, value)

            let eq columnName value = compare QueryComparisons.Equal columnName value
            let ne columnName value = compare QueryComparisons.NotEqual columnName value
            let gt columnName value = compare QueryComparisons.GreaterThan columnName value
            let ge columnName value = compare QueryComparisons.GreaterThanOrEqual columnName value
            let lt columnName value = compare QueryComparisons.LessThan columnName value
            let le columnName value = compare QueryComparisons.LessThanOrEqual columnName value

        module Binary =
            let compare operator columnName value =
                TableQuery.GenerateFilterConditionForBinary(columnName, operator, value)

            let eq columnName value = compare QueryComparisons.Equal columnName value
            let ne columnName value = compare QueryComparisons.NotEqual columnName value
            let gt columnName value = compare QueryComparisons.GreaterThan columnName value
            let ge columnName value = compare QueryComparisons.GreaterThanOrEqual columnName value
            let lt columnName value = compare QueryComparisons.LessThan columnName value
            let le columnName value = compare QueryComparisons.LessThanOrEqual columnName value

        module Bool =
            let compare operator columnName value =
                TableQuery.GenerateFilterConditionForBool(columnName, operator, value)

            let eq columnName value = compare QueryComparisons.Equal columnName value
            let ne columnName value = compare QueryComparisons.NotEqual columnName value
            let gt columnName value = compare QueryComparisons.GreaterThan columnName value
            let ge columnName value = compare QueryComparisons.GreaterThanOrEqual columnName value
            let lt columnName value = compare QueryComparisons.LessThan columnName value
            let le columnName value = compare QueryComparisons.LessThanOrEqual columnName value

        module Date =
            let compare operator columnName value =
                TableQuery.GenerateFilterConditionForDate(columnName, operator, value)

            let eq columnName value = compare QueryComparisons.Equal columnName value
            let ne columnName value = compare QueryComparisons.NotEqual columnName value
            let gt columnName value = compare QueryComparisons.GreaterThan columnName value
            let ge columnName value = compare QueryComparisons.GreaterThanOrEqual columnName value
            let lt columnName value = compare QueryComparisons.LessThan columnName value
            let le columnName value = compare QueryComparisons.LessThanOrEqual columnName value

        module Double =
            let compare operator columnName value =
                TableQuery.GenerateFilterConditionForDouble(columnName, operator, value)

            let eq columnName value = compare QueryComparisons.Equal columnName value
            let ne columnName value = compare QueryComparisons.NotEqual columnName value
            let gt columnName value = compare QueryComparisons.GreaterThan columnName value
            let ge columnName value = compare QueryComparisons.GreaterThanOrEqual columnName value
            let lt columnName value = compare QueryComparisons.LessThan columnName value
            let le columnName value = compare QueryComparisons.LessThanOrEqual columnName value

        module Guid =
            let compare operator columnName value =
                TableQuery.GenerateFilterConditionForGuid(columnName, operator, value)

            let eq columnName value = compare QueryComparisons.Equal columnName value
            let ne columnName value = compare QueryComparisons.NotEqual columnName value
            let gt columnName value = compare QueryComparisons.GreaterThan columnName value
            let ge columnName value = compare QueryComparisons.GreaterThanOrEqual columnName value
            let lt columnName value = compare QueryComparisons.LessThan columnName value
            let le columnName value = compare QueryComparisons.LessThanOrEqual columnName value

        module Int =
            let compare operator columnName value =
                TableQuery.GenerateFilterConditionForInt(columnName, operator, value)

            let eq columnName value = compare QueryComparisons.Equal columnName value
            let ne columnName value = compare QueryComparisons.NotEqual columnName value
            let gt columnName value = compare QueryComparisons.GreaterThan columnName value
            let ge columnName value = compare QueryComparisons.GreaterThanOrEqual columnName value
            let lt columnName value = compare QueryComparisons.LessThan columnName value
            let le columnName value = compare QueryComparisons.LessThanOrEqual columnName value

        module Long =
            let compare operator columnName value =
                TableQuery.GenerateFilterConditionForLong(columnName, operator, value)

            let eq columnName value = compare QueryComparisons.Equal columnName value
            let ne columnName value = compare QueryComparisons.NotEqual columnName value
            let gt columnName value = compare QueryComparisons.GreaterThan columnName value
            let ge columnName value = compare QueryComparisons.GreaterThanOrEqual columnName value
            let lt columnName value = compare QueryComparisons.LessThan columnName value
            let le columnName value = compare QueryComparisons.LessThanOrEqual columnName value

        module PartitionKey =
            let compare operator value = String.compare operator PartitionKeyName value

            let eq value = compare QueryComparisons.Equal value
            let ne value = compare QueryComparisons.NotEqual value
            let gt value = compare QueryComparisons.GreaterThan value
            let ge value = compare QueryComparisons.GreaterThanOrEqual value
            let lt value = compare QueryComparisons.LessThan value
            let le value = compare QueryComparisons.LessThanOrEqual value

        module RowKey =
            let compare operator value = String.compare operator RowKeyName value

            let eq value = compare QueryComparisons.Equal value
            let ne value = compare QueryComparisons.NotEqual value
            let gt value = compare QueryComparisons.GreaterThan value
            let ge value = compare QueryComparisons.GreaterThanOrEqual value
            let lt value = compare QueryComparisons.LessThan value
            let le value = compare QueryComparisons.LessThanOrEqual value

        module EntityKey =
            let compare operator partitionKey rowKey =
                PartitionKey.eq partitionKey |> and' (RowKey.compare operator rowKey)

            let eq partitionKey rowKey = compare QueryComparisons.Equal partitionKey rowKey
            let ne partitionKey rowKey = compare QueryComparisons.NotEqual partitionKey rowKey
            let gt partitionKey rowKey = compare QueryComparisons.GreaterThan partitionKey rowKey
            let ge partitionKey rowKey = compare QueryComparisons.GreaterThanOrEqual partitionKey rowKey
            let lt partitionKey rowKey = compare QueryComparisons.LessThan partitionKey rowKey
            let le partitionKey rowKey = compare QueryComparisons.LessThanOrEqual partitionKey rowKey

    let (&&&&) cond1 cond2 = Query.and' cond1 cond2
    let (||||) cond1 cond2 = Query.or' cond1 cond2

    [<RequireQualifiedAccess>]
    module EntityProperty =
        open System

        let internal getTyped edmType get = fun (p: EntityProperty) ->
            if p.PropertyType = edmType
            then get p |> Option.ofObj
            else None
        let internal getTypedNullable edmType get = fun (p: EntityProperty) ->
            if p.PropertyType = edmType
            then get p |> Option.ofNullable
            else None

        let getBinary = getTyped EdmType.Binary (fun p -> p.BinaryValue)
        let getString = getTyped EdmType.String (fun p -> p.StringValue)
        let getBool = getTypedNullable EdmType.Boolean (fun p -> p.BooleanValue)
        let getDateTime = getTypedNullable EdmType.DateTime (fun p -> p.DateTime)
        let getDateTimeOffset = getTypedNullable EdmType.DateTime (fun p -> p.DateTimeOffsetValue)
        let getDouble = getTypedNullable EdmType.Double (fun p -> p.DoubleValue)
        let getGuid = getTypedNullable EdmType.Guid (fun p -> p.GuidValue)
        let getInt32 = getTypedNullable EdmType.Int32 (fun p -> p.Int32Value)
        let getInt64 = getTypedNullable EdmType.Int64 (fun p -> p.Int64Value)

        let forBinary value = EntityProperty.GeneratePropertyForByteArray value
        let forString value = EntityProperty.GeneratePropertyForString value
        let forBool value = EntityProperty.GeneratePropertyForBool (Nullable value)
        let forDateTime value = EntityProperty.GeneratePropertyForDateTimeOffset (Nullable <| DateTimeOffset value)
        let forDateTimeOffset value = EntityProperty.GeneratePropertyForDateTimeOffset (Nullable value)
        let forDouble value = EntityProperty.GeneratePropertyForDouble (Nullable value)
        let forGuid value = EntityProperty.GeneratePropertyForGuid (Nullable value)
        let forInt32 value = EntityProperty.GeneratePropertyForInt (Nullable value)
        let forInt64 value = EntityProperty.GeneratePropertyForLong (Nullable value)

        let makeBinary = Option.matches forBinary (konst null)
        let makeString = Option.matches forString (konst null)
        let makeBool = Option.matches forBool (konst null)
        let makeDateTime = Option.matches forDateTime (konst null)
        let makeDateTimeOffset = Option.matches forDateTimeOffset (konst null)
        let makeDouble = Option.matches forDouble (konst null)
        let makeGuid = Option.matches forGuid (konst null)
        let makeInt32 = Option.matches forInt32 (konst null)
        let makeInt64 = Option.matches forInt64 (konst null)

        let string = OptLens.cons' getString (fun v _ -> makeString v)
        let binary = OptLens.cons' getBinary (fun v _ -> makeBinary v)
        let bool = OptLens.cons' getBool (fun v _ -> makeBool v)
        let dateTime = OptLens.cons' getDateTime (fun v _ -> makeDateTime v)
        let dateTimeOffset = OptLens.cons' getDateTimeOffset (fun v _ -> makeDateTimeOffset v)
        let double = OptLens.cons' getDouble (fun v _ -> makeDouble v)
        let guid = OptLens.cons' getGuid (fun v _ -> makeGuid v)
        let int32 = OptLens.cons' getInt32 (fun v _ -> makeInt32 v)
        let int64 = OptLens.cons' getInt64 (fun v _ -> makeInt64 v)

    let (|StringProperty|_|) prop = EntityProperty.getString prop
    let (|BinaryProperty|_|) prop = EntityProperty.getBinary prop
    let (|BoolProperty|_|) prop = EntityProperty.getBool prop
    let (|DateTimeProperty|_|) prop = EntityProperty.getDateTime prop
    let (|DateTimeOffsetProperty|_|) prop = EntityProperty.getDateTimeOffset prop
    let (|DoubleProperty|_|) prop = EntityProperty.getDouble prop
    let (|GuidProperty|_|) prop = EntityProperty.getGuid prop
    let (|Int32Property|_|) prop = EntityProperty.getInt32 prop
    let (|Int64Property|_|) prop = EntityProperty.getInt64 prop

    [<RequireQualifiedAccess>]
    module Entity =
        open System.Text.RegularExpressions

        let create pk rk = DynamicTableEntity(pk, rk)

        let partitionKey: Lens<DynamicTableEntity, _> =
            Lens.cons' (fun e -> e.PartitionKey) (fun v e -> e.PartitionKey <- v; e)
        let rowKey: Lens<DynamicTableEntity, _> =
            Lens.cons' (fun e -> e.RowKey) (fun v e -> e.RowKey <- v; e)
        let etag: Lens<DynamicTableEntity, _> =
            Lens.cons' (fun e -> e.ETag) (fun v e -> e.ETag <- v; e)
        let timestamp: Lens<DynamicTableEntity, _> =
            Lens.cons' (fun e -> e.Timestamp) (fun v e -> e.Timestamp <- v; e)

        let getProperty name (entity: DynamicTableEntity) =
            entity.Properties.TryGetValue name
            |> Option.ofTryOp
        let setProperty name prop (entity: DynamicTableEntity) =
            match prop with
            | Some prop -> entity.Properties.[name] <- prop
            | None -> entity.Properties.Remove(name) |> ignore
            entity

        let property getValue makeProp name =
            OptLens.cons'
                (getProperty name >> Option.bind getValue)
                (makeProp >> Option.ofObj >> setProperty name)
        
        let string = property EntityProperty.getString EntityProperty.makeString
        let binary = property EntityProperty.getBinary EntityProperty.makeBinary
        let bool = property EntityProperty.getBool EntityProperty.makeBool
        let dateTime = property EntityProperty.getDateTime EntityProperty.makeDateTime
        let dateTimeOffset = property EntityProperty.getDateTimeOffset EntityProperty.makeDateTimeOffset
        let double = property EntityProperty.getDouble EntityProperty.makeDouble
        let guid = property EntityProperty.getGuid EntityProperty.makeGuid
        let int32 = property EntityProperty.getInt32 EntityProperty.makeInt32
        let int64 = property EntityProperty.getInt64 EntityProperty.makeInt64

        let addProperty name prop =
            tee (fun (entity: DynamicTableEntity) -> 
                entity.Properties.Add(name, prop))

        let addProperties propPairs =
            tee (fun (entity: DynamicTableEntity) -> 
                propPairs
                |> Seq.iter (fun (name, prop) ->
                    addProperty name prop entity |> ignore))

        let getProperties (entity: DynamicTableEntity) =
            entity.Properties
            |> Seq.map (fun pair -> pair.Key, pair.Value)

        let filterPrefixedBy prefix =
            Seq.bindOpt (fun (name, prop) ->
                if String.length name > String.length prefix && name |> String.startsWith prefix then
                    Some (String.substringFrom prefix.Length name, prop)
                else None)

        let filterBinary props =
            props
            |> Seq.bindOpt (fun (name, prop) ->
                if (prop: EntityProperty).PropertyType = EdmType.Binary then
                    Some (name, prop.BinaryValue)
                else None)

        let filterString props =
            props
            |> Seq.bindOpt (fun (name, prop) ->
                if (prop: EntityProperty).PropertyType = EdmType.String then
                    Some (name, prop.StringValue)
                else None)

        let filterIndexed props =
            props
            |> Seq.bindOpt (fun (name, prop) ->
                Int32.tryParse name
                |> Option.map (fun index -> index, prop))

        let internal indexedNameRegex = Regex(@"^(?<prefix>.+?)(?<index>\d+)$", RegexOptions.Compiled)
        let internal encodeIndexedName prefix index = 
            sprintf "%s%i" prefix index
        let internal decodeIndexedName name =
            let m = indexedNameRegex.Match name
            if m.Success && m.Groups.["prefix"].Success && m.Groups.["index"].Success then
                let prefix = m.Groups.["prefix"].Value
                let index = m.Groups.["index"].Value |> Int32.parse
                Some (prefix, index)
            else
                None

        let encodeLargeBinary prefix (bytes: byte[]) =
            let maxLength = 0x10000
            tee (fun (entity: DynamicTableEntity) ->
                seq { 0 .. bytes.Length / maxLength }
                |> Seq.iter (fun index ->
                    let fromPos = index * maxLength
                    let toPos = min (fromPos + maxLength) bytes.Length
                    let len = toPos - fromPos
                    let segment = Array.zeroCreate len
                    do Array.blit bytes fromPos segment 0 len
                    let name = encodeIndexedName prefix index
                    let prop = EntityProperty.GeneratePropertyForByteArray(segment)
                    entity |> addProperty name prop |> ignore))

        let decodeLargeBinary prefix (entity: DynamicTableEntity) =
            entity
            |> getProperties
            |> filterPrefixedBy prefix
            |> filterIndexed
            |> filterBinary
            |> Seq.sortBy fst
            |> Seq.map snd
            |> Seq.fold Array.append [||]

        let encodeLargeString prefix (text: string) =
            let maxLength = 0x8000
            tee (fun (entity: DynamicTableEntity) ->
                seq { 0 .. text.Length / maxLength }
                |> Seq.iter (fun index ->
                    let fromPos = index * maxLength
                    let toPos = min (fromPos + maxLength) text.Length
                    let len = toPos - fromPos
                    let segment = text.Substring(fromPos, len)
                    let name = encodeIndexedName prefix index
                    let prop = EntityProperty.forString segment
                    entity |> addProperty name prop |> ignore))

        let decodeLargeString prefix (entity: DynamicTableEntity) =
            entity
            |> getProperties
            |> filterPrefixedBy prefix
            |> filterIndexed
            |> filterString
            |> Seq.sortBy fst
            |> Seq.map snd
            |> Seq.fold (+) String.empty

        let encodeStringMap prefix (map: Map<string, string>) = 
            tee (fun (entity: DynamicTableEntity) ->
                map
                |> Map.toSeq
                |> Seq.iter (fun (key, value) ->
                    let name = sprintf "%s%s" prefix key
                    let prop = EntityProperty.forString value
                    entity |> addProperty name prop |> ignore))

        let decodeStringMap prefix (entity: DynamicTableEntity) =
            entity
            |> getProperties
            |> filterPrefixedBy prefix
            |> filterString
            |> Map.ofSeq


    [<RequireQualifiedAccess>]
    module With =
        let getServiceProperties requestOptions operationContext (client: CloudTableClient) =
            client.GetServicePropertiesAsync(requestOptions, operationContext)

        let setServiceProperties requestOptions operationContext properties (client: CloudTableClient) =
            client.SetServicePropertiesAsync(properties, requestOptions, operationContext)

        let getServiceStats requestOptions operationContext (client: CloudTableClient) =
            client.GetServiceStatsAsync(requestOptions, operationContext)
        

        let listTablesSegmented requestOptions operationContext prefix maxResults currentToken (client: CloudTableClient) =
            client.ListTablesSegmentedAsync(prefix, Option.toNullable maxResults ,currentToken, requestOptions, operationContext)

        let listTables requestOptions operationContext prefix maxResults client =
            fun token -> listTablesSegmented requestOptions operationContext prefix maxResults token client
            |> fromSegmented null getTableContToken getTableResults isNull
        

        let execute requestOptions operationContext operation (table: CloudTable) =
            table.ExecuteAsync(operation, requestOptions, operationContext)

        let executeBatch requestOptions operationContext batch (table: CloudTable) =
            table.ExecuteBatchAsync(batch, requestOptions, operationContext)

        let executeRetrieve requestOptions operationContext partitionKey rowKey (table: CloudTable) = task {
            let! result = execute requestOptions operationContext (retrieve partitionKey rowKey) table
            match result.Result with
            | :? DynamicTableEntity as x -> return Some x
            | _ -> return None
        }


        let executeQuerySegmented requestOptions operationContext continuationToken (query: TableQuery) (table: CloudTable) =
            table.ExecuteQuerySegmentedAsync(query, continuationToken, requestOptions, operationContext)

        let executeQuerySegmentedOf requestOptions operationContext continuationToken (query: TableQuery<_>) (table: CloudTable) =
            table.ExecuteQuerySegmentedAsync(query, continuationToken, requestOptions, operationContext)
        
        let executeQuerySegmentedResolver requestOptions operationContext (resolver: EntityResolver<_>) continuationToken (query: TableQuery) (table: CloudTable) =
            table.ExecuteQuerySegmentedAsync(query, resolver, continuationToken, requestOptions, operationContext)

        let executeQuerySegmentedResolverOf requestOptions operationContext (resolver: EntityResolver<_>) continuationToken (query: TableQuery<_>) (table: CloudTable) =
            table.ExecuteQuerySegmentedAsync(query, resolver, continuationToken, requestOptions, operationContext)


        let executeQuery requestOptions operationContext query table =
            fun token -> executeQuerySegmented requestOptions operationContext token query table
            |> fromSegmented null getQueryContToken getQueryResults isNull

    [<RequireQualifiedAccess>]
    module Cancellable =
        let getServiceProperties cancellationToken requestOptions operationContext (client: CloudTableClient) =
            client.GetServicePropertiesAsync(requestOptions, operationContext, cancellationToken)

        let setServiceProperties cancellationToken requestOptions operationContext properties (client: CloudTableClient) =
            client.SetServicePropertiesAsync(properties, requestOptions, operationContext, cancellationToken)

        let getServiceStats cancellationToken requestOptions operationContext (client: CloudTableClient) =
            client.GetServiceStatsAsync(requestOptions, operationContext, cancellationToken)


        let listTablesSegmented cancellationToken requestOptions operationContext prefix maxResults currentToken (client: CloudTableClient) =
            client.ListTablesSegmentedAsync(prefix, Option.toNullable maxResults ,currentToken, requestOptions, operationContext, cancellationToken)

        let listTables cancellationToken requestOptions operationContext prefix maxResults client =
            fun token -> listTablesSegmented cancellationToken requestOptions operationContext prefix maxResults token client
            |> fromSegmented null getTableContToken getTableResults isNull
        

        let execute cancellationToken requestOptions operationContext operation (table: CloudTable) =
            table.ExecuteAsync(operation, requestOptions, operationContext, cancellationToken)

        let executeBatch cancellationToken requestOptions operationContext batch (table: CloudTable) =
            table.ExecuteBatchAsync(batch, requestOptions, operationContext, cancellationToken)

        let executeRetrieve cancellationToken requestOptions operationContext partitionKey rowKey (table: CloudTable) = task {
            let! result = execute cancellationToken requestOptions operationContext (retrieve partitionKey rowKey) table
            match result.Result with
            | :? DynamicTableEntity as x -> return Some x
            | _ -> return None
        }

        let executeQuerySegmented cancellationToken requestOptions operationContext continuationToken (query: TableQuery) (table: CloudTable) =
            table.ExecuteQuerySegmentedAsync(query, continuationToken, requestOptions, operationContext, cancellationToken)

        let executeQuerySegmentedOf cancellationToken requestOptions operationContext (continuationToken: TableContinuationToken) (query: TableQuery<_>) (table: CloudTable) =
            table.ExecuteQuerySegmentedAsync(query, continuationToken, requestOptions, operationContext, cancellationToken)
        
        let executeQuerySegmentedResolver cancellationToken requestOptions operationContext (resolver: EntityResolver<_>) continuationToken (query: TableQuery) (table: CloudTable) =
            table.ExecuteQuerySegmentedAsync(query, resolver, continuationToken, requestOptions, operationContext, cancellationToken)

        let executeQuerySegmentedResolverOf cancellationToken requestOptions operationContext (resolver: EntityResolver<_>) continuationToken (query: TableQuery<_>) (table: CloudTable) =
            table.ExecuteQuerySegmentedAsync(query, resolver, continuationToken, requestOptions, operationContext, cancellationToken)


        let executeQuery cancellationToken requestOptions operationContext query table =
            fun token -> executeQuerySegmented cancellationToken requestOptions operationContext token query table
            |> fromSegmented null getQueryContToken getQueryResults isNull
