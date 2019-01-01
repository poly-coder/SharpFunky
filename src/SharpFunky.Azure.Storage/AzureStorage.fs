module SharpFunky.AzureStorage

open System.Collections.Generic
open Microsoft.WindowsAzure.Storage
open FSharp.Control.Tasks.V2
open System.Threading.Tasks
open SharpFunky

module Account =
    let parse connectionString = CloudStorageAccount.Parse(connectionString)
    let tryParse connectionString = CloudStorageAccount.TryParse(connectionString) |> Option.ofTryOp

    let blobClient (account: CloudStorageAccount) = account.CreateCloudBlobClient()
    let fileClient (account: CloudStorageAccount) = account.CreateCloudFileClient()
    let queueClient (account: CloudStorageAccount) = account.CreateCloudQueueClient()
    let tableClient (account: CloudStorageAccount) = account.CreateCloudTableClient()

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


    let executeRetrieve partitionKey rowKey (table: CloudTable) = task {
        let! result = execute (retrieve partitionKey rowKey) table
        match result.Result with
        | :? DynamicTableEntity as x -> return Some x
        | _ -> return None
    }

    let executeQuerySegmented continuationToken (query: TableQuery) (table: CloudTable) =
        table.ExecuteQuerySegmentedAsync(query, continuationToken)

    let executeQuerySegmentedOf continuationToken (query: TableQuery<_>) (table: CloudTable) =
        table.ExecuteQuerySegmentedAsync(query, continuationToken)
    
    let executeQuerySegmentedResolver (resolver: EntityResolver<_>) continuationToken (query: TableQuery) (table: CloudTable) =
        table.ExecuteQuerySegmentedAsync(query, resolver, continuationToken)

    let executeQuerySegmentedResolverOf (resolver: EntityResolver<_>) continuationToken (query: TableQuery<_>) (table: CloudTable) =
        table.ExecuteQuerySegmentedAsync(query, resolver, continuationToken)


    let internal getQueryContToken (s: TableQuerySegment) = s.ContinuationToken
    let internal getQueryResults (s: TableQuerySegment) = s.Results |> Seq.map id
    let internal getQueryContTokenOf (s: TableQuerySegment<_>) = s.ContinuationToken
    let internal getQueryResultsOf (s: TableQuerySegment<_>) = s.Results |> Seq.map id

    let executeQuery query table =
        fun token -> executeQuerySegmented token query table
        |> fromSegmented null getQueryContToken getQueryResults isNull

    let executeQueryOf query table =
        fun token -> executeQuerySegmentedOf token query table
        |> fromSegmented null getQueryContTokenOf getQueryResultsOf isNull

    let executeQueryResolver resolver query table =
        fun token -> executeQuerySegmentedResolver resolver token query table
        |> fromSegmented null getQueryContTokenOf getQueryResultsOf isNull

    let executeQueryResolverOf resolver query table =
        fun token -> executeQuerySegmentedResolverOf resolver token query table
        |> fromSegmented null getQueryContTokenOf getQueryResultsOf isNull


    let SystemKeys = ["PartitionKey"; "RowKey"; "Timestamp"; "ETag"] |> Set.ofList

    module Query =
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
            let compare operator value = String.compare operator "PartitionKey" value

            let eq value = compare QueryComparisons.Equal value
            let ne value = compare QueryComparisons.NotEqual value
            let gt value = compare QueryComparisons.GreaterThan value
            let ge value = compare QueryComparisons.GreaterThanOrEqual value
            let lt value = compare QueryComparisons.LessThan value
            let le value = compare QueryComparisons.LessThanOrEqual value

        module RowKey =
            let compare operator value = String.compare operator "RowKey" value

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

    module EntityProperty =
        let internal typed edmType get set: OptLens<EntityProperty, _> =
            OptLens.cons'
                (fun p -> if p.PropertyType = edmType then Some (get p) else None)
                (fun optVal ->
                    tee (fun prop ->
                        match optVal with
                        | Some v -> set v prop
                        | None -> ()))

        let internal typedNullable edmType get set: OptLens<EntityProperty, _> =
            OptLens.cons'
                (fun p -> if p.PropertyType = edmType then Option.ofNullable (get p) else None)
                (fun optVal ->
                    tee (fun prop -> set (Option.toNullable optVal) prop))

        let string = typed EdmType.String (fun p -> p.StringValue) (fun v p -> p.StringValue <- v)
        let binary = typed EdmType.Binary (fun p -> p.BinaryValue) (fun v p -> p.BinaryValue <- v)
        let boolean = typedNullable EdmType.Boolean (fun p -> p.BooleanValue) (fun v p -> p.BooleanValue <- v)
        let dateTime = typedNullable EdmType.DateTime (fun p -> p.DateTime) (fun v p -> p.DateTime <- v)
        let double = typedNullable EdmType.Double (fun p -> p.DoubleValue) (fun v p -> p.DoubleValue <- v)
        let guid = typedNullable EdmType.Guid (fun p -> p.GuidValue) (fun v p -> p.GuidValue <- v)
        let int32 = typedNullable EdmType.Int32 (fun p -> p.Int32Value) (fun v p -> p.Int32Value <- v)
        let int64 = typedNullable EdmType.Int64 (fun p -> p.Int64Value) (fun v p -> p.Int64Value <- v)

    module DynamicTableEntity =
        open System.Text.RegularExpressions

        let property name =
            OptLens.cons'
                (fun (entity: DynamicTableEntity) ->
                    entity.Properties.TryGetValue name |> Option.ofTryOp)
                (fun prop ->
                    tee (fun entity ->
                        match prop with
                        | Some prop -> entity.Properties.[name] <- prop
                        | None -> entity.Properties.Remove(name) |> ignore))
        
        let stringProperty name = OptLens.compose (property name) (EntityProperty.string)
        let binaryProperty name = OptLens.compose (property name) (EntityProperty.binary)
        let booleanProperty name = OptLens.compose (property name) (EntityProperty.boolean)
        let dateTimeProperty name = OptLens.compose (property name) (EntityProperty.dateTime)
        let doubleProperty name = OptLens.compose (property name) (EntityProperty.double)
        let guidProperty name = OptLens.compose (property name) (EntityProperty.guid)
        let int32Property name = OptLens.compose (property name) (EntityProperty.int32)
        let int64Property name = OptLens.compose (property name) (EntityProperty.int64)

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
                    let prop = EntityProperty.GeneratePropertyForString(segment)
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

        let executeQueryOf requestOptions operationContext query table =
            fun token -> executeQuerySegmentedOf requestOptions operationContext token query table
            |> fromSegmented null getQueryContTokenOf getQueryResultsOf isNull

        let executeQueryResolver requestOptions operationContext resolver query table =
            fun token -> executeQuerySegmentedResolver requestOptions operationContext resolver token query table
            |> fromSegmented null getQueryContTokenOf getQueryResultsOf isNull

        let executeQueryResolverOf requestOptions operationContext resolver query table =
            fun token -> executeQuerySegmentedResolverOf requestOptions operationContext resolver token query table
            |> fromSegmented null getQueryContTokenOf getQueryResultsOf isNull

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

        let executeQueryOf cancellationToken requestOptions operationContext query table =
            fun token -> executeQuerySegmentedOf cancellationToken requestOptions operationContext token query table
            |> fromSegmented null getQueryContTokenOf getQueryResultsOf isNull

        let executeQueryResolver cancellationToken requestOptions operationContext resolver query table =
            fun token -> executeQuerySegmentedResolver cancellationToken requestOptions operationContext resolver token query table
            |> fromSegmented null getQueryContTokenOf getQueryResultsOf isNull

        let executeQueryResolverOf cancellationToken requestOptions operationContext resolver query table =
            fun token -> executeQuerySegmentedResolverOf cancellationToken requestOptions operationContext resolver token query table
            |> fromSegmented null getQueryContTokenOf getQueryResultsOf isNull
