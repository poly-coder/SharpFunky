module SharpFunky.AzureStorage

open System.Collections.Generic
open Microsoft.WindowsAzure.Storage
open FSharp.Control.Tasks.V2

[<RequireQualifiedAccess>]
module Account =
    let parse connectionString = CloudStorageAccount.Parse(connectionString)
    let tryParse connectionString = CloudStorageAccount.TryParse(connectionString) |> Option.ofTryOp

    let blobClient (account: CloudStorageAccount) = account.CreateCloudBlobClient()
    let fileClient (account: CloudStorageAccount) = account.CreateCloudFileClient()
    let queueClient (account: CloudStorageAccount) = account.CreateCloudQueueClient()
    let tableClient (account: CloudStorageAccount) = account.CreateCloudTableClient()

let fromSegmented initToken getNextToken getItems isLastTokenFn segmentedFn = task {
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

[<RequireQualifiedAccess>]
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
    let internal getContToken (s: TableResultSegment) = s.ContinuationToken
    let internal getResults (s: TableResultSegment) = s.Results
    let listTables client =
        fun token -> listTablesSegmented token client
        |> fromSegmented null getContToken getResults isNull
    let listPrefixedTables prefix client =
        fun token -> listPrefixedTablesSegmented prefix token client
        |> fromSegmented null getContToken getResults isNull

    let SystemKeys = ["PartitionKey"; "RowKey"; "Timestamp"] |> Set.ofList

    module Query =
        let and' cond1 cond2 = TableQuery.CombineFilters(cond1, TableOperators.And, cond2)
        let or' cond1 cond2 = TableQuery.CombineFilters(cond1, TableOperators.Or, cond2)

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

    module DynamicTableEntity =
        ()

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
            |> fromSegmented null getContToken getResults isNull

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
            |> fromSegmented null getContToken getResults isNull
