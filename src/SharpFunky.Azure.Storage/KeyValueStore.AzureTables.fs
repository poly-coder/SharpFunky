module SharpFunky.Storage.KeyValueStore.AzureTables

open SharpFunky
open SharpFunky.Conversion
open SharpFunky.Storage
open Microsoft.WindowsAzure.Storage.Table
open SharpFunky.AzureStorage

type Options<'a> = {
    table: CloudTable
    partitionKey: string
    rowKeyPrefix: string
    converter: IAsyncReversibleConverter<'a, (Map<string, string> * string)>
    updateKey: string -> 'a -> 'a
    dataColumnName: string
}

[<RequireQualifiedAccess>]
module Options =
    let from partitionKey table converter = 
        {
            table = table
            converter = converter
            partitionKey = partitionKey
            rowKeyPrefix = ""
            dataColumnName = "__Data"
            updateKey = fun _ v -> v
        }
    let table<'a> : Lens<Options<'a>, _> =
        Lens.cons' (fun opts -> opts.table) (fun value opts -> { opts with table = value })
    let converter<'a> : Lens<Options<'a>, _> =
        Lens.cons' (fun opts -> opts.converter) (fun value opts -> { opts with converter = value })
    let partitionKey<'a> : Lens<Options<'a>, _> =
        Lens.cons' (fun opts -> opts.partitionKey) (fun value opts -> { opts with partitionKey = value })
    let rowKeyPrefix<'a> : Lens<Options<'a>, _> =
        Lens.cons' (fun opts -> opts.rowKeyPrefix) (fun value opts -> { opts with rowKeyPrefix = value })
    let updateKey<'a> : Lens<Options<'a>, _> =
        Lens.cons' (fun opts -> opts.updateKey) (fun value opts -> { opts with updateKey = value })
    let dataColumnName<'a> : Lens<Options<'a>, _> =
        Lens.cons' (fun opts -> opts.dataColumnName) (fun value opts -> { opts with dataColumnName = value })

let create opts =
    let getRowKey = sprintf "%s%s" opts.rowKeyPrefix

    let extractData (item: DynamicTableEntity) =
        let mutable data = None
        let mutable map = Map.empty
        for p in item.Properties do
            match p.Key with
            | key when Set.contains key Tables.SystemKeys -> ()
            | key when key = opts.dataColumnName ->
                match data with
                | None when p.Value.PropertyType = EdmType.String ->
                    data <- Some p.Value.StringValue
                | None ->
                    raise (exn <| sprintf "Data column type should be string but %A found" p.Value.PropertyType)
                | Some _ ->
                    raise (exn <| sprintf "Duplicate data column")
            | key ->
                match p.Value.PropertyType with
                | EdmType.String ->
                    map <- map |> Map.add key p.Value.StringValue
                | propType ->
                    raise (exn <| sprintf "%s column type should be string but %A found" key propType)
        match data with
        | None -> 
            raise (exn <| "Data column not found")
        | Some data ->
            data, map
        
    let insertData meta data (item: DynamicTableEntity) =
        for k, v in meta |> Map.toSeq do
            let prop = EntityProperty.GeneratePropertyForString(v)
            item.Properties.Add(k, prop)
        item.Properties.Add(opts.dataColumnName, EntityProperty.GeneratePropertyForString(data))
        item

    let get key =
        async {
            let! retrieveResult =
                opts.table
                |> Tables.execute (Tables.retrieve opts.partitionKey key)
                |> Async.ofTask
            match retrieveResult.Result with
            | :? DynamicTableEntity as entity ->
                let data, meta = extractData entity
                let! result = opts.converter.convertBack(meta, data)
                return Some result
            | _ -> return None
        }

    let put key value =
        async {
            let! meta, data =
                value
                |> opts.updateKey key
                |> opts.converter.convert
            do! DynamicTableEntity()
                |> insertData meta data
                |> Tables.insertOrReplace
                |> opts.table.ExecuteAsync
                |> Async.ofTaskVoid
        }
        
    let del =
        getRowKey
        >> Tables.deleteOf opts.partitionKey
        >> opts.table.ExecuteAsync
        >> Async.ofTaskVoid

    KeyValueStore.createInstance get put del
