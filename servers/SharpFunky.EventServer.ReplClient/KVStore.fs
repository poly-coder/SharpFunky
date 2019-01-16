module KVStore

open Microsoft.Extensions.DependencyInjection
open Orleans
open FSharp.Control.Tasks.V2
open SharpFunky
open SharpFunky.EventServer.Interfaces
open SharpFunky.Repl

let commands context = task {
    match context.parameters with
    | containerName :: _ ->
        let client = context.services.GetService<IClusterClient>()
        let kvstore = client.GetGrain<IKeyValueStoreGrain> containerName
        do printfn "Connected to KeyValue store '%s'" containerName
        let prompt = sprintf "kvstore(%s)" containerName

        let get ctx = task {
            match ctx.parameters with
            | key :: _ ->
                match! kvstore.get key with
                | Some bytes ->
                    try
                        let text = String.fromUtf8 bytes
                        printfn "'%s'" text
                    with _ ->
                        let base64 = String.toBase64 bytes
                        printfn "base64:%s" base64
                | None ->
                    printfn "Value not found"

            | [] ->
                printfn "Key is required. Try %s <key>" ctx.command
            return KeepContext
        }

        let put ctx = task {
            match ctx.parameters with
            | key :: strValue :: _ ->
                let bytes =
                    if strValue |> String.startsWith "base64:" then
                        strValue |> String.substringFrom "base64:".Length |> String.fromBase64
                    else
                        strValue |> String.toUtf8
                do! kvstore.put (key, bytes)

            | [_] ->
                printfn "Value is required. Try %s <key> <value>" ctx.command

            | [] ->
                printfn "Key and value are required. Try %s <key> <value>" ctx.command
            return KeepContext
        }

        let remove ctx = task {
            match ctx.parameters with
            | key :: _ ->
                do! kvstore.remove key

            | [] ->
                printfn "Key is required. Try %s <key>" ctx.command
            return KeepContext
        }

        let run =
            [ 
                "get", get
                "put", put
                "remove", remove
            ]
            |> subCommands
        return PushContext(prompt, run)

    | [] ->
        printfn "* Missing containerName parameter. Try %s <key-value-container>" context.command
        return KeepContext
}
