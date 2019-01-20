// Learn more about F# at http://fsharp.org

open System
open FSharp.Control.Tasks.V2
open SharpFunky
open SharpFunky.Repl
open Grpc.Core
open System.Transactions
open KVStore.Protocols.BinaryKVStore
open KVStore.Clients
open KVStore

let runConnectBin (ctx: CommandRunContext) = task {
    let channelOpt =
        match ctx.parameters with
        | [hostPort] -> Channel(hostPort, ChannelCredentials.Insecure) |> Some
        | [host; port] -> Channel(sprintf "%s:%s" host port, ChannelCredentials.Insecure) |> Some
        | _ -> None

    match channelOpt with
    | None ->
        printfn "Try %s <host> <port>" ctx.command
        return KeepContext
    | Some channel ->
        let client = BinaryKVStoreService.BinaryKVStoreServiceClient(channel)
        let kvstore = BinaryKVStoreClient(client) :> IKVStoreService<string, byte[]>
        printfn "Connected to KVStore at %s" channel.Target

        let runGet (ctx: CommandRunContext) = task {
            match ctx.parameters with
            | [key] ->
                match! kvstore.getValue key with
                | Some value ->
                    try
                        let text = String.fromUtf8 value
                        printfn "'%s'" text
                    with _ -> 
                        let base64 = String.toBase64 value
                        printfn "base64:%s" base64
                | None ->
                    printfn "Not Found"
            | _ ->
                printfn "Try %s <key>" ctx.command
            return KeepContext
        }
        let runPut (ctx: CommandRunContext) = task {
            match ctx.parameters with
            | [key; value] ->
                let bytes = 
                    if String.startsWith "base64:" value then
                        String.fromBase64 (String.substringFrom 7 value)
                    else
                        String.toUtf8 value
                do! kvstore.putValue key bytes
            | _ ->
                printfn "Try %s <key> <value>" ctx.command
            return KeepContext
        }
        let runDelete (ctx: CommandRunContext) = task {
            match ctx.parameters with
            | [key] ->
                do! kvstore.deleteValue key
            | _ ->
                printfn "Try %s <key>" ctx.command
            return KeepContext
        }

        let prompt = sprintf "bin[%s]" channel.ResolvedTarget
        let runner =
            [ "get", runGet
              "put", runPut
              "del", runDelete ]
            |> subCommands
        return PushContext(prompt, runner)
}

let commands = 
    [ "binary", runConnectBin ]
    |> subCommands

[<EntryPoint>]
let main argv =
    let t = task {
        do! runCommandRepl Unchecked.defaultof<_> commands
    }

    t.Wait()
    0 // return an integer exit code
