open System
open System.Threading.Tasks
open FSharp.Control.Tasks.V2
open SharpFunky
open System.Text.RegularExpressions
open SharpFunky.Repl
open DataStream.Protocols.BinaryDataStream
open DataStream.Clients
open DataStream
open Grpc.Core

let runConn (ctx: CommandRunContext) = task {
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
        let client = BinaryDataStreamService.BinaryDataStreamServiceClient(channel)
        let store = BinaryDataStreamClient(client) :> IDataStreamService<uint64, byte[], Map<string, string>>
        printfn "Connected to DataStream service at %s" channel.Target

        let runStream (ctx: CommandRunContext) = task {
            match ctx.parameters with
            | [streamId] ->

                let runStatus (ctx: CommandRunContext) = task {
                    let request =
                        GetStatusReq.empty
                        |> GetStatusReq.setStreamId streamId
                    let! status = store.getStatus request
                    printfn "%A" status
                    return KeepContext
                }

                let runSave (ctx: CommandRunContext) = task {
                    match ctx.parameters with
                    | MapParameters(metadata, []) ->
                        let request =
                            GetStatusReq.empty
                            |> GetStatusReq.setStreamId streamId
                        let! status = store.getStatus request
                        printfn "previous %A" status

                        let request =
                            SaveStatusReq.empty metadata
                            |> SaveStatusReq.setStreamId streamId
                            |> SaveStatusReq.setEtag status.etag

                        let! status = store.saveStatus request
                        printfn "%A" status
                        return KeepContext
                    | _ ->
                        printfn "Try %s <key1=value1> ..." ctx.command
                        return KeepContext
                }

                let runAppend (ctx: CommandRunContext) = task {
                    match ctx.parameters with
                    | MapParameters(metadata, []) ->
                        let request =
                            GetStatusReq.empty
                            |> GetStatusReq.setStreamId streamId
                        let! status = store.getStatus request
                        printfn "previous %A" status

                        let request =
                            SaveStatusReq.empty metadata
                            |> SaveStatusReq.setStreamId streamId
                            |> SaveStatusReq.setEtag status.etag

                        let! status = store.saveStatus request
                        printfn "%A" status
                        return KeepContext
                    | _ ->
                        printfn "Try %s <key1=value1> ..." ctx.command
                        return KeepContext
                }

                let prompt = sprintf "stream[%s]" streamId
                let runner =
                    [ "status", runStatus
                      "save", runSave 
                      "append", runAppend ]
                    |> subCommands
                return PushContext(prompt, runner)
            | _ ->
                printfn "Try %s <stream>" ctx.command
                return KeepContext
        }

        let prompt = sprintf "store[%s]" channel.ResolvedTarget
        let runner =
            [ "stream", runStream ]
            |> subCommands
        return PushContext(prompt, runner)
}

let commands = 
    [ "conn", runConn ]
    |> subCommands

[<EntryPoint>]
let main argv =
    let t = task {
        do! runCommandRepl Unchecked.defaultof<_> commands
    }

    t.Wait()
    0 // return an integer exit code
