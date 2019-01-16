module Events

open Common
open Orleans
open SharpFunky
open SharpFunky.Repl
open FSharp.Control.Tasks.V2
open SharpFunky.EventServer.Interfaces
open Microsoft.Extensions.DependencyInjection
open System.Threading.Tasks

let streamStatusCommand (streamGrain: IEventStreamGrain) context = task {
    match context.parameters with
    | [] ->
        let! status = streamGrain.getStatus()
        do printfn "getStatus: '%A'" status
    | _ -> 
        printfn "Try %s" context.command
    return KeepContext
}

let streamCommitOneCommand (streamGrain: IEventStreamGrain) context = task {
    match context.parameters with
    | MapAndBytesParameters (meta, data) ->
        let eventData = { meta = meta; data = data }
        do! streamGrain.commitEvents [eventData]
    | _ -> 
        printfn "Try %s <data> [key1=value1] ..." context.command
    return KeepContext
}

let readCommand (client: IClusterClient) (streamGrain: IEventStreamGrain) context = task {
    match context.parameters with
    | OneParsedOptionalParameter 5 Int32.tryParse count ->
        let! readerGrain = streamGrain.createReader(ReadFromStart)
        do printfn "Connected to EventStreamReader '%O'" <| streamGrain.GetPrimaryKey()
        do! readerGrain.addReadQuota count
        let remaining = ref count
        let ts = TaskCompletionSource()
        let observer =
            { new IEventStreamReaderGrainObserver with
                member __.eventsAvailable events =
                    do printfn "Read '%A'" events
                    remaining := !remaining - (List.length events)
                    if !remaining <= 0 then
                        ts.TrySetResult () |> ignore
            }
        let! observer = client.CreateObjectReference(observer)
        do! readerGrain.subscribe observer
        do! ts.Task
        do! readerGrain.unsubscribe observer
    | _ -> 
        printfn "Try %s [count]" context.command
    return KeepContext
}


let streamCommand client (nsGrain: IEventStreamNamespaceGrain) context = task {
    match context.parameters with
    | OneParameter streamId ->
        let! streamGrain = nsGrain.getStream(streamId)
        do printfn "Connected to EventStream '%s'" <| streamGrain.GetPrimaryKeyString()
        let prompt = sprintf "stream(%s)" streamId
        let run =
            [
                "status", streamStatusCommand streamGrain
                "commit", streamCommitOneCommand streamGrain
                "read", readCommand client streamGrain
            ]
            |> subCommands
        return PushContext(prompt, run)
    | _ -> 
        printfn "Try %s [streamId]" context.command
        return KeepContext
}

let nameSpaceCommand client (service: IEventStreamServiceGrain) context = task {
    match context.parameters with
    | OneParameter nameSpace ->
        let! nsGrain = service.getNamespace(nameSpace)
        do printfn "Connected to EventStreamNamespace '%s'" <| nsGrain.GetPrimaryKeyString()
        let prompt = sprintf "ns(%s)" nameSpace
        let run = ["stream", streamCommand client nsGrain] |> subCommands
        return PushContext(prompt, run)
    | _ -> 
        printfn "Try %s [namespace]" context.command
        return KeepContext
}

let commands context = task {
    match context.parameters with
    | OneOptionalParameter "default" configName ->
        let client = context.services.GetService<IClusterClient>()
        let service = client.GetGrain<IEventStreamServiceGrain> configName
        do printfn "Connected to EventStreamService '%s'" configName
        let prompt = sprintf "events(%s)" configName
        let run = ["ns", nameSpaceCommand client service] |> subCommands
        return PushContext(prompt, run)
    | _ -> 
        printfn "Try %s [configName]" context.command
        return KeepContext
}

//let commands context = task {
//    match context.parameters with
//    | partition :: _ ->
//        let client = context.services.GetService<IClusterClient>()
//        let estream = client.GetGrain<IEventStreamGrain> partition
//        do printfn "Connected to EventStream '%s'" partition
//        let prompt = sprintf "events(%s)" partition

//        let getStatus ctx = task {
//            let! status = estream.getStatus()
//            printfn "Event stream status: %A" status
//            return KeepContext
//        }

//        let commitOne ctx = task {
//            let paramMap =
//                ctx.parameters
//                |> Seq.bindOpt (fun p ->
//                    let pos = p |> String.indexOf "="
//                    if pos < 0 then None
//                    else Some (String.substring 0 pos p, String.substringFrom (pos+1) p))
//                |> Map.ofSeq
//            let content =
//                ctx.parameters
//                |> Seq.filter (fun p -> p.Contains("=") |> not)
//                |> Seq.toList
//                |> function [ c ] -> Some c | _ -> None
//            match content with
//            | None ->
//                printfn "Event content is required. Add a single parameter without '='"
//                return KeepContext
//            | Some content ->
//                let bytes =
//                    if content.StartsWith "base64:" then
//                        content.Substring(7) |> String.fromBase64
//                    else
//                        content |> String.toUtf8
//                let eventData: EventData = { meta = paramMap; data = bytes }
//                do! estream.commitEvents [eventData]
//                return KeepContext
//        }

//        let run =
//            [ 
//                "status", getStatus
//                "commit", commitOne
//            ]
//            |> subCommands
//        return PushContext(prompt, run)
//    | [] ->
//        printfn "* Missing partition parameter. Try %s <partition>" context.command
//        return KeepContext
//}

