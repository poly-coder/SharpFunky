open System
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Orleans
open Orleans.Hosting
open FSharp.Control.Tasks.V2
open SharpFunky
open SharpFunky.EventServer.Interfaces
open System.Text.RegularExpressions
open SharpFunky.Repl

/// Repl commands
let getService<'s> (serv: IServiceProvider) = serv.GetService<'s>()
let section<'s> name (serv: IServiceProvider) =
    (getService<IConfigurationRoot> serv).GetSection(name).Get<'s>()

let kvstoreCommands context = task {
    match context.parameters with
    | containerName :: _ ->
        let client = getService<IClusterClient> context.services
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

let eventsCommands context = task {
    match context.parameters with
    | partition :: _ ->
        let client = getService<IClusterClient> context.services
        let estream = client.GetGrain<IEventStreamGrain> partition
        do printfn "Connected to EventStream '%s'" partition
        let prompt = sprintf "events(%s)" partition

        let getStatus ctx = task {
            let! status = estream.getStatus()
            printfn "Event stream status: %A" status
            return KeepContext
        }

        let commitOne ctx = task {
            let paramMap =
                ctx.parameters
                |> Seq.bindOpt (fun p ->
                    let pos = p |> String.indexOf "="
                    if pos < 0 then None
                    else Some (String.substring 0 pos p, String.substringFrom (pos+1) p))
                |> Map.ofSeq
            let content =
                ctx.parameters
                |> Seq.filter (fun p -> p.Contains("=") |> not)
                |> Seq.toList
                |> function [ c ] -> Some c | _ -> None
            match content with
            | None ->
                printfn "Event content is required. Add a single parameter without '='"
                return KeepContext
            | Some content ->
                let bytes =
                    if content.StartsWith "base64:" then
                        content.Substring(7) |> String.fromBase64
                    else
                        content |> String.toUtf8
                let eventData: EventData = { meta = paramMap; data = bytes }
                do! estream.commitEvents [eventData]
                return KeepContext
        }

        let run =
            [ 
                "status", getStatus
                "commit", commitOne
            ]
            |> subCommands
        return PushContext(prompt, run)
    | [] ->
        printfn "* Missing partition parameter. Try %s <partition>" context.command
        return KeepContext
}

let commands =
    [
        "kvstore", kvstoreCommands
        "events", eventsCommands
    ]
    |> subCommands

/// Hosting

let buildClient argv =
    let configuration =
        ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional = false, reloadOnChange = false)
            .AddEnvironmentVariables("REPL_")
            .AddCommandLine(argv: string[])
            .Build()

    ClientBuilder()
        .UseLocalhostClustering()
        .ConfigureServices(fun services ->
            services
                .AddSingleton<IConfigurationRoot>(configuration)
            |> ignore
        )
        .ConfigureApplicationParts(fun parts ->
            parts
                .AddApplicationPart(typeof<IKeyValueStoreGrain>.Assembly)
                .WithCodeGeneration() 
            |> ignore
        )
        .ConfigureLogging(fun logging -> 
            // logging.AddConsole() |> ignore
            ()
        )
        .Build()

[<EntryPoint>]
let main argv =
    let t = task {
        use client = buildClient argv
        do! client.Connect( fun (ex: Exception) -> task {
            do! Task.Delay(1000)
            return true
        })
        printfn "Client successfully connect to silo host"
        do! runCommandRepl client.ServiceProvider commands
    }

    t.Wait()

    // Console.ReadKey() |> ignore

    0