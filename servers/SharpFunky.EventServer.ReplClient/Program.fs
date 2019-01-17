open System
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Orleans
open Orleans.Hosting
open Orleans.Streams
open FSharp.Control.Tasks.V2
open SharpFunky
open SharpFunky.EventServer.Interfaces
open System.Text.RegularExpressions
open Microsoft.Extensions.DependencyInjection
open SharpFunky.Repl
open SharpFunky.EventServer.Azure.Grains

/// Repl commands

let readCommand (ctx: CommandRunContext) = task {
    let client = ctx.services.GetService<IClusterClient>()
    let chatRoomId = Guid("{ABA53A13-DF86-4D43-A0A9-ADF9D4E5E904}")
    let streamProvider = client.GetStreamProvider("SMSProvider")
    let stream = streamProvider.GetStream<string>(chatRoomId, "text")
    let ts = TaskCompletionSource()
    let mutable subscriptionRef = Unchecked.defaultof<StreamSubscriptionHandle<_>>
    let! subscription =
        stream.SubscribeAsync (
            Func<_, _, _>(fun txt token ->
                task {
                    printfn "Other: %s [%O]" txt token
                    if txt = "bye" then
                        do! subscriptionRef.UnsubscribeAsync()
                        ts.TrySetResult() |> ignore
                } :> Task
            ),
            Func<_>(fun () ->
                task {
                    printfn "Other Completed"
                    do! subscriptionRef.UnsubscribeAsync()
                    ts.TrySetResult() |> ignore
                } :> Task
            )
        )
    subscriptionRef <- subscription
    do! ts.Task
    return KeepContext
}

let writeCommand (ctx: CommandRunContext) = task {
    let client = ctx.services.GetService<IClusterClient>()
    let chatRoomId = Guid("{ABA53A13-DF86-4D43-A0A9-ADF9D4E5E904}")
    let streamProvider = client.GetStreamProvider("SMSProvider")
    let stream = streamProvider.GetStream<string>(chatRoomId, "text")
    let completeCommand (ctx: CommandRunContext) = task {
        do! stream.OnCompletedAsync()
        return KeepContext
    }
    let sendText (ctx: CommandRunContext) = task {
        let text = String.joinWith " " ctx.parameters
        do! stream.OnNextAsync(text)
        if text = "bye" then
            do! stream.OnCompletedAsync()
            return PopContext
        else
            return KeepContext
    }
    let run = 
        [
            "send", sendText
            "x", completeCommand
            "exit", completeCommand
        ]
        |> subCommands
    return PushContext("write", run)
}

let commands =
    [
        // "kvstore", KVStore.commands
        "events", Events.commands
        "read", readCommand
        "write", writeCommand
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
                .AddApplicationPart(typeof<BlobKeyValueStoreGrain>.Assembly)
                .AddApplicationPart(typeof<IKeyValueStoreGrain>.Assembly)
                .WithCodeGeneration() 
            |> ignore
        )
        .ConfigureLogging(fun logging -> 
            // logging.AddConsole() |> ignore
            ()
        )
        .AddSimpleMessageStreamProvider("SMSProvider")
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