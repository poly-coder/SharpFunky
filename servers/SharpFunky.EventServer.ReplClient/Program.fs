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
open SharpFunky.EventServer.Azure.Grains

/// Repl commands

let commands =
    [
        // "kvstore", KVStore.commands
        "events", Events.commands
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