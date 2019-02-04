open System
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.Logging
open EventStore
open EventStore.Abstractions
open EventStore.GrainInterfaces
open EventStore.Grains.PropertyGrains
open Orleans
open Orleans.Runtime.Configuration
open Orleans.Hosting
open FSharp.Control.Tasks.V2

let loadConfiguration argv =
    ConfigurationBuilder()
        .AddJsonFile("appsettings.json", optional = false, reloadOnChange = false)
        .AddJsonFile("appsettings.production.json", optional = true, reloadOnChange = false)
        .AddEnvironmentVariables("KVStoreService")
        .AddCommandLine(argv: string[])
        .Build()

let buildSiloHost config =
      let builder = new SiloHostBuilder()
      builder
        .UseLocalhostClustering()
        .ConfigureApplicationParts(fun parts ->
            parts
                .AddApplicationPart(typeof<IPropertiesManagerGrain>.Assembly)
                .AddApplicationPart(typeof<PropertiesManagerGrain>.Assembly)
                .WithCodeGeneration() |> ignore)
        .ConfigureLogging(fun logging -> logging.AddConsole() |> ignore)
        .Build()

[<EntryPoint>]
let main argv =
    // let configuration = loadConfiguration argv
    task {
        let host = buildSiloHost ()
        do! host.StartAsync ()

        printfn "Press any keys to terminate..."
        Console.Read() |> ignore

        do! host.StopAsync()

        printfn "SiloHost is stopped"
    } |> fun t -> t.Wait()
    0
