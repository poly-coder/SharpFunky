open System
open SharpFunky
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.Logging
open Microsoft.Extensions.DependencyInjection
open DataStream
open DataStream.Azure.Storage
open EventStore
open EventStore.Abstractions
open EventStore.GrainInterfaces
open EventStore.Grains.PropertyGrains
open Orleans
open Orleans.Runtime.Configuration
open Orleans.Hosting
open FSharp.Control.Tasks.V2

let createMessageStoreConfigLocator (config: #IConfigurationSection) (svc: IServiceProvider) =
    { new IMessageStoreGrainConfigLocator with
        member this.getConfig name = task {
            let configItem = config.GetSection(sprintf "Configs:%s" name)
            if isNull configItem then
                return invalidOp (sprintf "Missing configuration path %s:Configs:%s" config.Path name)
            else

            let providerName = configItem.["UseProvider"]
            let providerConfig = config.GetSection(sprintf "Providers:%s" providerName)
            if isNull configItem then
                return invalidOp (sprintf "Missing configuration path %s:Providers:%s" config.Path providerName)
            else

            let readLimit = Int32.parse <| providerConfig.["ReadLimit"]
            let dataStreamOptions = AzureTableDataStreamOptions()
            dataStreamOptions.StorageConnectionString <- providerConfig.["StorageConnectionString"]
            dataStreamOptions.TableName <- providerConfig.["TableName"]
            let dataStream = AzureTableDataStream(dataStreamOptions) :> IBinaryDataStreamService
            return {
                readLimit = readLimit
                getDataStream = fun () -> task { return dataStream }
            }
        }
    }

let loadConfiguration argv =
    ConfigurationBuilder()
        .AddJsonFile("appsettings.json", optional = false, reloadOnChange = false)
        .AddJsonFile("appsettings.production.json", optional = true, reloadOnChange = false)
        .AddEnvironmentVariables("KVStoreService")
        .AddCommandLine(argv: string[])
        .Build()

let buildSiloHost (config: #IConfiguration) =
      let builder = new SiloHostBuilder()
      builder
        .UseLocalhostClustering()
        .ConfigureApplicationParts(fun parts ->
            parts
                .AddApplicationPart(typeof<IPropertiesManagerGrain>.Assembly)
                .AddApplicationPart(typeof<PropertiesManagerGrain>.Assembly)
                .WithCodeGeneration()
            |> ignore)
        .ConfigureLogging(fun logging -> logging.AddConsole() |> ignore)
        .ConfigureServices(fun services ->
            config.GetSection("InternalMessageStores")
                |> createMessageStoreConfigLocator
                |> services.AddSingleton<IMessageStoreGrainConfigLocator>
                |> ignore
            do ()
        )
        .Build()

[<EntryPoint>]
let main argv =
    let configuration = loadConfiguration argv
    task {
        let host = buildSiloHost configuration
        do! host.StartAsync ()

        printfn "Press any keys to terminate..."
        Console.Read() |> ignore

        do! host.StopAsync()

        printfn "SiloHost is stopped"
    } |> fun t -> t.Wait()
    0
