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

[<Literal>]
let REQUIRED = "REQUIRED"

let readConfigSimpleMap configKey (config: #IConfigurationSection) defaultMap =
    let configItem = config.GetSection(sprintf "Configs:%s" configKey) |> Option.ofObj
    let defaultsItem = maybe {
        let! configItem = configItem
        let! useDefaults = configItem.["UseDefaults"] |> String.nonWhiteSpace
        let section = config.GetSection(sprintf "Defaults:%s" useDefaults)
        if isNull section then
            invalidOp (sprintf "Missing configuration path %s:Defaults:%s" config.Path useDefaults)
        return section
    }

    defaultMap
    |> Seq.map (fun (key, value) ->
        orElse {
            return! configItem |> Option.bind (fun c -> c.[key] |> String.nonWhiteSpace)
            return! defaultsItem |> Option.bind (fun c -> c.[key] |> String.nonWhiteSpace)
        }
        |> Option.matches (Tup2.withFst key) (fun() ->
            match value with
            | REQUIRED ->
                invalidOp (sprintf "Missing configuration path %s:Configs:%s:%s" config.Path configKey key)
            | value -> key, value
        )
    )
    |> Map.ofSeq

let createMessageStoreConfigLocator (config: #IConfigurationSection) (svc: IServiceProvider) =
    { new IMessageStoreGrainConfigLocator with
        member this.getConfig name = task {
            let options =
                [ "ReadLimit", "100"
                  "StorageConnectionString", REQUIRED
                  "TableName", REQUIRED
                ]
                |> readConfigSimpleMap name config
            let dataStreamOptions = AzureTableDataStreamOptions()
            dataStreamOptions.StorageConnectionString <- options |> Map.find "StorageConnectionString"
            dataStreamOptions.TableName <- options |> Map.find "TableName"
            let readLimit = options |> Map.find "ReadLimit" |> Int32.parse
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
            config.GetSection("MessageStores")
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
