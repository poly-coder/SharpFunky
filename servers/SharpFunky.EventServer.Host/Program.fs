open System
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Orleans
open Orleans.Hosting
open FSharp.Control.Tasks.V2
open SharpFunky.EventServer.Interfaces
open SharpFunky.EventServer.Azure.Grains

let section name (serv: IServiceProvider) =
    serv.GetService<IConfigurationRoot>().GetSection(name).Get<_>()

let getEventStreamOptions sectionName (serv: IServiceProvider) =
    { new ITableEventStreamServiceOptions with
        member __.loadOptions configName =
            let config = serv.GetService<IConfigurationRoot>()
            let subSectionName = sprintf "%s:%s" sectionName configName
            match config.GetSection(subSectionName) with
            | null ->
                invalidArg "configName" (sprintf "Section %s doesn't exist" subSectionName)
            | subSection ->
                { connectionString = subSection.Item("connectionString") }
    }

let buildHost argv =
    let configuration =
        ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional = false, reloadOnChange = false)
            .AddEnvironmentVariables("HOST_")
            .AddCommandLine(argv: string[])
            .Build()

    SiloHostBuilder()
        .UseLocalhostClustering()
        .ConfigureServices(fun services ->
            services
                .AddSingleton<IConfigurationRoot>(configuration)
                .AddSingleton<BlobKeyValueStoreOptions>(section "BlobKeyValueStore")
                .AddSingleton<ITableEventStreamServiceOptions>(getEventStreamOptions "EventStreamServices")
            |> ignore
        )
        .ConfigureApplicationParts(fun parts ->
            parts
                .AddApplicationPart(typeof<BlobKeyValueStoreGrain>.Assembly)
                .AddApplicationPart(typeof<IKeyValueStoreGrain>.Assembly)
                .WithCodeGeneration() 
            |> ignore
        )
        .ConfigureLogging(fun logging -> logging.AddConsole() |> ignore)
        .Build()

[<EntryPoint>]
let main argv =
    task {
        let host = buildHost argv
        do! host.StartAsync()
        printfn "Press any keys to terminate..."
        Console.Read() |> ignore
        do! host.StopAsync()
        printfn "SiloHost is stopped"
    } |> fun t -> t.GetAwaiter().GetResult()
    0 // return an integer exit code
