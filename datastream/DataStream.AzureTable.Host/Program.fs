open System
open Microsoft.Extensions.Configuration
open DataStream.Protocols.BinaryDataStream
open Grpc.Core
open DataStream.Azure.Storage
open DataStream.Services

type ServerConfig() =
    member val Host = "localhost" with get, set
    member val Port = 50200 with get, set

let loadConfiguration argv =
    ConfigurationBuilder()
        .AddJsonFile("appsettings.json", optional = false, reloadOnChange = false)
        .AddJsonFile("appsettings.production.json", optional = true, reloadOnChange = false)
        .AddEnvironmentVariables()
        .AddCommandLine(argv: string[])
        .Build()

[<EntryPoint>]
let main argv =
    let configuration = loadConfiguration argv
    let storeOptions = configuration.GetSection("DataStream").Get<AzureTableDataStreamOptions>()
    let serverConfig = configuration.GetSection("Server").Get<ServerConfig>()
    let server = new Server()
    do ServerPort(serverConfig.Host, serverConfig.Port, ServerCredentials.Insecure)
        |> server.Ports.Add
        |> ignore
    do storeOptions
        |> AzureTableDataStream
        |> BinaryDataStreamServiceImpl
        |> BinaryDataStreamService.BindService
        |> server.Services.Add
    do printfn "Connecting ..."
    do server.Start()
    do printfn "Connected!"
    do printfn "Press enter to stop server..."
    do Console.ReadLine() |> ignore
    do server.ShutdownAsync().GetAwaiter().GetResult()
    0
