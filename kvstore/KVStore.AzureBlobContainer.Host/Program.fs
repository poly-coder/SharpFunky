open System
open Microsoft.Extensions.Configuration
open KVStore.Azure.Storage
open KVStore.Protocols.BinaryKVStore
open Grpc.Core
open KVStore.Services

type ServerConfig() =
    member val Host = "localhost" with get, set
    member val Port = 50100 with get, set

let loadConfiguration argv =
    ConfigurationBuilder()
        .AddJsonFile("appsettings.json", optional = false, reloadOnChange = false)
        .AddJsonFile("appsettings.production.json", optional = true, reloadOnChange = false)
        .AddEnvironmentVariables("KVStoreService")
        .AddCommandLine(argv: string[])
        .Build()

[<EntryPoint>]
let main argv =
    let configuration = loadConfiguration argv
    let kvstoreOptions = configuration.GetSection("BlobContainerKVStore").Get<AzureBlobContainerKVStoreOptions>()
    let serverConfig = configuration.GetSection("Server").Get<ServerConfig>()
    let server = new Server()
    do ServerPort(serverConfig.Host, serverConfig.Port, ServerCredentials.Insecure)
        |> server.Ports.Add
        |> ignore
    do kvstoreOptions
        |> AzureBlobContainerKVStore
        |> BinaryKVStoreServiceImpl
        |> KVStore.Protocols.BinaryKVStore.BinaryKVStoreService.BindService
        |> server.Services.Add
    do printfn "Connecting ..."
    do server.Start()
    do printfn "Connected!"
    do printfn "Press enter to stop server..."
    do Console.ReadLine() |> ignore
    do server.ShutdownAsync().GetAwaiter().GetResult()
    0
