open System
open Microsoft.Extensions.Configuration
open DataStream.Protocols.BinaryDataStream
open Grpc.Core
open SharpFunky.AzureStorage
open SharpFunky.AzureStorage.Tables
open FSharp.Control.Tasks.V2
open SharpFunky

type ServerConfig() =
    member val Host = "localhost" with get, set
    member val Port = 50200 with get, set

type DataStreamConfig() =
    member val StorageConnectionString = "UseDevelopmentStorage=true" with get, set
    member val TableName = "datastream" with get, set

type DataStoreServiceImpl(config: DataStreamConfig) =
    inherit BinaryDataStoreService.BinaryDataStoreServiceBase()
    let account = Account.parse config.StorageConnectionString
    let client = account.CreateCloudTableClient()
    let table = client.GetTableReference(config.TableName)

    override this.GetStatus(request, context) = task {
        let! result =
            Cancellable.executeRetrieve
                context.CancellationToken Unchecked.defaultof<_> Unchecked.defaultof<_>
                request.StreamId "A_STATUS" table

        match result with
        | Some entity ->
            let metadata = Entity.decodeStringMap "Meta_" entity
            return
                DataStreamStatus()
                |> tee (fun status ->
                    status.StreamId <- request.StreamId
                    status.Exists <- true
                    status.Etag <- entity.ETag
                    metadata
                    |> Map.toSeq
                    |> Seq.iter (fun (key, value) -> status.Metadata.Add(key, value)))

        | None ->
            return
                DataStreamStatus()
                |> tee (fun status ->
                    status.StreamId <- request.StreamId
                    status.Exists <- false)
    }

    override this.SaveStatus(request, context) = task {
    }

    override this.Append(request, context) = task {
    }

    override this.Read(request, context) = task {
    }

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
    let dataStreamConfig = configuration.GetSection("DataStream").Get<DataStreamConfig>()
    let serverConfig = configuration.GetSection("Server").Get<ServerConfig>()
    let server = new Server()
    do ServerPort(serverConfig.Host, serverConfig.Port, ServerCredentials.Insecure)
        |> server.Ports.Add
        |> ignore
    do dataStreamConfig
        |> DataStoreServiceImpl
        |> BinaryDataStoreService.BindService
        |> server.Services.Add
    do printfn "Connecting ..."
    do server.Start()
    do printfn "Connected!"
    do printfn "Press enter to stop server..."
    do Console.ReadLine() |> ignore
    do server.ShutdownAsync().GetAwaiter().GetResult()
    0 // return an integer exit code
