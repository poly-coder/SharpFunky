open System
open FSharp.Control.Tasks.V2
open System.Threading.Tasks
open SharpFunky
open SharpFunky.AzureStorage
open Microsoft.Extensions.Configuration
open SharpFunky.Storage
open SharpFunky.EventStorage
open SharpFunky.EventStorage.Stateless
open SharpFunky.EventStorage.StatelessEventStream

type AzureStorageAccountConfig() =
    member val ConnectionString = "UseDevelopmentStorage=true" with get, set

let configuration argv =
    ConfigurationBuilder()
        .AddJsonFile("appsettings.json", optional = false, reloadOnChange = false)
        .AddJsonFile("appsettings.development.json", optional = true, reloadOnChange = false)
        .AddEnvironmentVariables("SHARPFUNKY_")
        .AddCommandLine(argv: string[])
        .Build()

let testStorageEventStream argv =
    task {
        let conf = configuration argv
        let accountConfig: AzureStorageAccountConfig = conf.GetSection("AzureStorageAccount").Get()
        let account = Account.parse accountConfig.ConnectionString
        let client = account.CreateCloudTableClient()
        let tableFactory = AzureTables.createEventStreamFromTable client
        let! partitionFactory = tableFactory.create "eventstream"
        let sample1 = partitionFactory.create "sample1"
        
        let! status = sample1.status()
        printfn "Stream status: %A" status
        printfn ""

        let readRequest: ReadEventsRequest = {
            fromSequence = None
            limit = Some 10
            reverse = false
        }
        let! readResponse = sample1.read readRequest
        printfn "Read 10: %A" readResponse
        printfn ""

        let writeRequest: WriteEventsRequest = {
            startSequence = status.nextSequence
            meta = [ "IsFrozen", MetaBool false ] |> Map.ofSeq
            events = [
                {
                    meta = [] |> Map.ofSeq
                    data = EmptyEvent
                }
                {
                    meta = [
                        "AggId", MetaString (Guid.NewGuid().ToString("N"))
                        "Timestamp", MetaLong DateTime.UtcNow.Ticks
                        "Encodings", MetaStrings ["gzip"; "base64"; "utf8"]
                    ] |> Map.ofSeq
                    data = BinaryEvent (String.toUtf8 "Hello world")
                }
                {
                    meta = [
                        "AggId", MetaString (Guid.NewGuid().ToString("N"))
                        "Timestamp", MetaLong DateTime.UtcNow.Ticks
                    ] |> Map.ofSeq
                    data = StringEvent "Hello world"
                }
            ]
        }
        let! writeResponse = sample1.write writeRequest
        printfn "Write 10: %A" writeResponse
        printfn ""

        return ()
    }
    |> fun t -> t.GetAwaiter().GetResult()


[<EntryPoint>]
let main argv =
    testStorageEventStream argv
    0 // return an integer exit code
