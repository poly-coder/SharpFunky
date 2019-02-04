namespace EventStore.Grains.PropertyGrains

open Orleans
open SharpFunky
open EventStore.Abstractions
open EventStore.GrainInterfaces
open FSharp.Control.Tasks.V2
open System.Threading.Tasks
open DataStream

type IBinaryDataStreamService = IDataStreamService<uint64, byte[], Map<string, string>>

type MessageStoreGrainConfig = {
    readLimit: int
    getDataStream: unit -> Task<IBinaryDataStreamService>
}

type IMessageStoreGrainConfigLocator =
    abstract getConfig: name: string -> Task<MessageStoreGrainConfig>

type DataStreamService_MessageStoreGrain
    (
        configLocator: IMessageStoreGrainConfigLocator
    ) =
    inherit Grain()

    let mutable configKey = ""
    let mutable streamId = ""
    let mutable nextSequence = 0UL
    let mutable dataStream = Unchecked.defaultof<IBinaryDataStreamService>
    let mutable config = Unchecked.defaultof<MessageStoreGrainConfig>

    let toMessageData (item: DataStreamItem<_, _, _>): MessageData = {
        data = item.data
        metadata = item.metadata
        sequence = item.sequence
    }

    let fromMessageData (message: MessageData): DataStreamItem<_, _, _> = {
        data = message.data
        metadata = message.metadata
        sequence = message.sequence
    }

    let toMetadata (nextSequence: uint64) =
        [ "NextSequence", sprintf "%d" nextSequence ]
        |> Map.ofSeq

    let fromMetadata (metadata: Map<string, string>) =
        let nextSequence =
            metadata
            |> Map.tryFind "NextSequence"
            |> Option.bind UInt64.tryParse
            |> Option.defaultValue 0UL
        nextSequence

    override this.OnActivateAsync() =
        task {
            let grainKey = this.GetPrimaryKeyString()
            match MessageStoreExtensions.fromGrainKey grainKey with
            | None ->
                return invalidOp (sprintf "Malformed MessageStore key: %s" grainKey)
            | Some(configKey', streamId') ->
                let! config' = configLocator.getConfig configKey'
                let! dataStream' = config'.getDataStream()
                let! status = dataStream'.getStatus (GetStatusReq.create streamId')
                let nextSequence' = fromMetadata status.metadata

                configKey <- configKey'
                streamId <- streamId'
                nextSequence <- nextSequence'
                config <- config'
                dataStream <- dataStream'
        } :> Task


    interface IMessageStoreGrain with
        member this.ReadMessages fromSequence = task {
            let request =
                ReadReq.create fromSequence
                |> ReadReq.setStreamId streamId
                |> ReadReq.setLimit config.readLimit

            let! response = dataStream.read request

            let nextSequence =
                if response.reachedEnd then None else Some response.nextSequence
            let messages =
                response.items
                |> List.map toMessageData
            return {
                messages = messages
                nextSequence = nextSequence
            }
        }

        member this.AppendMessages events = task {
            let count = List.length events
            if count = 0 then
                return {
                    firstSequence = nextSequence
                    nextSequence = nextSequence
                }
            else
            let items' =
                events
                |> List.mapi (fun i e -> fromMessageData { e with sequence = nextSequence + uint64 i })
            let nextSequence' = nextSequence + uint64 count
            let metadata = toMetadata nextSequence'
            let request =
                AppendReq.create metadata items'
                |> AppendReq.setStreamId streamId

            let! response = dataStream.append request

            let firstSequence = nextSequence
            nextSequence <- nextSequence'

            return {
                firstSequence = firstSequence
                nextSequence = nextSequence
            }
        }


