namespace EventStore.GrainInterfaces

open System.Threading.Tasks
open Orleans
open System
open SharpFunky

type MessageData = {
    data: byte[]
    metadata: Map<string, string>
    sequence: uint64
}

type MessageDataSegment = {
    messages: MessageData list
    nextSequence: uint64 option
}

type AppendMessagesResult = {
    firstSequence: uint64
    nextSequence: uint64
}

type IMessageStoreGrain =
    inherit IGrainWithStringKey

    abstract ReadMessages: fromSequence: uint64 -> Task<MessageDataSegment>
    abstract AppendMessages: events: MessageData list -> Task<AppendMessagesResult>

module MessageStoreExtensions =
    open EventStore.Abstractions
    open FSharp.Control.Tasks.V2

    let toGrainKey configKey streamId =
        sprintf "%s--%s" configKey streamId

    let fromGrainKey grainKey =
        String.contentAround "--" grainKey

    type MessageStoreImpl(grain: IMessageStoreGrain) =
        let toMessageData (message: Message): MessageData = {
            data = message.data
            metadata = message.metadata
            sequence = message.sequence
        }

        let fromMessageData (message: MessageData): Message = {
            data = message.data
            metadata = message.metadata
            sequence = message.sequence
        }

        interface IMessageStore with
            member this.readMessages fromSequence =
                task {
                    let! segment = grain.ReadMessages(fromSequence)
                    let segment': MessageSegment = {
                        messages = segment.messages |> List.map fromMessageData
                        nextSequence = segment.nextSequence
                    }
                    return segment'
                } |> Async.AwaitTask

            member this.appendMessages messages =
                task {
                    let messages' = messages |> List.map toMessageData
                    let! result'= grain.AppendMessages(messages')
                    let result: EventStore.Abstractions.AppendMessagesResult = {
                        firstSequence = result'.firstSequence
                        nextSequence = result'.nextSequence
                    }
                    return result
                } |> Async.AwaitTask

    type MessageStoreLocatorGuidImpl(configKey: string, grainFactory: IGrainFactory) =
        
        interface IMessageStoreLocator<Guid> with
            member this.getMessageStore key =
                task {
                    let grainKey = toGrainKey configKey (key.ToString("N"))
                    let grain = grainFactory.GetGrain<IMessageStoreGrain>(grainKey)
                    let impl = MessageStoreImpl(grain)
                    return impl :> IMessageStore
                } |> Async.AwaitTask

    type MessageStoreLocatorStringImpl(configKey: string, grainFactory: IGrainFactory) =
        
        interface IMessageStoreLocator<string> with
            member this.getMessageStore key =
                task {
                    let grainKey = toGrainKey configKey key
                    let grain = grainFactory.GetGrain<IMessageStoreGrain>(grainKey)
                    let impl = MessageStoreImpl(grain)
                    return impl :> IMessageStore
                } |> Async.AwaitTask
