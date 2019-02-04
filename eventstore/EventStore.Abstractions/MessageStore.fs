namespace EventStore.Abstractions

type Message = {
    data: byte[]
    metadata: Map<string, string>
    sequence: uint64
}

type MessageSegment = {
    messages: Message list
    nextSequence: uint64 option
}

type AppendMessagesResult = {
    firstSequence: uint64
    nextSequence: uint64
}

type IMessageStore =
    abstract readMessages: fromSequence: uint64 -> Async<MessageSegment>
    abstract appendMessages: messages: Message list -> Async<AppendMessagesResult>

type IMessageStoreLocator<'key> =
    abstract getMessageStore: key: 'key -> Async<IMessageStore>

