namespace EventStore.Abstractions
open System

type EventData = {
    content: byte[]
    metadata: Map<string, string>
    topicSequence: uint64
    eventId: string
    timestamp: uint64
    // eventType: string
    // aggregateType: string
    // aggregateId: string
    // aggregateVersion: uint32
}

type PersistedEventInfo = {
    topicSequence: uint64
    timestamp: uint64
    eventId: string
}

type GetTopicStatusReq = {
    topicRef: TopicRef
}

type GetTopicStatusRes = {
    error: EventStoreErrorInfo
}

type AppendEventsReq =
    | AppendEventsConnect of topicRef: TopicRef * respondOnSuccess: bool
    | AppendEventsDisconnect
    | AppendEvents of events: EventData list

type AppendEventsRes =
    | AppendEventsConnected
    | AppendEventsDisconnected
    | AppendEventsQuotaAssigned of quota: int
    | AppendEventsSuccess of eventInfos: PersistedEventInfo list
    | AppendEventsError of error: EventStoreErrorInfo

type ReadEventsReq =
    | ReadEventsConnect of topicRef: TopicRef * startAtSequence: uint64
    | ReadEventsDisconnect
    | ReadEventsAssignQuota of quota: int

type ReadEventsRes =
    | ReadEventsConnected
    | ReadEventsDisconnected
    | ReadEventsResult of events: EventData list
    | ReadEventsError of error: EventStoreErrorInfo


type IEventStoreService =
    abstract getTopicStatus: request: GetTopicStatusReq -> Async<GetTopicStatusRes>
    abstract appendEvents: requestStream: IObservable<AppendEventsReq> -> IObservable<AppendEventsRes>
    abstract readEvents: requestStream: IObservable<ReadEventsReq> -> IObservable<ReadEventsRes>
