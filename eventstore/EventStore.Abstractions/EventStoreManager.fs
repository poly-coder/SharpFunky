namespace EventStore.Abstractions

type EventStoreStatus =
    | Open = 0
    | Frozen = 1
    | Deleting = 2

type EventStoreErrorCode =
    | Unknown = 0
    | DuplicateName = 1
    | IsFrozen = 2
    | IsDeleting = 3
    | PropertyNotFound = 4
    | NameSpaceNotFound = 5
    | TopicNotFound = 6
    | DuplicateEvent = 7
    | QuotaExceeded = 8

type EventStoreErrorInfo = EventStoreErrorInfo of code: EventStoreErrorCode * message: string

type PropertyRef = PropertyRef of propertyName: string
type NameSpaceRef = NameSpaceRef of propertyName: string * nameSpaceName: string
type TopicRef = TopicRef of propertyName: string * nameSpaceName: string * topicName: string

type PropertyInfo = {
    propertyRef: PropertyRef
    status: EventStoreStatus
    accessKey1: string
    accessKey2: string
}

type NameSpaceInfo = {
    nameSpaceRef: NameSpaceRef
    status: EventStoreStatus
}

type TopicInfo = {
    topicRef: TopicRef
    status: EventStoreStatus
}

type ListContinuationToken = string

(* Properties Requests and Responses *)

type GetPropertiesReq = {
    startAt: ListContinuationToken
    propertyNameFilter: string
    statusFilter: EventStoreStatus
    limit: int
}

module GetPropertiesReq =
    let create = {
        startAt = ""
        propertyNameFilter = ""
        statusFilter = EventStoreStatus.Open
        limit = 100
    }


type GetPropertiesRes = {
    properties: PropertyInfo list
    continueAt: ListContinuationToken
    error: EventStoreErrorInfo
}

type CreatePropertyReq = {
    propertyRef: PropertyRef
    frozen: bool
}

type CreatePropertyRes = {
    property: PropertyInfo
    error: EventStoreErrorInfo
}

type FreezePropertyReq = {
    propertyRef: PropertyRef
}

type FreezePropertyRes = {
    error: EventStoreErrorInfo
}

type UnfreezePropertyReq = {
    propertyRef: PropertyRef
}

type UnfreezePropertyRes = {
    error: EventStoreErrorInfo
}

type DeletePropertyReq = {
    propertyRef: PropertyRef
}

type DeletePropertyRes = {
    error: EventStoreErrorInfo
}

type RegenerateAccessKey1Req = {
    propertyRef: PropertyRef
}

type RegenerateAccessKey1Res = {
    accessKey1: string
    error: EventStoreErrorInfo
}

type RegenerateAccessKey2Req = {
    propertyRef: PropertyRef
}

type RegenerateAccessKey2Res = {
    accessKey2: string
    error: EventStoreErrorInfo
}

(* NameSpaces Requests and Responses *)

type GetNameSpacesReq = {
    startAt: string
    propertyRef: PropertyRef
    nameSpaceNameFilter: string
    statusFilter: EventStoreStatus
    limit: int
}

type GetNameSpacesRes = {
    nameSpaces: NameSpaceInfo list
    continueAt: string
    error: EventStoreErrorInfo
}

type CreateNameSpaceReq = {
    nameSpaceRef: NameSpaceRef
    frozen: bool
}

type CreateNameSpaceRes = {
    nameSpace: NameSpaceInfo
    error: EventStoreErrorInfo
}

type FreezeNameSpaceReq = {
    nameSpaceRef: NameSpaceRef
}

type FreezeNameSpaceRes = {
    error: EventStoreErrorInfo
}

type UnfreezeNameSpaceReq = {
    nameSpaceRef: NameSpaceRef
}

type UnfreezeNameSpaceRes = {
    error: EventStoreErrorInfo
}

type DeleteNameSpaceReq = {
    nameSpaceRef: NameSpaceRef
}

type DeleteNameSpaceRes = {
    error: EventStoreErrorInfo
}

(* Topics Requests and Responses *)

type GetTopicsReq = {
    startAt: string
    nameSpaceRef: NameSpaceRef
    topicNameFilter: string
    statusFilter: EventStoreStatus
    limit: int
}

type GetTopicsRes = {
    topics: TopicInfo list
    continueAt: string
    error: EventStoreErrorInfo
}

type CreateTopicReq = {
    topicRef: TopicRef
    frozen: bool
}

type CreateTopicRes = {
    topic: TopicInfo
    error: EventStoreErrorInfo
}

type FreezeTopicReq = {
    topicRef: TopicRef
}

type FreezeTopicRes = {
    error: EventStoreErrorInfo
}

type UnfreezeTopicReq = {
    topicRef: TopicRef
}

type UnfreezeTopicRes = {
    error: EventStoreErrorInfo
}

type DeleteTopicReq = {
    topicRef: TopicRef
}

type DeleteTopicRes = {
    error: EventStoreErrorInfo
}


type IEventStoreManager =
    abstract getProperties: request: GetPropertiesReq -> Async<GetPropertiesRes>
    abstract createProperty: request: CreatePropertyReq -> Async<CreatePropertyRes>
    abstract freezeProperty: request: FreezePropertyReq -> Async<FreezePropertyRes>
    abstract unfreezeProperty: request: UnfreezePropertyReq -> Async<UnfreezePropertyRes>
    abstract deleteProperty: request: DeletePropertyReq -> Async<DeletePropertyRes>
    abstract regenerateAccessKey1: request: RegenerateAccessKey1Req -> Async<RegenerateAccessKey1Res>
    abstract regenerateAccessKey2: request: RegenerateAccessKey2Req -> Async<RegenerateAccessKey2Res>

    abstract getNameSpaces: request: GetNameSpacesReq -> Async<GetNameSpacesRes>
    abstract createNameSpace: request: CreateNameSpaceReq -> Async<CreateNameSpaceRes>
    abstract freezeNameSpace: request: FreezeNameSpaceReq -> Async<FreezeNameSpaceRes>
    abstract unfreezeNameSpace: request: UnfreezeNameSpaceReq -> Async<UnfreezeNameSpaceRes>
    abstract deleteNameSpace: request: DeleteNameSpaceReq -> Async<DeleteNameSpaceRes>

    abstract getTopics: request: GetTopicsReq -> Async<GetTopicsRes>
    abstract createTopic: request: CreateTopicReq -> Async<CreateTopicRes>
    abstract freezeTopic: request: FreezeTopicReq -> Async<FreezeTopicRes>
    abstract unfreezeTopic: request: UnfreezeTopicReq -> Async<UnfreezeTopicRes>
    abstract deleteTopic: request: DeleteTopicReq -> Async<DeleteTopicRes>

