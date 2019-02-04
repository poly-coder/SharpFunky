namespace EventStore.GrainInterfaces

open System.Threading.Tasks
open Orleans

type IPropertiesManagerGrain =
    inherit IGrainWithStringKey

    abstract GetPropertyNames: unit -> Task<string list>

    abstract CreateProperty: propertyName: string -> Task<IPropertyManagerGrain>
    
    abstract GetProperty: propertyName: string -> Task<IPropertyManagerGrain option>


//type IEventStoreManagerGrain =
//    inherit IGrainWithStringKey

//    abstract getProperties: request: GetPropertiesReq -> Task<GetPropertiesRes>
//    abstract createProperty: request: CreatePropertyReq -> Task<CreatePropertyRes>
//    abstract freezeProperty: request: FreezePropertyReq -> Task<FreezePropertyRes>
//    abstract unfreezeProperty: request: UnfreezePropertyReq -> Task<UnfreezePropertyRes>
//    abstract deleteProperty: request: DeletePropertyReq -> Task<DeletePropertyRes>
//    abstract regenerateAccessKey1: request: RegenerateAccessKey1Req -> Task<RegenerateAccessKey1Res>
//    abstract regenerateAccessKey2: request: RegenerateAccessKey2Req -> Task<RegenerateAccessKey2Res>

//    abstract getNameSpaces: request: GetNameSpacesReq -> Task<GetNameSpacesRes>
//    abstract createNameSpace: request: CreateNameSpaceReq -> Task<CreateNameSpaceRes>
//    abstract freezeNameSpace: request: FreezeNameSpaceReq -> Task<FreezeNameSpaceRes>
//    abstract unfreezeNameSpace: request: UnfreezeNameSpaceReq -> Task<UnfreezeNameSpaceRes>
//    abstract deleteNameSpace: request: DeleteNameSpaceReq -> Task<DeleteNameSpaceRes>

//    abstract getTopics: request: GetTopicsReq -> Task<GetTopicsRes>
//    abstract createTopic: request: CreateTopicReq -> Task<CreateTopicRes>
//    abstract freezeTopic: request: FreezeTopicReq -> Task<FreezeTopicRes>
//    abstract unfreezeTopic: request: UnfreezeTopicReq -> Task<UnfreezeTopicRes>
//    abstract deleteTopic: request: DeleteTopicReq -> Task<DeleteTopicRes>
