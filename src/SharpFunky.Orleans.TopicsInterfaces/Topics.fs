namespace SharpFunky.Orleans.Topics

open Orleans
open SharpFunky
open SharpFunky.EventStorage
open System.Threading.Tasks
open System

type TopicPropertyInfo = {
    propertyId: Guid
    propertyName: string
}

type IEventStreamGrain =
    inherit IGrainWithStringKey

    abstract GetStatus: unit -> Task<EventStreamStatus>
    abstract Freeze: unit -> Task<unit>
    abstract Read: request: ReadEventsRequest -> Task<ReadEventsResponse>
    abstract Write: request: WriteEventsRequest -> Task<unit>

type IEventStreamFactoryGrain =
    inherit IGrainWithStringKey

    abstract Create: tableName: string * partition: string -> Task<IEventStreamGrain>


type ITopicSubscription =
    inherit IGrainWithGuidKey

type ITopicInstance =
    inherit IGrainWithGuidKey

type ITopicNamespace =
    inherit IGrainWithGuidKey

type ITopicProperty =
    inherit IGrainWithGuidKey

