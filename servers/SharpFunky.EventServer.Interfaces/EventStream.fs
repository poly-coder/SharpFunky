namespace SharpFunky.EventServer.Interfaces

open System.Threading.Tasks
open Orleans

type EventData = {
    meta: Map<string, string>
    data: byte[]
}

type PersistedEventData = {
    event: EventData
    sequence: int64
}

type EventStreamStatus = {
    nextSequence: int64
}

type ReadFromSequence =
    | ReadFromStart

type IEventStreamReaderGrainObserver =
    inherit IGrainObserver

    abstract eventsAvailable: events: PersistedEventData list -> unit

type IEventStreamReaderGrain =
    inherit IGrainWithGuidKey

    abstract addReadQuota: quota: int -> Task<unit>

    abstract subscribe: observer: IEventStreamReaderGrainObserver -> Task<unit>
    abstract unsubscribe: observer: IEventStreamReaderGrainObserver -> Task<unit>

type IEventStreamGrainObserver =
    inherit IGrainObserver

    abstract eventsCommitted: unit -> unit

type IEventStreamGrain =
    inherit IGrainWithStringKey

    abstract getStatus: unit -> Task<EventStreamStatus>

    abstract commitEvents: events: EventData list -> Task<unit>
    abstract createReader: readFrom: ReadFromSequence -> Task<IEventStreamReaderGrain>

    abstract subscribe: observer: IEventStreamGrainObserver -> Task<unit>
    abstract unsubscribe: observer: IEventStreamGrainObserver -> Task<unit>

type IEventStreamNamespaceGrain =
    inherit IGrainWithStringKey

    abstract getStream: streamId: string -> Task<IEventStreamGrain>

type IEventStreamServiceGrain =
    inherit IGrainWithStringKey

    abstract getNamespace: nameSpace: string -> Task<IEventStreamNamespaceGrain>
