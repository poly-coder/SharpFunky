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

type IEventStreamGrainObserver =
    inherit IGrainObserver

    abstract eventsCommitted: unit -> unit

type IEventStreamGrain =
    inherit IGrainWithStringKey

    abstract getStatus: unit -> Task<EventStreamStatus>

    abstract commitEvents: events: EventData list -> Task<unit>

    abstract subscribe: observer: IEventStreamGrainObserver -> Task<unit>
    abstract unsubscribe: observer: IEventStreamGrainObserver -> Task<unit>

type IEventStreamReaderGrainObserver =
    inherit IGrainObserver

    abstract eventsAvailable: PersistedEventData list -> unit

type IEventStreamReaderGrain =
    inherit IGrainWithGuidCompoundKey

    abstract subscribe: observer: IEventStreamReaderGrainObserver -> Task<unit>
    abstract unsubscribe: observer: IEventStreamReaderGrainObserver -> Task<unit>
