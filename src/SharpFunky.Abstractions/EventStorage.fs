namespace SharpFunky.EventStorage

open SharpFunky
open SharpFunky.Services

type EventStreamStatus = {
    isFrozen: bool
    nextSequence: int64
    meta: MetaData
}

[<RequireQualifiedAccess>]
module EventStreamStatus =
    let empty: EventStreamStatus = {
        isFrozen = false
        meta = Map.empty
        nextSequence = 0L
    }
    let meta = Lens.cons' (fun s -> s.meta) (fun v s -> { s with meta = v })
    let isFrozen = Lens.cons' (fun s -> s.isFrozen) (fun v s -> { s with isFrozen = v })
    let nextSequence = Lens.cons' (fun s -> s.nextSequence) (fun v s -> { s with nextSequence = v })
    let create nextSequence' =
        empty
        |> Lens.set nextSequence nextSequence'

type ReadEventsRequest = {
    fromSequence: int64 option
    limit: int option
    reverse: bool
}

[<RequireQualifiedAccess>]
module ReadEventsRequest =
    let empty: ReadEventsRequest = {
        fromSequence = None
        limit = None
        reverse = false
    }
    let fromSequence = Lens.cons' (fun s -> s.fromSequence) (fun v s -> { s with fromSequence = v })
    let limit = Lens.cons' (fun s -> s.limit) (fun v s -> { s with limit = v })
    let reverse = Lens.cons' (fun s -> s.reverse) (fun v s -> { s with reverse = v })

type ReadEventsResponse = {
    events: PersistedEvent list
    nextSequence: int64
    hasMore: bool
}

[<RequireQualifiedAccess>]
module ReadEventsResponse =
    let empty: ReadEventsResponse = {
        events = []
        nextSequence = 0L
        hasMore = false
    }
    let events = Lens.cons' (fun s -> s.events) (fun v s -> { s with events = v })
    let nextSequence = Lens.cons' (fun s -> s.nextSequence) (fun v s -> { s with nextSequence = v })
    let hasMore = Lens.cons' (fun s -> s.hasMore) (fun v s -> { s with hasMore = v })
    let create events' nextSequence' hasMore' =
        empty
        |> Lens.set events events'
        |> Lens.set nextSequence nextSequence'
        |> Lens.set hasMore hasMore'

type WriteEventsRequest = {
    events: EventData list
    meta: MetaData
}

[<RequireQualifiedAccess>]
module WriteEventsRequest =
    let empty: WriteEventsRequest = {
        events = []
        meta = Map.empty
    }
    let events = Lens.cons' (fun s -> s.events) (fun v s -> { s with events = v })
    let meta = Lens.cons' (fun s -> s.meta) (fun v s -> { s with meta = v })
    let create events' =
        empty
        |> Lens.set events events'

type IEventStream =
    abstract status: unit -> Async<EventStreamStatus>
    abstract freeze: unit -> Async<unit>
    abstract read: ReadEventsRequest -> Async<ReadEventsResponse>
    abstract write: WriteEventsRequest -> Async<unit>

type IEventStreamFactory =
    inherit IKeyServiceFactory<string, IEventStream>

type IEventStreamFactoryAsync =
    inherit IKeyServiceFactoryAsync<string, IEventStream>
