namespace SharpFunky.EventStorage.Stateless

open SharpFunky
open SharpFunky.Services
open SharpFunky.EventStorage

type EventStreamStatus = {
    nextSequence: int64
    meta: MetaData
}

[<RequireQualifiedAccess>]
module EventStreamStatus =
    let empty: EventStreamStatus = {
        meta = Map.empty 
        nextSequence = 0L
    }
    let meta = Lens.cons' (fun s -> s.meta) (fun v s -> { s with meta = v })
    let nextSequence = Lens.cons' (fun s -> s.nextSequence) (fun v s -> { s with nextSequence = v })
    let create nextSequence' =
        empty |> Lens.set nextSequence nextSequence'

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
}

[<RequireQualifiedAccess>]
module ReadEventsResponse =
    let empty: ReadEventsResponse = {
        events = []
        nextSequence = 0L
    }
    let events = Lens.cons' (fun s -> s.events) (fun v s -> { s with events = v })
    let nextSequence = Lens.cons' (fun s -> s.nextSequence) (fun v s -> { s with nextSequence = v })
    let create events' nextSequence' =
        empty
        |> Lens.set events events'
        |> Lens.set nextSequence nextSequence'

type WriteEventsRequest = {
    events: EventData list
    startSequence: int64
    meta: MetaData
}

[<RequireQualifiedAccess>]
module WriteEventsRequest =
    let empty: WriteEventsRequest = {
        events = []
        startSequence = 0L
        meta = Map.empty
    }
    let events = Lens.cons' (fun s -> s.events) (fun v s -> { s with events = v })
    let startSequence = Lens.cons' (fun s -> s.startSequence) (fun v s -> { s with startSequence = v })
    let meta = Lens.cons' (fun s -> s.meta) (fun v s -> { s with meta = v })
    let create events' startSequence' =
        empty
        |> Lens.set events events'
        |> Lens.set startSequence startSequence'

type IStatelessEventStream =
    abstract status: unit -> Async<EventStreamStatus>
    abstract read: ReadEventsRequest -> Async<ReadEventsResponse>
    abstract write: WriteEventsRequest -> Async<unit>

type IStatelessEventStreamFactory =
    inherit IKeyServiceFactory<string, IStatelessEventStream>

type IStatelessEventStreamFactoryAsync =
    inherit IKeyServiceFactoryAsync<string, IStatelessEventStream>

