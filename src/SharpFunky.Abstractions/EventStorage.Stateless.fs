namespace SharpFunky.EventStorage.Stateless

open SharpFunky
open SharpFunky.Services
open SharpFunky.EventStorage


type StatelessEventStreamStatus = {
    nextSequence: int64
    meta: MetaData
}

[<RequireQualifiedAccess>]
module StatelessEventStreamStatus =
    let empty: StatelessEventStreamStatus = {
        meta = Map.empty 
        nextSequence = 0L
    }
    let meta = Lens.cons' (fun s -> s.meta) (fun v s -> { s with meta = v })
    let nextSequence = Lens.cons' (fun s -> s.nextSequence) (fun v s -> { s with nextSequence = v })
    let create nextSequence' =
        empty |> Lens.set nextSequence nextSequence'

type StatelessReadEventsRequest = {
    fromSequence: int64 option
    limit: int option
    reverse: bool
}

[<RequireQualifiedAccess>]
module StatelessReadEventsRequest =
    let empty: StatelessReadEventsRequest = {
        fromSequence = None
        limit = None
        reverse = false
    }
    let fromSequence = Lens.cons' (fun s -> s.fromSequence) (fun v s -> { s with fromSequence = v })
    let limit = Lens.cons' (fun s -> s.limit) (fun v s -> { s with limit = v })
    let reverse = Lens.cons' (fun s -> s.reverse) (fun v s -> { s with reverse = v })

type StatelessReadEventsResponse = {
    events: PersistedEvent list
    nextSequence: int64
    hasMore: bool
}

[<RequireQualifiedAccess>]
module StatelessReadEventsResponse =
    let empty: StatelessReadEventsResponse = {
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

type StatelessWriteEventsRequest = {
    events: EventData list
    startSequence: int64
    meta: MetaData
}

[<RequireQualifiedAccess>]
module StatelessWriteEventsRequest =
    let empty: StatelessWriteEventsRequest = {
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
    abstract status: unit -> Async<StatelessEventStreamStatus>
    abstract read: StatelessReadEventsRequest -> Async<StatelessReadEventsResponse>
    abstract write: StatelessWriteEventsRequest -> Async<unit>

type IStatelessEventStreamFactory =
    inherit IKeyServiceFactory<string, IStatelessEventStream>

type IStatelessEventStreamFactoryAsync =
    inherit IKeyServiceFactoryAsync<string, IStatelessEventStream>

