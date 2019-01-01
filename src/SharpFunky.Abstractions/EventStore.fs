namespace SharpFunky.Storage

open SharpFunky

type EventContent =
    | EmptyEvent
    | BinaryEvent of byte[]
    | StringEvent of string

type MetaValue =
    | MetaNull
    | MetaString of string
    | MetaLong of int64
    | MetaStrings of string list

type MetaData = Map<string, MetaValue>

type EventData = {
    meta: MetaData
    data: EventContent
}

type PersistedEvent = {
    aggregateId: string
    sequence: int64
    timestamp: int64
    event: EventData
}

[<RequireQualifiedAccess>]
module EventContent =
    let optBinary =
        OptLens.cons'
            (function BinaryEvent v -> Some v | _ -> None)
            (fun value _ -> match value with Some v -> BinaryEvent v | _ -> EmptyEvent)
    let optString =
        OptLens.cons'
            (function StringEvent v -> Some v | _ -> None)
            (fun value _ -> match value with Some v -> StringEvent v | _ -> EmptyEvent)

[<RequireQualifiedAccess>]
module MetaValue =
    let optString =
        OptLens.cons'
            (function MetaString v -> Some v | _ -> None)
            (fun value _ -> match value with Some v -> MetaString v | _ -> MetaNull)
    let optLong =
        OptLens.cons'
            (function MetaLong v -> Some v | _ -> None)
            (fun value _ -> match value with Some v -> MetaLong v | _ -> MetaNull)
    let optStrings =
        OptLens.cons'
            (function MetaStrings f -> Some f | _ -> None)
            (fun value _ -> match value with Some f -> MetaStrings f | _ -> MetaNull)
    
    let mapStringDef def =
        Lens.cons
            (function MetaString v -> v | _ -> def)
            (fun valueFn md ->
                match md with
                | MetaString v -> valueFn v |> MetaString
                | _ -> valueFn def |> MetaString
            )
    let mapString = mapStringDef ""
    
    let mapLongDef def =
        Lens.cons
            (function MetaLong v -> v | _ -> def)
            (fun valueFn md ->
                match md with
                | MetaLong v -> valueFn v |> MetaLong
                | _ -> valueFn def |> MetaLong
            )
    let mapLong = mapLongDef 0L
    
    let mapStringsDef def =
        Lens.cons
            (function MetaStrings v -> v | _ -> def)
            (fun valueFn md ->
                match md with
                | MetaStrings v -> valueFn v |> MetaStrings
                | _ -> valueFn def |> MetaStrings
            )
    let mapStrings = mapStringsDef []

[<RequireQualifiedAccess>]
module MetaData =
    let optString key = OptLens.compose (OptLens.mapKey key) (MetaValue.optString)
    let optLong key = OptLens.compose (OptLens.mapKey key) (MetaValue.optLong)
    let optStrings key = OptLens.compose (OptLens.mapKey key) (MetaValue.optStrings)

[<RequireQualifiedAccess>]
module EventData =
    let empty = {
        meta = Map.empty
        data = EmptyEvent
    }
    let data = Lens.cons' (fun (e: EventData) -> e.data) (fun v e -> { e with data = v })
    let dataBinary = OptLens.compose (OptLens.ofLens data) EventContent.optBinary
    let dataString = OptLens.compose (OptLens.ofLens data) EventContent.optString
    let meta = Lens.cons' (fun (e: EventData) -> e.meta) (fun v e -> { e with meta = v })

[<RequireQualifiedAccess>]
module PersistedEvent =
    let empty = {
        aggregateId = ""
        sequence = 0L
        timestamp = 0L
        event = EventData.empty
    }
    let aggregateId = Lens.cons' (fun (e: PersistedEvent) -> e.aggregateId) (fun v e -> { e with aggregateId = v })
    let sequence = Lens.cons' (fun (e: PersistedEvent) -> e.sequence) (fun v e -> { e with sequence = v })
    let timestamp = Lens.cons' (fun (e: PersistedEvent) -> e.timestamp) (fun v e -> { e with timestamp = v })
    let event = Lens.cons' (fun (e: PersistedEvent) -> e.event) (fun v e -> { e with event = v })
    let data = Lens.compose event EventData.data
    let dataBinary = OptLens.compose (OptLens.ofLens data) EventContent.optBinary
    let dataString = OptLens.compose (OptLens.ofLens data) EventContent.optString
    let meta = Lens.compose event EventData.meta
    let metaString key = OptLens.compose (OptLens.ofLens meta) (MetaData.optString key)
    let metaLong key = OptLens.compose (OptLens.ofLens meta) (MetaData.optLong key)
    let metaStrings key = OptLens.compose (OptLens.ofLens meta) (MetaData.optStrings key)

type EventStoreStatus = {
    isFrozen: bool
    nextSequence: int64
}

[<RequireQualifiedAccess>]
module EventStoreStatus =
    let empty = {
        isFrozen = false
        nextSequence = 0L
    }
    let isFrozen = Lens.cons' (fun s -> s.isFrozen) (fun v s -> { s with isFrozen = v })
    let nextSequence = Lens.cons' (fun s -> s.nextSequence) (fun v s -> { s with nextSequence = v })

type GetEventsRequest = {
    fromSequence: int64 option
    limit: int option
    reverse: bool
}

[<RequireQualifiedAccess>]
module GetEventsRequest =
    let empty = {
        fromSequence = None
        limit = None
        reverse = false
    }
    let fromSequence = Lens.cons' (fun s -> s.fromSequence) (fun v s -> { s with fromSequence = v })
    let limit = Lens.cons' (fun s -> s.limit) (fun v s -> { s with limit = v })
    let reverse = Lens.cons' (fun s -> s.reverse) (fun v s -> { s with reverse = v })

type GetEventsResponse = {
    events: PersistedEvent list
    nextSequence: int64
}

[<RequireQualifiedAccess>]
module GetEventsResponse =
    let empty = {
        events = []
        nextSequence = 0L
    }
    let events = Lens.cons' (fun s -> s.events) (fun v s -> { s with events = v })
    let nextSequence = Lens.cons' (fun s -> s.nextSequence) (fun v s -> { s with nextSequence = v })

type IEventStoreReader =
    abstract status: unit -> AsyncResult<EventStoreStatus, exn>
    abstract read: GetEventsRequest -> AsyncResult<GetEventsResponse, exn>

type IEventStore =
    inherit IEventStoreReader
