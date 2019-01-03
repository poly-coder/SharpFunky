namespace SharpFunky.EventStorage

open SharpFunky
open SharpFunky.Services

type EventContent =
    | EmptyEvent
    | BinaryEvent of byte[]
    | StringEvent of string

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

type MetaValue =
    | MetaNull
    | MetaString of string
    | MetaLong of int64
    | MetaBool of bool
    | MetaStrings of string list

[<RequireQualifiedAccess>]
module MetaValue =
    let optString =
        OptLens.cons'
            (function MetaString v -> Some v | _ -> None)
            (fun value _ -> match value with Some v -> MetaString v | _ -> MetaNull)
    let optBool =
        OptLens.cons'
            (function MetaBool v -> Some v | _ -> None)
            (fun value _ -> match value with Some v -> MetaBool v | _ -> MetaNull)
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
    
    let mapBoolDef def =
        Lens.cons
            (function MetaBool v -> v | _ -> def)
            (fun valueFn md ->
                match md with
                | MetaBool v -> valueFn v |> MetaBool
                | _ -> valueFn def |> MetaBool
            )
    let mapBool = mapBoolDef false
    
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

type MetaData = Map<string, MetaValue>

[<RequireQualifiedAccess>]
module MetaData =
    let optString key = OptLens.compose (OptLens.mapKey key) (MetaValue.optString)
    let optBool key = OptLens.compose (OptLens.mapKey key) (MetaValue.optBool)
    let optLong key = OptLens.compose (OptLens.mapKey key) (MetaValue.optLong)
    let optStrings key = OptLens.compose (OptLens.mapKey key) (MetaValue.optStrings)

type EventData = {
    meta: MetaData
    data: EventContent
}

[<RequireQualifiedAccess>]
module EventData =
    let empty: EventData = {
        meta = Map.empty
        data = EmptyEvent
    }
    let data = Lens.cons' (fun (e: EventData) -> e.data) (fun v e -> { e with data = v })
    let dataBinary = OptLens.compose (OptLens.ofLens data) EventContent.optBinary
    let dataString = OptLens.compose (OptLens.ofLens data) EventContent.optString
    let meta = Lens.cons' (fun (e: EventData) -> e.meta) (fun v e -> { e with meta = v })

type PersistedEvent = {
    streamId: string
    sequence: int64
    event: EventData
}

[<RequireQualifiedAccess>]
module PersistedEvent =
    let empty: PersistedEvent = {
        streamId = ""
        sequence = 0L
        event = EventData.empty
    }
    let streamId = Lens.cons' (fun (e: PersistedEvent) -> e.streamId) (fun v e -> { e with streamId = v })
    let sequence = Lens.cons' (fun (e: PersistedEvent) -> e.sequence) (fun v e -> { e with sequence = v })
    let event = Lens.cons' (fun (e: PersistedEvent) -> e.event) (fun v e -> { e with event = v })
    let data = Lens.compose event EventData.data
    let dataBinary = OptLens.compose (OptLens.ofLens data) EventContent.optBinary
    let dataString = OptLens.compose (OptLens.ofLens data) EventContent.optString
    let meta = Lens.compose event EventData.meta
    let metaString key = OptLens.compose (OptLens.ofLens meta) (MetaData.optString key)
    let metaLong key = OptLens.compose (OptLens.ofLens meta) (MetaData.optLong key)
    let metaStrings key = OptLens.compose (OptLens.ofLens meta) (MetaData.optStrings key)
    let create streamId' sequence' eventData' =
        empty
        |> Lens.set streamId streamId'
        |> Lens.set sequence sequence'
        |> Lens.set event eventData'
