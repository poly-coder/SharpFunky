namespace SharpFunky.Topic.Core
open System
open SharpFunky

type MetaValue =
| MetaNull
| MetaString of string
| MetaLong of int64
| MetaFloat of float
| MetaBool of bool
| MetaDate of DateTime
| MetaStrings of string list
| MetaList of MetaValue list
| MetaDict of Map<string, string>
| MetaMap of Map<string, MetaValue>

type MessageMeta = Map<string, MetaValue>

type Message = {
    meta: MessageMeta
    data: byte[] option
}

type MessageId = string
type TopicSequence = int64
type Timestamp = DateTime

module Message =

    let empty = { meta = Map.empty; data = None }
    let create meta data = { meta = meta; data = data }

    module Data =
        let get m = m.data
        let upd fn m = m |> get |> fn |> fun v -> { m with data = v }
        let set value = upd (konst value)
        let put value = upd (konst <| Some value)
        let clear = upd (konst None)

    module Meta =
        let get m = m.meta
        let upd fn m = m |> get |> fn |> fun v -> { m with meta = v }
        let set value = upd (konst value)
        let clearMeta = upd (konst Map.empty)
    
    module MetaKey =
        let tryGet key = Meta.get >> Map.tryFind key
        let getOr def key = tryGet key >> Option.defaultValue def
        let upd key fn message = 
            message
            |> Meta.upd (fun meta ->
                meta
                |> Map.tryFind key
                |> fn
                |> Option.matches
                    (fun value -> meta |> Map.add key value)
                    (fun () -> meta |> Map.remove key))
        let matches key fn defFn = upd key (Option.matches fn defFn)
        let ensure key fn defFn = matches key (fn >> Some) (defFn >> Some)
        let sure def key fn = ensure key fn (fun () -> fn def)
        let put key value = sure MetaNull key (konst value)
        let pop key = upd key (konst None)
        let bind key fn = upd key (Option.matches fn (konst None))
        let map key fn = bind key (fn >> Some)

    module MetaString =
        let internal defVal = ""
        let internal toValue = function Some (MetaString v) -> Some v | _ -> None
        let internal ofValue = function Some v -> Some (MetaString v) | _ -> None

        let tryGet key = MetaKey.tryGet key >> toValue
        let getOr def key = tryGet key >> Option.defaultValue def
        let get key = getOr defVal key
        let upd key fn = MetaKey.upd key (toValue >> fn >> ofValue)
        let matches key fn defFn = upd key (Option.matches fn defFn)
        let ensure key fn defFn = matches key (fn >> Some) (defFn >> Some)
        let ensureDef key fn = ensure key fn (fun () -> fn defVal)
        let sure def key fn = ensure key fn (fun () -> fn def)
        let sureDef key fn = sure defVal key fn
        let put key value = sure defVal key (konst value)
        let pop key = upd key (konst None)
        let bind key fn = upd key (Option.matches fn (konst None))
        let map key fn = bind key (fn >> Some)

    module MetaLong =
        let internal defVal = int64 0
        let internal toValue = function Some (MetaLong v) -> Some v | _ -> None
        let internal ofValue = function Some v -> Some (MetaLong v) | _ -> None

        let tryGet key = MetaKey.tryGet key >> toValue
        let getOr def key = tryGet key >> Option.defaultValue def
        let get key = getOr defVal key
        let upd key fn = MetaKey.upd key (toValue >> fn >> ofValue)
        let matches key fn defFn = upd key (Option.matches fn defFn)
        let ensure key fn defFn = matches key (fn >> Some) (defFn >> Some)
        let ensureDef key fn = ensure key fn (fun () -> fn defVal)
        let sure def key fn = ensure key fn (fun () -> fn def)
        let sureDef key fn = sure defVal key fn
        let put key value = sure defVal key (konst value)
        let pop key = upd key (konst None)
        let bind key fn = upd key (Option.matches fn (konst None))
        let map key fn = bind key (fn >> Some)

    module MetaFloat =
        let internal defVal = float 0
        let internal toValue = function Some (MetaFloat v) -> Some v | _ -> None
        let internal ofValue = function Some v -> Some (MetaFloat v) | _ -> None

        let tryGet key = MetaKey.tryGet key >> toValue
        let getOr def key = tryGet key >> Option.defaultValue def
        let get key = getOr defVal key
        let upd key fn = MetaKey.upd key (toValue >> fn >> ofValue)
        let matches key fn defFn = upd key (Option.matches fn defFn)
        let ensure key fn defFn = matches key (fn >> Some) (defFn >> Some)
        let ensureDef key fn = ensure key fn (fun () -> fn defVal)
        let sure def key fn = ensure key fn (fun () -> fn def)
        let sureDef key fn = sure defVal key fn
        let put key value = sure defVal key (konst value)
        let pop key = upd key (konst None)
        let bind key fn = upd key (Option.matches fn (konst None))
        let map key fn = bind key (fn >> Some)

    module MetaBool =
        let internal defVal = false
        let internal toValue = function Some (MetaBool v) -> Some v | _ -> None
        let internal ofValue = function Some v -> Some (MetaBool v) | _ -> None

        let tryGet key = MetaKey.tryGet key >> toValue
        let getOr def key = tryGet key >> Option.defaultValue def
        let get key = getOr defVal key
        let upd key fn = MetaKey.upd key (toValue >> fn >> ofValue)
        let matches key fn defFn = upd key (Option.matches fn defFn)
        let ensure key fn defFn = matches key (fn >> Some) (defFn >> Some)
        let ensureDef key fn = ensure key fn (fun () -> fn defVal)
        let sure def key fn = ensure key fn (fun () -> fn def)
        let sureDef key fn = sure defVal key fn
        let put key value = sure defVal key (konst value)
        let pop key = upd key (konst None)
        let bind key fn = upd key (Option.matches fn (konst None))
        let map key fn = bind key (fn >> Some)

    module MetaDate =
        let internal defVal = DateTime.MinValue
        let internal toValue = function Some (MetaDate v) -> Some v | _ -> None
        let internal ofValue = function Some v -> Some (MetaDate v) | _ -> None

        let tryGet key = MetaKey.tryGet key >> toValue
        let getOr def key = tryGet key >> Option.defaultValue def
        let get key = getOr defVal key
        let upd key fn = MetaKey.upd key (toValue >> fn >> ofValue)
        let matches key fn defFn = upd key (Option.matches fn defFn)
        let ensure key fn defFn = matches key (fn >> Some) (defFn >> Some)
        let ensureDef key fn = ensure key fn (fun () -> fn defVal)
        let sure def key fn = ensure key fn (fun () -> fn def)
        let sureDef key fn = sure defVal key fn
        let put key value = sure defVal key (konst value)
        let pop key = upd key (konst None)
        let bind key fn = upd key (Option.matches fn (konst None))
        let map key fn = bind key (fn >> Some)

    module MetaStrings =
        let internal defVal = []
        let internal toValue = function Some (MetaStrings v) -> Some v | _ -> None
        let internal ofValue = function Some v -> Some (MetaStrings v) | _ -> None

        let tryGet key = MetaKey.tryGet key >> toValue
        let getOr def key = tryGet key >> Option.defaultValue def
        let get key = getOr defVal key
        let upd key fn = MetaKey.upd key (toValue >> fn >> ofValue)
        let matches key fn defFn = upd key (Option.matches fn defFn)
        let ensure key fn defFn = matches key (fn >> Some) (defFn >> Some)
        let ensureDef key fn = ensure key fn (fun () -> fn defVal)
        let sure def key fn = ensure key fn (fun () -> fn def)
        let sureDef key fn = sure defVal key fn
        let put key value = sure defVal key (konst value)
        let pop key = upd key (konst None)
        let bind key fn = upd key (Option.matches fn (konst None))
        let map key fn = bind key (fn >> Some)
        let append key value = sureDef key (fun l -> l @ [value])
        let prepend key value = sureDef key (fun l -> value :: l)
        let concat key values = sureDef key (fun l -> l @ values)
        let filterItems key fn = sureDef key (List.filter fn)
        let mapItems key fn = sureDef key (List.map fn)

    module MetaList =
        let internal defVal = []
        let internal toValue = function Some (MetaList v) -> Some v | _ -> None
        let internal ofValue = function Some v -> Some (MetaList v) | _ -> None

        let tryGet key = MetaKey.tryGet key >> toValue
        let getOr def key = tryGet key >> Option.defaultValue def
        let get key = getOr defVal key
        let upd key fn = MetaKey.upd key (toValue >> fn >> ofValue)
        let matches key fn defFn = upd key (Option.matches fn defFn)
        let ensure key fn defFn = matches key (fn >> Some) (defFn >> Some)
        let ensureDef key fn = ensure key fn (fun () -> fn defVal)
        let sure def key fn = ensure key fn (fun () -> fn def)
        let sureDef key fn = sure defVal key fn
        let put key value = sure defVal key (konst value)
        let pop key = upd key (konst None)
        let bind key fn = upd key (Option.matches fn (konst None))
        let map key fn = bind key (fn >> Some)
        let append key value = sureDef key (fun l -> l @ [value])
        let prepend key value = sureDef key (fun l -> value :: l)
        let concat key values = sureDef key (fun l -> l @ values)
        let filterItems key fn = sureDef key (List.filter fn)
        let mapItems key fn = sureDef key (List.map fn)

    module MetaDict =
        let internal defVal = Map.empty
        let internal toValue = function Some (MetaDict v) -> Some v | _ -> None
        let internal ofValue = function Some v -> Some (MetaDict v) | _ -> None

        let tryGet key = MetaKey.tryGet key >> toValue
        let getOr def key = tryGet key >> Option.defaultValue def
        let get key = getOr defVal key
        let upd key fn = MetaKey.upd key (toValue >> fn >> ofValue)
        let matches key fn defFn = upd key (Option.matches fn defFn)
        let ensure key fn defFn = matches key (fn >> Some) (defFn >> Some)
        let ensureDef key fn = ensure key fn (fun () -> fn defVal)
        let sure def key fn = ensure key fn (fun () -> fn def)
        let sureDef key fn = sure defVal key fn
        let put key value = sure defVal key (konst value)
        let pop key = upd key (konst None)
        let bind key fn = upd key (Option.matches fn (konst None))
        let map key fn = bind key (fn >> Some)

    module MetaMap =
        let internal defVal = Map.empty
        let internal toValue = function Some (MetaMap v) -> Some v | _ -> None
        let internal ofValue = function Some v -> Some (MetaMap v) | _ -> None

        let tryGet key = MetaKey.tryGet key >> toValue
        let getOr def key = tryGet key >> Option.defaultValue def
        let get key = getOr defVal key
        let upd key fn = MetaKey.upd key (toValue >> fn >> ofValue)
        let matches key fn defFn = upd key (Option.matches fn defFn)
        let ensure key fn defFn = matches key (fn >> Some) (defFn >> Some)
        let ensureDef key fn = ensure key fn (fun () -> fn defVal)
        let sure def key fn = ensure key fn (fun () -> fn def)
        let sureDef key fn = sure defVal key fn
        let put key value = sure defVal key (konst value)
        let pop key = upd key (konst None)
        let bind key fn = upd key (Option.matches fn (konst None))
        let map key fn = bind key (fn >> Some)

    module MessageId =
        let internal gen(): MessageId = Guid.NewGuid().ToString("N").ToLowerInvariant()
        let key = "_MID"
        let tryGet msg: MessageId option = MetaString.tryGet key msg
        let set (id: MessageId) msg = MetaString.put key id msg
        let generateIfMissingWith (gen: unit -> MessageId) = MetaString.ensure key id gen
        let generateIfMissing = generateIfMissingWith gen
        let generateWith fn = fn() |> set
        let generate msg = msg |> set (gen())

    module TopicSequence =
        let key = "_SEQ"
        let tryGet msg: TopicSequence option = MetaLong.tryGet key msg
        let set (id: TopicSequence) msg = MetaLong.put key id msg

    module Timestamp =
        let internal gen(): Timestamp = DateTime.UtcNow
        let key = "_TMS"
        let tryGet msg: Timestamp option = MetaDate.tryGet key msg
        let set (id: Timestamp) = MetaDate.put key id
        let generateIfMissingWith (gen: unit -> Timestamp) = MetaDate.ensure key id gen
        let generateIfMissing = generateIfMissingWith gen
        let generateWith fn = fn() |> set
        let generate msg = msg |> set (gen())
