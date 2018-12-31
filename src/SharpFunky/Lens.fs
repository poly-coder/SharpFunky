namespace SharpFunky

type LensGetter<'a, 'b> = 'a -> 'b
type LensSetter<'a, 'b> = 'b -> 'a -> 'a
type LensUpdater<'a, 'b> = ('b -> 'b) -> 'a -> 'a

type Lens<'a, 'b> = Lens of LensGetter<'a, 'b> * LensUpdater<'a, 'b>

[<RequireQualifiedAccess>]
module Lens =
    let cons getter updater = Lens(getter, updater)
    let cons' getter (setter: LensSetter<_, _>) =
        cons getter (fun fn a -> setter (fn (getter a)) a)

    let get (Lens(g, _)) = g
    let upd (Lens(_, u)) = u
    let set (Lens(_, u)) : LensSetter<_, _> = konst >> u
    let updWith lens = upd lens |> flip

    let identity<'a> : Lens<'a, 'a> = cons' id konst

    let compose l1 l2 =
        cons (get l1 >> get l2) (upd l2 >> upd l1)

    let mapKey key =
        cons' (Map.find key) (Map.add key)

    module Infix =
        let inline (>=>) l1 l2 = compose l1 l2
