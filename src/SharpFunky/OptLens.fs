﻿namespace SharpFunky

type OptLensGetter<'a, 'b> = 'a -> 'b option
type OptLensSetter<'a, 'b> = 'b option -> 'a -> 'a
type OptLensUpdater<'a, 'b> = ('b option -> 'b option) -> 'a -> 'a

type OptLens<'a, 'b> = Lens<'a, 'b option>

[<RequireQualifiedAccess>]
module OptLens =
    let cons getter updater : OptLens<'a, 'b> = Lens.cons getter updater
    let cons' getter setter : OptLens<'a, 'b> = Lens.cons' getter setter

    let getOpt (l: OptLens<'a, 'b>) : OptLensGetter<'a, 'b> = Lens.get l
    let upd (l: OptLens<'a, 'b>): OptLensUpdater<'a, 'b> = Lens.upd l
    let set (l: OptLens<'a, 'b>): OptLensSetter<'a, 'b> = Lens.set l

    let get lens = getOpt lens >> Option.get
    let setSome lens = Some >> set lens
    let reset lens = (fun () -> None) >> set lens

    let updWith lens = upd lens |> flip
    let updBind lens = Option.bind >> upd lens
    let updMap lens = Option.map >> upd lens

    let composeGetters (g1: OptLensGetter<_, _>) (g2: OptLensGetter<_, _>): OptLensGetter<_, _> =
        g1 >> Option.bind g2
    let composeUpdater (u1: OptLensUpdater<_, _>) (u2: OptLensUpdater<_, _>): OptLensUpdater<_, _> =
        u2 >> Option.map >> u1

    let compose (l1: OptLens<'a, 'b>) (l2: OptLens<'b, 'c>): OptLens<'a, 'c> =
        cons
            (composeGetters (getOpt l1) (getOpt l2))
            (composeUpdater (upd l1) (upd l2))

    let ofLens (lens: Lens<'a, 'b>): OptLens<'a, 'b> =
        let getter = Lens.get lens >> Some
        let updater fn = Lens.upd lens (fun b -> match fn (Some b) with Some b' -> b' | None -> b)
        cons getter updater

    let mapKey key =
        let getter m = Map.tryFind key m
        let setter = function Some v -> Map.add key v | None -> Map.remove key
        cons' getter setter

    let parser defSource tryParse format: OptLens<'a, 'b> =
        let tryFormat (value: 'b option) : 'a =
            match value with
            | Some v -> format v
            | None -> defSource
        cons' tryParse (fun value _ -> tryFormat value)

    module Infix =
        let inline (>=>) l1 l2 = compose l1 l2