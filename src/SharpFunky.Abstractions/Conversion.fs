namespace SharpFunky.Conversion

open SharpFunky

type ISyncConverter<'a, 'b> =
    abstract convert: 'a -> 'b

type IAsyncConverter<'a, 'b> =
    abstract convert: 'a -> Async<'b>

type ISyncReversibleConverter<'a, 'b> =
    inherit ISyncConverter<'a, 'b>
    abstract convertBack: 'b -> 'a

type IAsyncReversibleConverter<'a, 'b> =
    inherit IAsyncConverter<'a, 'b>
    abstract convertBack: 'b -> Async<'a>

module SyncConverter =
    let createInstance convert =
        { new ISyncConverter<'a, 'b> with
            member __.convert a = convert a }

    let toFun (converter: ISyncConverter<'a, 'b>) : Fn<_, _> =
        fun a -> converter.convert a

    let compose cab cbc =
        toFun cab >> toFun cbc |> createInstance

module AsyncConverter =
    let createInstance convert =
        { new IAsyncConverter<'a, 'b> with
            member __.convert a = convert a }

    let toFun (converter: IAsyncConverter<'a, 'b>) : AsyncFn<_, _> =
        fun a -> converter.convert a

    let compose cab cbc =
        toFun cab |> AsyncFn.bind (toFun cbc) |> createInstance

module AsyncReversibleConverter =
    let createInstance convert convertBack =
        { new IAsyncReversibleConverter<'a, 'b> with
            member __.convert a = convert a
            member __.convertBack b = convertBack b }

    let toFun (converter: IAsyncReversibleConverter<'a, 'b>) : AsyncFn<_, _> =
        fun a -> converter.convert a

    let toFunBack (converter: IAsyncReversibleConverter<'a, 'b>) : AsyncFn<_, _> =
        fun a -> converter.convertBack a

    let rev converter =
        let convert = toFun converter
        let convertBack = toFunBack converter
        createInstance convertBack convert

    let compose cab cbc =
        let convert = toFun cab |> AsyncFn.bind (toFun cbc)
        let convertBack = toFunBack cbc |> AsyncFn.bind (toFunBack cab)
        createInstance convert convertBack

module SyncReversibleConverter =
    let createInstance convert convertBack =
        { new ISyncReversibleConverter<'a, 'b> with
            member __.convert a = convert a
            member __.convertBack b = convertBack b }

    let toFun (converter: ISyncReversibleConverter<'a, 'b>) : Fn<_, _> =
        fun a -> converter.convert a

    let toFunBack (converter: ISyncReversibleConverter<'a, 'b>) : Fn<_, _> =
        fun a -> converter.convertBack a

    let rev converter =
        let convert = toFun converter
        let convertBack = toFunBack converter
        createInstance convertBack convert

    let compose cab cbc =
        let convert = toFun cab >> toFun cbc
        let convertBack = toFunBack cbc >> toFunBack cab
        createInstance convert convertBack

module Converters =
    let toBase64 =
        SyncReversibleConverter.createInstance String.toBase64 String.fromBase64
    let fromBase64 = SyncReversibleConverter.rev toBase64

    let fromUtf8 =
        SyncReversibleConverter.createInstance String.fromUtf8 String.toUtf8
    let toUtf8 = SyncReversibleConverter.rev fromUtf8

    let fromUtf7 =
        SyncReversibleConverter.createInstance String.fromUtf7 String.toUtf7
    let toUtf7 = SyncReversibleConverter.rev fromUtf7

    let fromUtf32 =
        SyncReversibleConverter.createInstance String.fromUtf32 String.toUtf32
    let toUtf32 = SyncReversibleConverter.rev fromUtf32

    let fromUnicode =
        SyncReversibleConverter.createInstance String.fromUnicode String.toUnicode
    let toUnicode = SyncReversibleConverter.rev fromUnicode

    let fromAscii =
        SyncReversibleConverter.createInstance String.fromAscii String.toAscii
    let toAscii = SyncReversibleConverter.rev fromAscii

    let toByte =
        SyncReversibleConverter.createInstance Byte.parse Byte.toString
    let fromByte = SyncReversibleConverter.rev toByte

    let toInt32 =
        SyncReversibleConverter.createInstance Int32.parse Int32.toString
    let fromInt32 = SyncReversibleConverter.rev toInt32
