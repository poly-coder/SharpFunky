namespace SharpFunky.Topic.Core.Serialization

open System
open MsgPack
open SharpFunky
open SharpFunky.Topic.Core

type IMessageSerializer =
    abstract serialize: Packer -> Message -> Packer
    abstract deserialize: Unpacker -> Message
    abstract deserializeMeta: Unpacker -> MessageMeta

[<RequireQualifiedAccess>]
module MsgPackV1 =
    module Impl =
        [<Literal>]
        let MetaNullCode = 0uy
        [<Literal>]
        let MetaStringCode = 1uy
        [<Literal>]
        let MetaLongCode = 2uy
        [<Literal>]
        let MetaFloatCode = 3uy
        [<Literal>]
        let MetaBoolCode = 4uy
        [<Literal>]
        let MetaDateCode = 5uy
        [<Literal>]
        let MetaStringsCode = 6uy
        [<Literal>]
        let MetaListCode = 7uy
        [<Literal>]
        let MetaDictCode = 8uy
        [<Literal>]
        let MetaMapCode = 9uy

        let packThis fn (pk: Packer) = fn pk
        let unpackOrExn fn (upk: Unpacker) = fn upk |> Option.ofTryOp |> Option.get

        let packNull = packThis <| fun pk -> pk.PackNull()
        let packString s = packThis <| fun pk -> pk.PackString(s)
        let unpackString = unpackOrExn <| fun u -> u.ReadString()
        let packBool v = packThis <| fun pk -> pk.Pack(v: bool)
        let unpackBool = unpackOrExn <| fun u -> u.ReadBoolean()
        let packByte v = packThis <| fun pk -> pk.Pack(v: byte)
        let unpackByte = unpackOrExn <| fun u -> u.ReadByte()
        let packInt v = packThis <| fun pk -> pk.Pack(v: int)
        let unpackInt = unpackOrExn <| fun u -> u.ReadInt32()
        let packLong v = packThis <| fun pk -> pk.Pack(v: int64)
        let unpackLong = unpackOrExn <| fun u -> u.ReadInt64()
        let packFloat v = packThis <| fun pk -> pk.Pack(v: float)
        let unpackFloat = unpackOrExn <| fun u -> u.ReadDouble()
        
        let packDateTime (v: DateTime) = packByte (byte v.Kind) >> packLong (v.Ticks)
        let unpackDateTime upk =
            let kind = unpackByte upk |> int
            let ticks = unpackLong upk
            DateTime(ticks, enum kind)

        let rec packLenSeqWith len arr packItem =
            packInt len >> fun packer ->
                Seq.fold (fun pk item -> packItem item pk) packer arr

        let rec packSeqWith arr packItem =
            packLenSeqWith (Seq.length arr) arr packItem

        let unpackLenArrayWith len unpackItem unpacker =
            let arr = Array.zeroCreate len
            for index in seq { 0 .. len-1 } do
                arr.[index] <- unpackItem unpacker
            arr

        let unpackArrayWith unpackItem unpacker =
            let len = unpackInt unpacker
            unpackLenArrayWith len unpackItem unpacker

        let rec packMapWith map packKey packValue =
            let pairs = map |> Map.toSeq
            packSeqWith pairs (fun (k, v) -> packKey k >> packValue v)

        let unpackMapWith unpackKey unpackValue =
            unpackArrayWith (fun upk -> unpackKey upk, unpackValue upk)
            >> Map.ofSeq

        let packStrings value = packSeqWith value packString
        let packDict value = packMapWith value packString packString
        let unpackStrings upk = upk |> unpackArrayWith unpackString |> List.ofArray
        let unpackDict upk = upk |> unpackMapWith unpackString unpackString

        let rec packMetaValue value =
            match value with
            | MetaNull -> packByte MetaNullCode
            | MetaString v -> packByte MetaStringCode >> packString v
            | MetaLong v -> packByte MetaLongCode >> packLong v
            | MetaFloat v -> packByte MetaFloatCode >> packFloat v
            | MetaBool v -> packByte MetaBoolCode >> packBool v
            | MetaDate v -> packByte MetaDateCode >> packDateTime v
            | MetaStrings v -> packByte MetaStringsCode >> packStrings v
            | MetaList v -> packByte MetaListCode >> packList v
            | MetaDict v -> packByte MetaDictCode >> packDict v
            | MetaMap v -> packByte MetaMapCode >> packMap v
        and packList value = packSeqWith value packMetaValue
        and packMap value = packMapWith value packString packMetaValue

        let rec unpackMetaValue upk =
            let code = unpackByte upk
            match code with
            | MetaNullCode -> MetaNull
            | MetaStringCode -> unpackString upk |> MetaString
            | MetaLongCode -> unpackLong upk |> MetaLong
            | MetaFloatCode -> unpackFloat upk |> MetaFloat
            | MetaBoolCode -> unpackBool upk |> MetaBool
            | MetaDateCode -> unpackDateTime upk |> MetaDate
            | MetaStringsCode -> unpackStrings upk |> MetaStrings
            | MetaListCode -> unpackList upk |> MetaList
            | MetaDictCode -> unpackDict upk |> MetaDict
            | MetaMapCode -> unpackMap upk |> MetaMap
            | _ -> invalidOp (sprintf "Unknown meta value code: %d" code)
        and unpackList upk = upk |> unpackListWith unpackMetaValue
        and unpackMap upk = upk |> unpackMapWith unpackString unpackMetaValue

    open Impl

    let packMessageMeta meta = packMap meta
    let unpackMessageMeta upk = unpackMap upk

    let packMessage message =
        packMessageMeta (Message.Meta.get message)
        >> packArray (Message.Data.get message)
