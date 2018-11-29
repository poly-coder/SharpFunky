namespace SharpFunky.Topic.Core.Serialization

open System
open MsgPack
open SharpFunky
open SharpFunky.Topic.Core

type IMessagePacking =
    abstract pack: Message -> Packer -> Packer
    abstract unpack: Unpacker -> Message
    abstract unpackMeta: Unpacker -> MessageMeta

module MsgPack =
    let packThis fn (pk: Packer) = fn pk
    let unpackOrExn fn (upk: Unpacker) = fn upk |> Option.ofTryOp |> Option.get

    let packNull pk = packThis (fun pk -> pk.PackNull()) pk
    let packString s = packThis <| fun pk -> pk.PackString(s)
    let unpackString pk = unpackOrExn (fun u -> u.ReadString()) pk
    let packBool v = packThis <| fun pk -> pk.Pack(v: bool)
    let unpackBool pk = unpackOrExn (fun u -> u.ReadBoolean()) pk
    let packByte v = packThis <| fun pk -> pk.Pack(v: byte)
    let unpackByte pk = unpackOrExn (fun u -> u.ReadByte()) pk
    let packInt v = packThis <| fun pk -> pk.Pack(v: int)
    let unpackInt pk = unpackOrExn (fun u -> u.ReadInt32()) pk
    let packLong v = packThis <| fun pk -> pk.Pack(v: int64)
    let unpackLong pk = unpackOrExn (fun u -> u.ReadInt64()) pk
    let packFloat v = packThis <| fun pk -> pk.Pack(v: float)
    let unpackFloat pk = unpackOrExn (fun u -> u.ReadDouble()) pk
        
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

    let packOptionWith opt packValue =
        match opt with
        | None -> packBool false
        | Some v -> packBool true >> packValue v

    let unpackOptionWith unpackValue upk =
        match unpackBool upk with
        | false -> None
        | true -> unpackValue upk |> Some

open MsgPack

module MessagePacking =
    let createVersionedPackingWith packVer unpackVer currentVersion versions =
        if Map.isEmpty versions then 
            failwith "At least one version must be available"
        if  versions |> Map.containsKey currentVersion |> not then 
            failwith "currentVersion is not found in versions map"
        let lastPacking: IMessagePacking = versions |> Map.find currentVersion

        let packMessage message =
            packVer currentVersion 
            >> lastPacking.pack message
        
        let unpackWith fn unpacker =
            let version = unpackVer unpacker
            match versions |> Map.tryFind version with
            | None -> failwith (sprintf "No packing found for version %A" version)
            | Some packing -> fn packing unpacker
        
        let unpackMessage unpacker = unpackWith (fun p -> p.unpack) unpacker
        let unpackMessageMeta unpacker = unpackWith (fun p -> p.unpackMeta) unpacker

        { new IMessagePacking with
            member __.pack message packer = packMessage message packer
            member __.unpack unpacker = unpackMessage unpacker
            member __.unpackMeta unpacker = unpackMessageMeta unpacker
        }

    let createStringVersionedPacking currentVersion versions =
        createVersionedPackingWith packString unpackString currentVersion versions

    let createIntVersionedPacking currentVersion versions =
        createVersionedPackingWith packInt unpackInt currentVersion versions

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
        and unpackList upk = upk |> unpackArrayWith unpackMetaValue |> Array.toList
        and unpackMap upk = upk |> unpackMapWith unpackString unpackMetaValue

    open Impl

    let packMessageMeta meta = packMap meta
    let unpackMessageMeta upk = unpackMap upk

    let packMessage message =
        packMessageMeta (Message.Meta.get message)
        >> packOptionWith (Message.Data.get message) (fun data -> packSeqWith data packByte)

    let unpackMessage upk =
        let meta = unpackMessageMeta upk
        let data = unpackOptionWith (unpackArrayWith unpackByte) upk
        Message.create meta data

    let createPacking() =
        { new IMessagePacking with
            member __.pack message packer = packMessage message packer
            member __.unpack unpacker = unpackMessage unpacker
            member __.unpackMeta unpacker = unpackMessageMeta unpacker
        }


