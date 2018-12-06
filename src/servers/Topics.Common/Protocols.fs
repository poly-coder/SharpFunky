namespace Topics.Protocols
open Google.Protobuf
open Google.Protobuf.Collections

[<AutoOpen>]
module ProtocolsExtensions =

    let metaToMap (meta: MapField<_, _>) =
        meta |> Seq.map (fun p -> p.Key, p.Value) |> Map.ofSeq

    let mapToMeta map (meta: MapField<_, _>) =
        map |> Map.toSeq |> Seq.iter meta.Add

    let fromBytes bytes = ByteString.CopyFrom(bytes)
    let toBytes (string: ByteString) = string.ToByteArray()

module PublishProtoMessage =
    let empty() = PublishProtoMessage()

    let getMessageId (msg: PublishProtoMessage) = msg.MessageId
    let setMessageId messageId (msg: PublishProtoMessage) = msg.MessageId <- messageId; msg

    let getMeta (msg: PublishProtoMessage) = msg.Meta |> metaToMap
    let setMeta meta (msg: PublishProtoMessage) = msg.Meta |> mapToMeta meta; msg

    let getData (msg: PublishProtoMessage) = msg.Data.ToByteArray()
    let setData data (msg: PublishProtoMessage) = msg.Data <- ByteString.CopyFrom(data); msg

    let create messageId meta data =
        empty()
        |> setMessageId messageId
        |> setMeta meta
        |> setData data
