module SerializationTests

open NUnit.Framework
open FsCheck
open Swensen.Unquote
open SharpFunky
open SharpFunky.Topic.Core
open SharpFunky.Topic.Core.Serialization
open System.IO
open MsgPack

[<Test>]
let ``MsgPackV1.packMessage is inverse of MsgPackV1.unpackMessage``() =
    Check.QuickThrowOnFailure <| fun msg ->
        use mem = new MemoryStream()
        let packer = Packer.Create(mem)
        MsgPackV1.packMessage msg packer |> ignore
        let bytes = mem.ToArray()
        use mem = new MemoryStream(bytes)
        let unpacker = Unpacker.Create(mem)
        let msg' = MsgPackV1.unpackMessage unpacker
        test <@ msg = msg' @>
