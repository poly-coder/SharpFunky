module CheckSerialization

open SharpFunky
open SharpFunky.Topic.Core
open SharpFunky.Topic.Core.Serialization
open MsgPack
open System.IO

let test1() =
    let msg = Message.empty
    use mem = new MemoryStream()
    let packer = Packer.Create(mem)
    MsgPackV1.packMessage msg packer |> ignore
    let bytes = mem.ToArray()
    printfn "Bytes [%d]: '%A'" bytes.Length bytes
    use mem = new MemoryStream(bytes)
    let unpacker = Unpacker.Create(mem)
    let msg' = MsgPackV1.unpackMessage unpacker
    printfn "Message: '%A'" msg'
    if msg <> msg' then
        printfn "Messages: '%A' and '%A' are not equal" msg msg'
