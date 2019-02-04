namespace EventStore.Grains

open SharpFunky
open EventStore.Abstractions
open EventStore.Abstractions.SnapshottedEntityState
open MsgPack
open System.IO


[<AbstractClass>]
type SnapshottedEntityStateDefinition<'state, 'message>(eventTypePrefix: string) =

    inherit EntityStateDefinition<'state, 'message>(eventTypePrefix)
    
    abstract deserializeSnapshot: version: string -> unpacker: Unpacker -> 'state option

    abstract serializeSnapshot: snapshot: 'state -> packer: Packer -> string

    interface ISnapshottedEntityStateDefinition<'state, 'message> with

        member this.deserializeSnapshot snapshot =
            snapshot.metadata
            |> Map.tryFind "version"
            |> Option.bind (fun version ->
                use mem = new MemoryStream(snapshot.data)
                let unpacker = Unpacker.Create(mem)
                this.deserializeSnapshot version unpacker)

        member this.serializeSnapshot state = 
            let version, data =
                use mem = new MemoryStream()
                let packer = Packer.Create(mem)
                let version = this.serializeSnapshot state packer
                do packer.Flush()
                version, mem.ToArray()
            {
                data = data
                metadata = [ "version", version ] |> Map.ofSeq
                sequence = 0UL
            }
