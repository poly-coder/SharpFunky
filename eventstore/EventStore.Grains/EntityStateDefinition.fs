﻿namespace EventStore.Grains

open SharpFunky
open EventStore.Abstractions
open EventStore.Abstractions.EntityState
open MsgPack
open System.IO

[<AbstractClass>]
type EntityStateDefinition<'state, 'message>(eventTypePrefix: string) =

    let eventSubType = String.contentAfter eventTypePrefix
    let asType typeName = eventTypePrefix + typeName

    abstract newState: unit -> 'state
    
    abstract applyMessage: 'state -> 'message -> 'state
    
    abstract deserializeMessage: eventType: string -> unpacker: Unpacker -> 'message list

    abstract serializeMessage: event: 'message -> packer: Packer -> string

    interface IEntityStateDefinition<'state, 'message> with
        member this.newState() = this.newState()

        member this.applyMessage state event = this.applyMessage state event

        member this.deserializeMessage message =
            message.metadata
            |> Map.tryFind "eventType"
            |> Option.bind eventSubType
            |> Option.map (fun subType ->
                use mem = new MemoryStream(message.data)
                let unpacker = Unpacker.Create(mem)
                this.deserializeMessage subType unpacker)
            |> Option.defaultValue List.Empty

        member this.serializeMessage event =
            let eventType, data =
                use mem = new MemoryStream()
                let packer = Packer.Create(mem)
                let subType = this.serializeMessage event packer
                do packer.Flush()
                asType subType, mem.ToArray()
            {
                data = data
                metadata =
                    [
                        "eventType", eventType
                    ] |> Map.ofSeq
                sequence = 0UL
            }