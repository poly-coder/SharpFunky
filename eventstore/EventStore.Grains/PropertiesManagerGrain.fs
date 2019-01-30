namespace EventStore.Grains.PropertyGrains

open Orleans
open SharpFunky
open EventStore.Abstractions
open EventStore.GrainInterfaces
open FSharp.Control.Tasks.V2
open System
open System.Threading.Tasks
open EventStore.Abstractions.SnapshottedEntityState
open MsgPack
open MsgPack.Serialization
open System.IO

type PropertyItemState = {
    propertyId: Guid
    propertyName: string
}

type PropertiesManagerState = {
    properties: PropertyItemState list
}

type PropertiesManagerEvent =
    | PropertyCreated of name: string * id: Guid

type PropertiesManagerStateDefinition() =
    let EventTypePrefix = "com.sharpfunky.eventstore.properties."
    let eventSubType = String.contentAfter EventTypePrefix
    let asType t = EventTypePrefix + t

    interface ISnapshottedEntityStateDefinition<PropertiesManagerState, PropertiesManagerEvent> with
        member this.newState() = {
            properties = []
        }

        member this.applyMessage state event =
            match event with
            | PropertyCreated(name, id) ->
                let item = { propertyId = id; propertyName = name }
                let properties = state.properties @ [item]
                { state with properties = properties }

        member this.deserializeMessage message =
            maybe {
                let! eventType = message.metadata |> Map.tryFind "eventType"
                let! subType = eventSubType eventType
                match subType with
                | "created" ->
                    use mem = new MemoryStream(message.data)
                    let pk = Unpacker.Create(mem)
                    let mutable name = ""
                    let mutable idBytes = Unchecked.defaultof<_>
                    do pk.ReadString(&name) |> ignore
                    do pk.ReadBinary(&idBytes) |> ignore
                    let id = Guid(idBytes)
                    return [ PropertyCreated(name, id) ]
                | _ -> return! None
            } |> Option.defaultValue []

        member this.serializeMessage event =
            let eventType, data =
                match event with
                | PropertyCreated(name, id) ->
                    use mem = new MemoryStream()
                    let pk = Packer.Create(mem)
                    do pk.PackString(name) |> ignore
                    do pk.PackBinary(id.ToByteArray()) |> ignore
                    do pk.Flush()
                    asType "created", mem.ToArray()
            {
                data = data
                metadata = [ "eventType", eventType] |> Map.ofSeq
                sequence = 0UL
            }

        member this.deserializeSnapshot snapshot =
            maybe {
                let! version = snapshot.metadata |> Map.tryFind "version"
                match version with
                | "1" ->
                    use mem = new MemoryStream(snapshot.data)
                    let pk = Unpacker.Create(mem)
                    let mutable len = 0
                    do pk.ReadInt32(&len) |> ignore
                    let properties = [
                        for _ in 0 .. len - 1 ->
                            let mutable name = ""
                            let mutable idBytes = Unchecked.defaultof<_>
                            do pk.ReadString(&name) |> ignore
                            do pk.ReadBinary(&idBytes) |> ignore
                            let id = Guid(idBytes)
                            { propertyId = id
                              propertyName = name }
                    ]
                    return { properties = properties }
                | _ -> return! None
            }

        member this.serializeSnapshot state = 
            let version, data =
                use mem = new MemoryStream()
                let pk = Packer.Create(mem)
                do pk.Pack(List.length state.properties) |> ignore
                do state.properties
                    |> List.iter (fun prop ->
                        do pk.PackString(prop.propertyName) |> ignore
                        do pk.PackBinary(prop.propertyId.ToByteArray()) |> ignore
                    )

                do pk.Flush()
                "1", mem.ToArray()
            {
                data = data
                metadata = [ "version", version ] |> Map.ofSeq
                sequence = 0UL
            }

type PropertiesManagerConfig = {
    snapshotStoreLocator: ISnapshotStoreLocator
    messageStoreLocator: IMessageStoreLocator
}

type IPropertiesManagerConfigLocator =
    abstract getConfig: name: string -> Task<PropertiesManagerConfig>

type PropertiesManagerGrain(configLocator: IPropertiesManagerConfigLocator) =
    inherit Grain()

    let stateDef = PropertiesManagerStateDefinition() :> ISnapshottedEntityStateDefinition<PropertiesManagerState, PropertiesManagerEvent>
    let mutable state = Unchecked.defaultof<PropertiesManagerState>
    let mutable snapshotStore = Unchecked.defaultof<_>
    let mutable messageStore = Unchecked.defaultof<_>
    let mutable unsnapshottedEvents = 0

    override this.OnActivateAsync() =
        task {
            let key = this.GetPrimaryKeyString()
            let! config = configLocator.getConfig key
            let! snapshotStore' = config.snapshotStoreLocator.getSnapshotStore key
            do snapshotStore <- snapshotStore'
            let! messageStore' = config.messageStoreLocator.getMessageStore key
            do messageStore <- messageStore'
            let! state' = SnapshottedEntityState.readEntity stateDef snapshotStore messageStore
            do state <- state'
            do unsnapshottedEvents <- 0
        } :> Task

    interface IPropertiesManagerGrain with
        member this.GetPropertyNames () = task {
            return state.properties |> List.map (fun p -> p.propertyName)
        }

        member this.CreateProperty propertyName = task {
            return invalidOp "NotImplemented"
        }
    
        member this.GetProperty propertyName = task {
            return invalidOp "NotImplemented"
        }
