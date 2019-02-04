namespace EventStore.Grains.PropertyGrains

open Orleans
open SharpFunky
open EventStore.Abstractions
open EventStore.GrainInterfaces
open FSharp.Control.Tasks.V2
open System
open System.Threading.Tasks
open EventStore.Abstractions.EntityState
open MsgPack
open System.IO
open EventStore.Grains

type PropertyManagerError =
    | PropertyIsAlreadyInitialized
    | PropertyIsNotInitialized
    | PropertyIsInitializedDifferently
    | PropertyNameAlreadyExists

exception PropertyManagerException of PropertyManagerError

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
    inherit EntityStateDefinition<PropertiesManagerState, PropertiesManagerEvent>
        ("com.sharpfunky.eventstore.properties.")

    override this.newState() = {
        properties = []
    }

    override this.applyMessage state event =
        match event with
        | PropertyCreated(name, id) ->
            let item = { propertyId = id; propertyName = name }
            let properties = state.properties @ [item]
            { state with properties = properties }

    override this.deserializeMessage eventType upk =
        match eventType with
        | "initialized" ->
            let mutable name = ""
            let mutable idBytes = Unchecked.defaultof<_>
            do upk.ReadString(&name) |> ignore
            do upk.ReadBinary(&idBytes) |> ignore
            let id = Guid(idBytes)
            [ PropertyCreated(name, id) ]
        | _ -> []

    override this.serializeMessage event pk =
        match event with
        | PropertyCreated(name, id) ->
            do pk.PackString(name) |> ignore
            do pk.PackBinary(id.ToByteArray()) |> ignore
            "created"

type PropertiesManagerConfig = {
    messageStoreLocator: IMessageStoreLocator<string>
    maxUnsnapshottedEventsCount: int
    maxUnsnapshottedEventsTime: TimeSpan
}

type IPropertiesManagerConfigLocator =
    abstract getConfig: name: string -> Task<PropertiesManagerConfig>

type PropertiesManagerGrain
    (
        configLocator: IPropertiesManagerConfigLocator,
        grainFactory: IGrainFactory
    ) =
    inherit Grain()

    let stateDef = PropertiesManagerStateDefinition() :> IEntityStateDefinition<PropertiesManagerState, PropertiesManagerEvent>
    let mutable state = Unchecked.defaultof<PropertiesManagerState>
    let mutable messageStore = Unchecked.defaultof<_>

    let normalizePropertyName name = name

    let findByName propertyName =
        let propertyName = normalizePropertyName propertyName
        state.properties
        |> List.tryFind (fun p -> p.propertyName = propertyName)

    override this.OnActivateAsync() =
        task {
            let configKey = this.GetPrimaryKeyString()
            let! config = configLocator.getConfig configKey
            let! messageStore' = config.messageStoreLocator.getMessageStore configKey
            let! state', _ = EntityState.readEntity stateDef messageStore'
            do messageStore <- messageStore'
            do state <- state'
        } :> Task

    interface IPropertiesManagerGrain with
        member this.GetPropertyNames () = task {
            return
                state.properties
                |> List.map (fun p -> p.propertyName)
        }

        member this.CreateProperty propertyName = task {
            match findByName propertyName with
            | Some _ ->
                return raise <| PropertyManagerException PropertyNameAlreadyExists
            | None ->
                let property = {
                    propertyName = normalizePropertyName propertyName
                    propertyId = Guid.NewGuid()
                }

                do! EntityState.writeMessages stateDef messageStore [
                    PropertyCreated(property.propertyName, property.propertyId)
                ]

                let manager = grainFactory.GetGrain<IPropertyManagerGrain>(property.propertyId)
                do! manager.Initialize {
                    propertyName = property.propertyName
                    configKey = this.GetPrimaryKeyString()
                }

                return manager
        }
    
        member this.GetProperty propertyName = task {
            match findByName propertyName with
            | Some property ->
                let propertyName = normalizePropertyName propertyName

                let manager = grainFactory.GetGrain<IPropertyManagerGrain>(property.propertyId)
                do! manager.Initialize {
                    propertyName = propertyName
                    configKey = this.GetPrimaryKeyString()
                }
                return Some manager
            | None ->
                return None
        }
