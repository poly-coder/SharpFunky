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
open System.Security.Cryptography


type PropertyManagerState = {
    configKey: string
    propertyName: string
    mode: PropertyMode
    accessKey1: string
    accessKey2: string
}

type PropertyManagerEvent =
    | PropertyInitialized of configKey: string * propertyName: string
    | PropertyOpen
    | PropertyFrozen
    | PropertyDeleted
    | PropertyAccessKey1Generated of accessKey1: string
    | PropertyAccessKey2Generated of accessKey2: string

type PropertyManagerStateDefinition() =
    inherit EntityStateDefinition<PropertyManagerState, PropertyManagerEvent>
        ("com.sharpfunky.eventstore.property.")

    override this.newState() = {
        configKey = ""
        propertyName = ""
        mode = PropertyMode.Uninitialized
        accessKey1 = ""
        accessKey2 = ""
    }

    override this.applyMessage state event =
        match event with
        | PropertyInitialized(configKey, propertyName) ->
            { state with
                configKey = configKey
                propertyName = propertyName
                mode = PropertyMode.Uninitialized }

        | PropertyAccessKey1Generated(accessKey1) -> { state with accessKey1 = accessKey1 }

        | PropertyAccessKey2Generated(accessKey2) -> { state with accessKey2 = accessKey2 }

        | PropertyOpen -> { state with mode = PropertyMode.Open }

        | PropertyFrozen -> { state with mode = PropertyMode.Frozen }
        
        | PropertyDeleted -> { state with mode = PropertyMode.Deleted }

    override this.deserializeMessage eventType upk =
        match eventType with
        | "initialized" ->
            let _, configKey = upk.ReadString()
            let _, propertyName = upk.ReadString()
            [ PropertyInitialized(configKey, propertyName) ]

        | "key1generated" ->
            let _, accessKey1 = upk.ReadString()
            [ PropertyAccessKey1Generated(accessKey1) ]

        | "key2generated" ->
            let _, accessKey2 = upk.ReadString()
            [ PropertyAccessKey2Generated(accessKey2) ]

        | "open" -> [ PropertyOpen ]
        | "frozen" -> [ PropertyFrozen ]
        | "deleted" -> [ PropertyDeleted ]

        | _ -> []

    override this.serializeMessage event pk =
        match event with
        | PropertyInitialized(configKey, propertyName) ->
            pk
                .PackString(configKey)
                .PackString(propertyName)
            |> ignore
            "initialized"

        | PropertyAccessKey1Generated(accessKey1) ->
            pk.PackString(accessKey1) |> ignore
            "key1generated"

        | PropertyAccessKey2Generated(accessKey2) ->
            pk.PackString(accessKey2) |> ignore
            "key2generated"

        | PropertyOpen -> "open"
        | PropertyFrozen -> "frozen"
        | PropertyDeleted -> "deleted"


type PropertyManagerConfig = {
    messageStoreLocator: IMessageStoreLocator<Guid>
    maxUnsnapshottedEventsCount: int
    maxUnsnapshottedEventsTime: TimeSpan
    accessKeySize: int
}

type IPropertyManagerConfigLocator =
    abstract getConfig: name: string -> Task<PropertyManagerConfig>

type PropertyManagerGrain
    (
        configLocator: IPropertyManagerConfigLocator,
        grainFactory: IGrainFactory
    ) =
    inherit Grain()

    let stateDef = PropertyManagerStateDefinition() 
                    :> IEntityStateDefinition<PropertyManagerState, PropertyManagerEvent>
    let mutable config = Unchecked.defaultof<PropertyManagerConfig>
    let mutable state = None
    let mutable snapshotStore = Unchecked.defaultof<_>
    let mutable messageStore = Unchecked.defaultof<_>
    let mutable unsnapshottedEvents = 0

    let generateAccessKey() =
        use rng = RandomNumberGenerator.Create()
        let bytes = Array.zeroCreate (config.accessKeySize)
        do rng.GetBytes(bytes)
        String.toBase64 bytes

    interface IPropertyManagerGrain with
        member this.Initialize req = task {
            match state with
            | Some _ ->
                return raise <| PropertyManagerException PropertyIsAlreadyInitialized

            | None ->
                let! config' = configLocator.getConfig(req.configKey)
                let! messageStore' = config'.messageStoreLocator.getMessageStore(this.GetPrimaryKey())
                let! state', _ = EntityState.readEntity stateDef messageStore'
                if String.isEmpty state'.configKey then
                    do! EntityState.writeMessages stateDef messageStore' [
                        PropertyInitialized(req.configKey, req.propertyName)
                        PropertyAccessKey1Generated(generateAccessKey())
                        PropertyAccessKey2Generated(generateAccessKey())
                        PropertyOpen
                    ]
                elif state'.propertyName <> req.propertyName || state'.configKey <> req.configKey then
                    return raise <| PropertyManagerException PropertyIsInitializedDifferently
                do messageStore <- messageStore'
                do config <- config'
                do state <- Some state'
        }

        member this.GetStatus() = task {
            match state with
            | None ->
                return raise <| PropertyManagerException PropertyIsNotInitialized

            | Some st ->
                return {
                    propertyName = st.propertyName
                    mode = st.mode
                    accessKey1 = st.accessKey1
                    accessKey2 = st.accessKey2
                }
        }

