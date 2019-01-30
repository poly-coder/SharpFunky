namespace EventStore.Grains

open Orleans
open EventStore.Abstractions
open EventStore.GrainInterfaces
open FSharp.Control.Tasks.V2
open System
open System.Threading.Tasks

type EventStorePropertiesManagerConfig = {
    name: string
}

type IEventStorePropertiesManagerConfigLocator =
    abstract getConfig: name: string -> Task<EventStorePropertiesManagerConfig>

type EventStorePropertiesManagerGrain(configLocator: IEventStorePropertiesManagerConfigLocator) =
    inherit Grain()

    let mutable config = Unchecked.defaultof<EventStorePropertiesManagerConfig>

    override this.OnActivateAsync() =
        task {
            let key = this.GetPrimaryKeyString()
            let! config' = configLocator.getConfig key
            do config <- config'
        } :> Task

    interface IEventStorePropertiesManagerGrain with

        member this.getProperties request = task {
            return invalidOp "NotImplemented"
        }

        member this.createProperty request = task {
            return invalidOp "NotImplemented"
        }
