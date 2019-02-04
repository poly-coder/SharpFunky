namespace EventStore.GrainInterfaces

open System.Threading.Tasks
open Orleans

type PropertyMode =
    | Uninitialized = 0
    | Open = 1
    | Frozen = 2
    | Deleted = 3

type PropertyStatus = {
    propertyName: string
    mode: PropertyMode
    accessKey1: string
    accessKey2: string
}

type InitializePropertyManagerReq = {
    configKey: string
    propertyName: string
}

type IPropertyManagerGrain =
    inherit IGrainWithGuidKey

    abstract Initialize: request: InitializePropertyManagerReq -> Task<unit>

    abstract GetStatus: unit -> Task<PropertyStatus>

