namespace EventStore.WebHost.Controllers

open System
open System.Collections.Generic
open System.Linq
open System.Threading.Tasks
open Microsoft.AspNetCore.Mvc
open Orleans
open FSharp.Control.Tasks.V2
open EventStore.GrainInterfaces

[<Route("api/[controller]")>]
[<ApiController>]
type PropertiesController (cluster: IClusterClient) =
    inherit ControllerBase()

    [<HttpGet>]
    member this.Get() = task {
        let propertiesGrain = cluster.GetGrain<IPropertiesManagerGrain>("default")
        let! propertyNames = propertiesGrain.GetPropertyNames()
        return ActionResult<_>(propertyNames |> List.toArray)
    }
