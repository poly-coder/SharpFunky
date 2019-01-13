namespace SharpFunky.Orleans.Topics

open Orleans
open SharpFunky
open SharpFunky.EventStorage
open System.Threading.Tasks
open System
open FSharp.Control.Tasks.V2
open SharpFunky.Services

type IEventStreamFactoryService =
    inherit IKeyServiceFactoryAsync<string, IEventStreamFactoryAsync>

type EventStreamOptions = {
    factoryService: IEventStreamFactoryService
}

module EventStreamOptions =
    let factoryService = Lens.cons' (fun s -> s.factoryService) (fun v s -> { s with factoryService = v })
    let create factoryService = {
        factoryService = factoryService
    }

type EventStreamGrain(options: EventStreamOptions) =
    inherit Grain()

    interface IEventStreamGrain with
        member this.GetStatus() =
            task {
            }

        member this.Read request =
            task {
            }

        member this.Write request =
            task {
            }

type EventStreamFactoryGrain(grainFactory: IGrainFactory) =
    inherit Grain()

    interface IEventStreamFactoryGrain with
        member __.Create(tableName, partition) =
            task {
                let key = sprintf "%s|%s" tableName partition
                return grainFactory.GetGrain<IEventStreamGrain>(key)
            }
