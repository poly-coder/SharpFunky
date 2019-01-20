namespace KVStore

open SharpFunky

type IKVStoreService<'k, 'v> =
    abstract getValue: 'k -> Async<'v option>
    abstract putValue: 'k -> 'v -> Async<unit>
    abstract deleteValue: 'k -> Async<unit>

type KVStoreCommand<'k, 'v> =
    | DoGetValue of key: 'k * Sink<Result<'v option, exn>>
    | DoPutValue of key: 'k * value: 'v * Sink<Result<unit, exn>>
    | DoDeleteValue of key: 'k * Sink<Result<unit, exn>>

type KVStoreProcessor<'k, 'v> = Sink<KVStoreCommand<'k, 'v>>


[<AutoOpen>]
module KVStoreExtensions =
    open System.Threading.Tasks

    let serviceFromProcessor (processor: KVStoreProcessor<'k, 'v>) =
        let execute fromSink = async {
            let tcs = TaskCompletionSource()
            let sink = function
                | Ok v -> tcs.TrySetResult v |> ignore
                | Error e -> tcs.TrySetException(e: exn) |> ignore
            do processor <| fromSink sink
            return! tcs.Task |> Async.AwaitTask
        }

        { new IKVStoreService<_, _> with
            member this.getValue key =
                execute (fun sink -> DoGetValue(key, sink))

            member this.putValue key value =
                execute (fun sink -> DoPutValue(key, value, sink))

            member this.deleteValue key =
                execute (fun sink -> DoDeleteValue(key, sink))
        }

    let processorFromService (service: IKVStoreService<'k, 'v>): KVStoreProcessor<'k, 'v> =
        let execute sink asyncFn =
            async {
                try
                    let! value = asyncFn()
                    sink <| Ok value
                with exn ->
                    sink <| Error exn
            }
            |> Async.Start

        function
        | DoGetValue(key, sink) ->
            execute sink <| fun () -> service.getValue key

        | DoPutValue(key, value, sink) ->
            execute sink <| fun () -> service.putValue key value

        | DoDeleteValue(key, sink) ->
            execute sink <| fun () -> service.deleteValue key

    let convertedService
        (keyConverter: Converter<'k, 'K>)
        (valueConverter: Converter<'v, 'V>)
        (service: IKVStoreService<'K, 'V>) =

        { new IKVStoreService<_, _> with
            member this.getValue key = async {
                let key' = key |> Converter.forward keyConverter
                let! value' = service.getValue key'
                return value' |> Option.map (Converter.backward valueConverter)
            }

            member this.putValue key value = async {
                let key' = key |> Converter.forward keyConverter
                let value' = value |> Converter.forward valueConverter
                return! service.putValue key' value'
            }

            member this.deleteValue key = async {
                let key' = key |> Converter.forward keyConverter
                return! service.deleteValue key'
            }
        }
