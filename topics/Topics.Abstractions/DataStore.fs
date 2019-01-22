namespace Topics

open SharpFunky

type AppendDataResult<'s> = {
    firstAssignedSequence: 's
    nextSequence: 's
}

type ReadDataRequest<'s> = {
    fromSequence: 's
}

type ReadDataResponse<'s, 'a> = {
    nextSequence: 's
    messages: 'a list
    reachedEnd: bool
}

type IDataStoreService<'s, 'a> =
    abstract member getNextSequence: unit -> Async<'s>

    abstract member append: 'a list -> Async<AppendDataResult<'s>>
    
    abstract member read: ReadDataRequest<'s> -> Async<ReadDataResponse<'s, 'a>>

type DataStoreCommand<'s, 'a> =
    | DoGetNextSequence of Sink<Result<'s, exn>>
    | DoAppend of 'a list * Sink<Result<AppendDataResult<'s>, exn>>
    | DoRead of ReadDataRequest<'s> * Sink<Result<ReadDataResponse<'s, 'a>, exn>>

type DataStoreProcessor<'s, 'a> = Sink<DataStoreCommand<'s, 'a>>


[<AutoOpen>]
module DataStoreExtensions =
    open System.Threading.Tasks

    let serviceFromProcessor (processor: DataStoreProcessor<'s, 'a>) =
        let execute fromSink = async {
            let tcs = TaskCompletionSource()
            let sink = function
                | Ok v -> tcs.TrySetResult v |> ignore
                | Error e -> tcs.TrySetException(e: exn) |> ignore
            do processor <| fromSink sink
            return! tcs.Task |> Async.AwaitTask
        }

        { new IDataStoreService<_, _> with
            member this.getNextSequence () =
                execute (fun sink -> DoGetNextSequence(sink))

            member this.append messages =
                execute (fun sink -> DoAppend(messages, sink))

            member this.read request =
                execute (fun sink -> DoRead(request, sink))
        }

    let processorFromService (service: IDataStoreService<'s, 'a>): DataStoreProcessor<'s, 'a> =
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
        | DoGetNextSequence(sink) ->
            execute sink <| fun () -> service.getNextSequence()

        | DoAppend(messages, sink) ->
            execute sink <| fun () -> service.append messages

        | DoRead(request, sink) ->
            execute sink <| fun () -> service.read request

    let convertedService
        (sequenceConverter: Converter<'s, 'S>)
        (dataConverter: Converter<'a, 'A>)
        (service: IDataStoreService<'S, 'A>) =

        { new IDataStoreService<_, _> with
            member this.getNextSequence () = async {
                let! nextSequence' = service.getNextSequence()
                return nextSequence' |> Converter.backward sequenceConverter
            }

            member this.append messages = async {
                let messages' = messages |> List.map (Converter.forward dataConverter)
                let! result' = service.append messages'
                return {
                    firstAssignedSequence = result'.firstAssignedSequence |> Converter.backward sequenceConverter
                    nextSequence = result'.nextSequence |> Converter.backward sequenceConverter
                }
            }

            member this.read request = async {
                let request' = {
                    fromSequence = request.fromSequence |> Converter.forward sequenceConverter
                }
                let! response' = service.read request'
                return {
                    reachedEnd = response'.reachedEnd
                    nextSequence = response'.nextSequence |> Converter.backward sequenceConverter
                    messages = response'.messages |> List.map (Converter.backward dataConverter)
                }
            }
        }
