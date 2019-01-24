namespace DataStream

open SharpFunky

type DataStreamErrorCode =
    | UnknownError = 0
    | EntityTooLargeError = 1
    | DuplicateSequenceError = 2
    | MetadataError = 3
    | DatabaseNotFoundError = 4
    | DatabaseFullError = 5
    | ConcurrencyError = 6

exception DataStreamException of DataStreamErrorCode

type DataStreamItem<'seq, 'data, 'meta> = {
    sequence: 'seq
    data: 'data
    metadata: Map<string, string>
}

type DataStreamStatus<'meta> = {
    exists: bool
    metadata: 'meta
    etag: string
}

type ReadDataStreamResponse<'seq, 'data, 'meta> = {
    nextSequence: 'seq
    items: DataStreamItem<'seq, 'data, 'meta> list
    reachedEnd: bool
}

type IDataStreamService<'seq, 'data, 'meta> =
    abstract getStatus: streamId: string -> Async<DataStreamStatus<'meta>>
    abstract saveStatus: streamId: string * etag: string * metadata: 'meta -> Async<DataStreamStatus<'meta>>
    abstract append: streamId: string * etag: string * metadata: 'meta * items: DataStreamItem<'seq, 'data, 'meta> -> Async<DataStreamStatus<'meta>>
    abstract read: streamId: string * fromSequence: 'seq * limit: int -> Async<ReadDataStreamResponse<'seq, 'data, 'meta>>

type DataStreamCommand<'seq, 'data, 'meta> =
    | DoGetStatus of streamId: string * Sink<Result<DataStreamStatus<'meta>, exn>>
    | DoSaveStatus of streamId: string * etag: string * metadata: 'meta * Sink<Result<DataStreamStatus<'meta>, exn>>
    | DoAppend of streamId: string * etag: string * metadata: 'meta * items: DataStreamItem<'seq, 'data, 'meta> * Sink<Result<DataStreamStatus<'meta>, exn>>
    | DoRead of streamId: string * fromSequence: 'seq * limit: int * Sink<Result<ReadDataStreamResponse<'seq, 'data, 'meta>, exn>>

type DataStreamProcessor<'seq, 'data, 'meta> = Sink<DataStreamCommand<'seq, 'data, 'meta>>


[<AutoOpen>]
module DataStreamExtensions =
    open System.Threading.Tasks

    let serviceFromProcessor (processor: DataStreamProcessor<'seq, 'data, 'meta>) =
        let execute fromSink = async {
            let tcs = TaskCompletionSource()
            let sink = function
                | Ok v -> tcs.TrySetResult v |> ignore
                | Error e -> tcs.TrySetException(e: exn) |> ignore
            do processor <| fromSink sink
            return! tcs.Task |> Async.AwaitTask
        }

        { new IDataStreamService<_, _, _> with
            member this.getStatus streamId =
                execute (fun sink -> DoGetStatus(streamId, sink))

            member this.saveStatus(streamId, etag, metadata) =
                execute (fun sink -> DoSaveStatus(streamId, etag, metadata, sink))

            member this.append(streamId, etag, metadata, items) =
                execute (fun sink -> DoAppend(streamId, etag, metadata, items, sink))

            member this.read(streamId, fromSequence, limit) =
                execute (fun sink -> DoRead(streamId, fromSequence, limit, sink))
        }

    let processorFromService (service: IDataStreamService<'seq, 'data, 'meta>): DataStreamProcessor<'seq, 'data, 'meta> =
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
         | DoGetStatus(streamId, sink) ->
             execute sink <| fun () -> service.getStatus streamId

         | DoSaveStatus(streamId, etag, metadata, sink) ->
             execute sink <| fun () -> service.saveStatus(streamId, etag, metadata)

         | DoAppend(streamId, etag, metadata, items, sink) ->
             execute sink <| fun () -> service.append(streamId, etag, metadata, items)

         | DoRead(streamId, fromSequence, limit, sink) ->
             execute sink <| fun () -> service.read(streamId, fromSequence, limit)

    //let convertedService
    //     (sequenceConverter: Converter<'seq, 'Seq>)
    //     (dataConverter: Converter<'data, 'Data>)
    //     (metaConverter: Converter<'meta, 'Meta>)
    //     (service: IDataStreamService<'Seq, 'Data, 'Meta>) =

    //     let statusConverter status = {
    //        exists = status.exists
    //        etag = status.etag
    //        metadata = (Converter.backward metaConverter) status.metadata
    //     }

    //     { new IDataStreamService<_, _, _> with
    //        member this.getStatus streamId = async {
    //             let! status' = service.getStatus streamId
    //             return status' |> statusConverter
    //        }

    //        member this.saveStatus(streamId, etag, metadata) = async {
    //             let metadata' = metadata |> Converter.forward metaConverter
    //             let! status' = service.saveStatus(streamId, etag, metadata')
    //             return status' |> statusConverter
    //        }

    //        member this.append(streamId, etag, metadata, items) = async {
    //             let key' = key |> Converter.forward keyConverter
    //             let! value' = service.getValue key'
    //             return value' |> Option.map (Converter.backward valueConverter)
    //        }

    //        member this.read(streamId, fromSequence, limit) = async {
    //             let key' = key |> Converter.forward keyConverter
    //             let! value' = service.getValue key'
    //             return value' |> Option.map (Converter.backward valueConverter)
    //        }
    //     }
