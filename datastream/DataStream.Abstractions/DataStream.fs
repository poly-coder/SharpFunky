namespace DataStream

open SharpFunky
open System.Threading

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
    metadata: 'meta
}

type DataStreamStatus<'meta> = {
    exists: bool
    metadata: 'meta
    etag: string
}

type GetStatusReq = {
    streamId: string
    cancellationToken: CancellationToken
}

module GetStatusReq =
    let empty = {
        streamId = ""
        cancellationToken = CancellationToken.None
    }

    let setStreamId v req = { req with streamId = v }
    let streamId = Lens.cons' (fun r -> r.streamId) setStreamId

    let setCancellationToken v req = { req with cancellationToken = v }
    let cancellationToken = Lens.cons' (fun r -> r.cancellationToken) setCancellationToken

type SaveStatusReq<'meta> = {
    streamId: string
    etag: string
    metadata: 'meta
    cancellationToken: CancellationToken
}

module SaveStatusReq =
    let empty metadata = {
        streamId = ""
        etag = ""
        metadata = metadata
        cancellationToken = CancellationToken.None
    }

    let setStreamId v req: SaveStatusReq<_> = { req with streamId = v }
    let streamId<'meta> : Lens<SaveStatusReq<'meta>, _> =
        Lens.cons' (fun r -> r.streamId) setStreamId

    let setEtag v req: SaveStatusReq<_> = { req with etag = v }
    let etag<'meta> : Lens<SaveStatusReq<'meta>, _> =
        Lens.cons' (fun r -> r.etag) setEtag

    let setMetadata v req: SaveStatusReq<_> = { req with metadata = v }
    let metadata<'meta> : Lens<SaveStatusReq<'meta>, _> =
        Lens.cons' (fun r -> r.metadata) setMetadata

    let setCancellationToken v req: SaveStatusReq<_> = { req with cancellationToken = v }
    let cancellationToken<'meta> : Lens<SaveStatusReq<'meta>, _> =
        Lens.cons' (fun r -> r.cancellationToken) setCancellationToken

type AppendReq<'seq, 'data, 'meta> = {
    streamId: string
    etag: string
    metadata: 'meta
    items: DataStreamItem<'seq, 'data, 'meta> list
    cancellationToken: CancellationToken
}

module AppendReq =
    let empty metadata items = {
        streamId = ""
        etag = ""
        metadata = metadata
        items = items
        cancellationToken = CancellationToken.None
    }

    let setStreamId v req: AppendReq<_, _, _> = { req with streamId = v }
    let streamId<'seq, 'data, 'meta> : Lens<AppendReq<'seq, 'data, 'meta>, _> =
        Lens.cons' (fun r -> r.streamId) setStreamId

    let setEtag v req: AppendReq<_, _, _> = { req with etag = v }
    let etag<'seq, 'data, 'meta> : Lens<AppendReq<'seq, 'data, 'meta>, _> =
        Lens.cons' (fun r -> r.etag) setEtag

    let setMetadata v req: AppendReq<_, _, _> = { req with metadata = v }
    let metadata<'seq, 'data, 'meta> : Lens<AppendReq<'seq, 'data, 'meta>, _> =
        Lens.cons' (fun r -> r.metadata) setMetadata

    let setItems v req: AppendReq<_, _, _> = { req with items = v }
    let items<'seq, 'data, 'meta> : Lens<AppendReq<'seq, 'data, 'meta>, _> =
        Lens.cons' (fun r -> r.items) setItems

    let setCancellationToken v req: AppendReq<_, _, _> = { req with cancellationToken = v }
    let cancellationToken<'seq, 'data, 'meta> : Lens<AppendReq<'seq, 'data, 'meta>, _> =
        Lens.cons' (fun r -> r.cancellationToken) setCancellationToken

type ReadReq<'seq> = {
    streamId: string
    fromSequence: 'seq
    limit: int
    cancellationToken: CancellationToken
}

module ReadReq =
    let empty fromSequence = {
        streamId = ""
        fromSequence = fromSequence
        limit = 100
        cancellationToken = CancellationToken.None
    }

    let setStreamId v req: ReadReq<_> = { req with streamId = v }
    let streamId<'seq> : Lens<ReadReq<'seq>, _> =
        Lens.cons' (fun r -> r.streamId) setStreamId

    let setFromSequence v req: ReadReq<_> = { req with fromSequence = v }
    let fromSequence<'seq> : Lens<ReadReq<'seq>, _> =
        Lens.cons' (fun r -> r.fromSequence) setFromSequence

    let setLimit v req: ReadReq<_> = { req with limit = v }
    let limit<'seq> : Lens<ReadReq<'seq>, _> =
        Lens.cons' (fun r -> r.limit) setLimit

    let setCancellationToken v req: ReadReq<_> = { req with cancellationToken = v }
    let cancellationToken<'seq> : Lens<ReadReq<'seq>, _> =
        Lens.cons' (fun r -> r.cancellationToken) setCancellationToken

type ReadDataStreamRes<'seq, 'data, 'meta> = {
    nextSequence: 'seq
    items: DataStreamItem<'seq, 'data, 'meta> list
    reachedEnd: bool
}

type IDataStreamService<'seq, 'data, 'meta> =
    abstract getStatus: GetStatusReq -> Async<DataStreamStatus<'meta>>

    abstract saveStatus: SaveStatusReq<'meta> -> Async<DataStreamStatus<'meta>>

    abstract append: AppendReq<'seq, 'data, 'meta> -> Async<DataStreamStatus<'meta>>

    abstract read: ReadReq<'seq> -> Async<ReadDataStreamRes<'seq, 'data, 'meta>>

type DataStreamCommand<'seq, 'data, 'meta> =
    | DoGetStatus of GetStatusReq * Sink<Result<DataStreamStatus<'meta>, exn>>

    | DoSaveStatus of SaveStatusReq<'meta> * Sink<Result<DataStreamStatus<'meta>, exn>>

    | DoAppend of AppendReq<'seq, 'data, 'meta> * Sink<Result<DataStreamStatus<'meta>, exn>>

    | DoRead of ReadReq<'seq> * Sink<Result<ReadDataStreamRes<'seq, 'data, 'meta>, exn>>

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
            member this.getStatus request =
                execute (fun sink -> DoGetStatus(request, sink))

            member this.saveStatus request =
                execute (fun sink -> DoSaveStatus(request, sink))

            member this.append request =
                execute (fun sink -> DoAppend(request, sink))

            member this.read request =
                execute (fun sink -> DoRead(request, sink))
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
         | DoGetStatus(request, sink) ->
             execute sink <| fun () -> service.getStatus request

         | DoSaveStatus(request, sink) ->
             execute sink <| fun () -> service.saveStatus request

         | DoAppend(request, sink) ->
             execute sink <| fun () -> service.append request

         | DoRead(request, sink) ->
             execute sink <| fun () -> service.read request

    let convertedService
         (sequenceConverter: Converter<'seq, 'Seq>)
         (dataConverter: Converter<'data, 'Data>)
         (metaConverter: Converter<'meta, 'Meta>)
         (service: IDataStreamService<'Seq, 'Data, 'Meta>) =

         let statusConverter status = {
            exists = status.exists
            etag = status.etag
            metadata = (Converter.backward metaConverter) status.metadata
         }
         let itemConverter =
            let conv seqConv dataConv metaConv item = {
                data = dataConv item.data
                metadata = metaConv item.metadata
                sequence = seqConv item.sequence
            }
            Converter.cons
                (conv (Converter.forward sequenceConverter) (Converter.forward dataConverter) (Converter.forward metaConverter))
                (conv (Converter.backward sequenceConverter) (Converter.backward dataConverter) (Converter.backward metaConverter))

         let itemsConverter =
            Converter.cons
                (fun items -> items |> List.map (Converter.forward itemConverter))
                (fun items -> items |> List.map (Converter.backward itemConverter))

         { new IDataStreamService<_, _, _> with
            member this.getStatus request = async {
                 let! status' = service.getStatus request
                 return status' |> statusConverter
            }

            member this.saveStatus request = async {
                 let request' =
                    SaveStatusReq.empty 
                        (Converter.forward metaConverter request.metadata)
                    |> SaveStatusReq.setStreamId request.streamId
                    |> SaveStatusReq.setEtag request.etag
                    |> SaveStatusReq.setCancellationToken request.cancellationToken
                 let! status' = service.saveStatus request'
                 return status' |> statusConverter
            }

            member this.append request = async {
                 let request' =
                    AppendReq.empty 
                        (Converter.forward metaConverter request.metadata)
                        (Converter.forward itemsConverter request.items)
                    |> AppendReq.setStreamId request.streamId
                    |> AppendReq.setEtag request.etag
                    |> AppendReq.setCancellationToken request.cancellationToken
                 let! status' = service.append request'
                 return status' |> statusConverter
            }

            member this.read request = async {
                 let request' =
                    ReadReq.empty 
                        (Converter.forward sequenceConverter request.fromSequence)
                    |> ReadReq.setStreamId request.streamId
                    |> ReadReq.setLimit request.limit
                    |> ReadReq.setCancellationToken request.cancellationToken
                 let! response' = service.read request'
                 return {
                     nextSequence = response'.nextSequence |> Converter.backward sequenceConverter
                     items = response'.items |> List.map (Converter.backward itemConverter)
                     reachedEnd = response'.reachedEnd
                 }
            }
         }
