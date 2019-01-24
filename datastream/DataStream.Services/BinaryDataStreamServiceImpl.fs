namespace DataStream.Services

open SharpFunky
open DataStream
open DataStream.Protocols.BinaryDataStream
open FSharp.Control.Tasks.V2
open Google.Protobuf
open Google.Protobuf.Collections

type BinaryDataStreamServiceImpl(store: IDataStreamService<uint64, byte[], Map<string, string>>) =
    inherit BinaryDataStreamService.BinaryDataStreamServiceBase()

    let fromErrorCode code =
        match code with
        | DataStreamErrorCode.EntityTooLargeError -> ErrorCode.EntityTooLargeError
        | DataStreamErrorCode.DuplicateSequenceError -> ErrorCode.DuplicateSequenceError
        | DataStreamErrorCode.MetadataError -> ErrorCode.MetadataError
        | DataStreamErrorCode.DatabaseNotFoundError -> ErrorCode.DatabaseNotFoundError
        | DataStreamErrorCode.DatabaseFullError -> ErrorCode.DatabaseFullError
        | DataStreamErrorCode.ConcurrencyError -> ErrorCode.ConcurrencyError
        | _ -> ErrorCode.UnknownError

    let mapFromProtobuf (mapField: MapField<_, _>) =
        mapField
        |> Seq.map (fun pair -> pair.Key, pair.Value)
        |> Map.ofSeq

    let mapToProtobuf (mapField: MapField<_, _>) map =
        map
        |> Map.toSeq
        |> Seq.iter (fun (key, value) -> mapField.Add(key, value))

    let dataItemFromProtobuf (item: DataItem) = {
        sequence = item.Sequence
        data = item.Content.ToByteArray()
        metadata = mapFromProtobuf item.Metadata
    }

    let dataItemToProtobuf item =
        DataItem()
        |> tee (fun it ->
            it.Sequence <- item.sequence
            it.Content <- ByteString.CopyFrom(item.data)
            mapToProtobuf it.Metadata item.metadata
        )

    let itemsFromProtobuf (itemsField: RepeatedField<_>) =
        itemsField
        |> Seq.map dataItemFromProtobuf
        |> Seq.toList

    let itemsToProtobuf (itemsField: RepeatedField<_>) items =
        items
        |> Seq.map dataItemToProtobuf
        |> Seq.iter (fun item -> itemsField.Add(item))

    let emptyStatusResponse streamId =
        DataStreamStatus()
        |> tee (fun response ->
            response.StreamId <- streamId
            response.Success <- true
            response.Exists <- false
        )

    let statusResponse streamId exists etag metadata =
        DataStreamStatus()
        |> tee (fun response ->
            response.StreamId <- streamId
            response.Success <- true
            response.Exists <- exists
            response.Etag <- etag
            mapToProtobuf response.Metadata metadata
        )

    let errorStatusResponse streamId errorCode =
        DataStreamStatus()
        |> tee (fun response ->
            response.StreamId <- streamId
            response.Success <- false
            response.ErrorCode <- fromErrorCode errorCode
        )

    let readResponse streamId nextSequence reachedEnd items  =
        ReadResponse()
        |> tee (fun response ->
            response.StreamId <- streamId
            response.Success <- true
            response.NextSequence <- nextSequence
            response.ReachedEnd <- reachedEnd
            itemsToProtobuf response.Items items
        )

    let errorReadResponse streamId errorCode =
        ReadResponse()
        |> tee (fun response ->
            response.StreamId <- streamId
            response.Success <- false
            response.ErrorCode <- fromErrorCode errorCode
        )

    override this.GetStatus(request, context) = task {
        try
            let request' =
                GetStatusReq.empty
                |> GetStatusReq.setStreamId request.StreamId
                |> GetStatusReq.setCancellationToken context.CancellationToken
            let! status = store.getStatus request'
            return statusResponse request.StreamId status.exists status.etag status.metadata
        with
        | DataStreamException code ->
            return errorStatusResponse request.StreamId code
    }

    override this.SaveStatus(request, context) = task {
        try
            let request' =
                SaveStatusReq.empty (mapFromProtobuf request.Metadata)
                |> SaveStatusReq.setStreamId request.StreamId
                |> SaveStatusReq.setEtag request.Etag
                |> SaveStatusReq.setCancellationToken context.CancellationToken
            let! status = store.saveStatus request'
            return statusResponse request.StreamId status.exists status.etag status.metadata
        with
        | DataStreamException code ->
            return errorStatusResponse request.StreamId code
    }

    override this.Append(request, context) = task {
        try
            let request' =
                AppendReq.empty
                    (mapFromProtobuf request.Metadata)
                    (itemsFromProtobuf request.Items)
                |> AppendReq.setStreamId request.StreamId
                |> AppendReq.setEtag request.Etag
                |> AppendReq.setCancellationToken context.CancellationToken
            let! status =
                store.append request'
            return statusResponse request.StreamId status.exists status.etag status.metadata
        with
        | DataStreamException code ->
            return errorStatusResponse request.StreamId code
    }

    override this.Read(request, context) = task {
        try
            let request' =
                ReadReq.empty request.FromSequence
                |> ReadReq.setStreamId request.StreamId
                |> ReadReq.setLimit request.Limit
                |> ReadReq.setCancellationToken context.CancellationToken
            let! response = store.read request'
            return readResponse request.StreamId response.nextSequence response.reachedEnd response.items
        with
        | DataStreamException code ->
            return errorReadResponse request.StreamId code
    }

