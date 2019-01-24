namespace DataStream.Clients

open SharpFunky
open DataStream
open DataStream.Protocols.BinaryDataStream
open Grpc.Core
open Google.Protobuf
open Google.Protobuf.Collections

type BinaryDataStreamClient(client: BinaryDataStreamService.BinaryDataStreamServiceClient) =

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

    let fromErrorCode code =
        match code with
        | ErrorCode.EntityTooLargeError -> DataStreamErrorCode.EntityTooLargeError
        | ErrorCode.DuplicateSequenceError -> DataStreamErrorCode.DuplicateSequenceError
        | ErrorCode.MetadataError -> DataStreamErrorCode.MetadataError
        | ErrorCode.DatabaseNotFoundError -> DataStreamErrorCode.DatabaseNotFoundError
        | ErrorCode.DatabaseFullError -> DataStreamErrorCode.DatabaseFullError
        | ErrorCode.ConcurrencyError -> DataStreamErrorCode.ConcurrencyError
        | _ -> DataStreamErrorCode.UnknownError

    let fromStatusResponse (status: DataStreamStatus) =
        if status.Success then
            {
                exists = status.Exists
                metadata = mapFromProtobuf status.Metadata
                etag = status.Etag
            }
        else
            fromErrorCode status.ErrorCode
            |> DataStreamException
            |> raise

    interface IDataStreamService<uint64, byte[], Map<string, string>> with
        member this.getStatus request = async {
            let request =
                GetStatusRequest()
                |> tee (fun req -> req.StreamId <- request.streamId)
            let! status = client.GetStatusAsync(request, CallOptions()).ResponseAsync |> Async.AwaitTask
            return fromStatusResponse status
        }

        member this.saveStatus request = async {
            let request =
                SaveStatusRequest()
                |> tee (fun req ->
                    req.StreamId <- request.streamId
                    req.Etag <- request.etag
                    mapToProtobuf req.Metadata request.metadata
                )
            let! status = client.SaveStatusAsync(request, CallOptions()).ResponseAsync |> Async.AwaitTask
            return fromStatusResponse status
        }

        member this.append request = async {
            let request =
                AppendRequest()
                |> tee (fun req ->
                    req.StreamId <- request.streamId
                    req.Etag <- request.etag
                    itemsToProtobuf req.Items request.items
                    mapToProtobuf req.Metadata request.metadata
                )
            let! status = client.AppendAsync(request, CallOptions()).ResponseAsync |> Async.AwaitTask
            return fromStatusResponse status
        }

        member this.read request = async {
            let request =
                ReadRequest()
                |> tee (fun req ->
                    req.StreamId <- request.streamId
                    req.FromSequence <- request.fromSequence
                    req.Limit <- request.limit
                )
            let! response = client.ReadAsync(request, CallOptions()).ResponseAsync |> Async.AwaitTask
            if response.Success then
                return {
                    nextSequence = response.NextSequence
                    items = itemsFromProtobuf response.Items
                    reachedEnd = response.ReachedEnd
                }
            else
                return fromErrorCode response.ErrorCode
                |> DataStreamException
                |> raise
        }
