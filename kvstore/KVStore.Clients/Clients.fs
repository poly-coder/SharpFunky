namespace KVStore.Clients

open SharpFunky
open KVStore
open KVStore.Protocols.BinaryKVStore
open Grpc.Core
open Google.Protobuf

type BinaryKVStoreClient(client: BinaryKVStoreService.BinaryKVStoreServiceClient) =

    interface IKVStoreService<string, byte[]> with
        member this.getValue request = async {
            let req =
                GetValueRequest()
                |> tee (fun req -> req.Key <- request.key)
            let options = CallOptions().WithCancellationToken(request.cancellationToken)
            let! response = client.GetValueAsync(req, options).ResponseAsync |> Async.AwaitTask
            if not response.Success then
                return invalidOp "Operation failed"
            elif not response.Found then
                return { value = None }
            else
                return { value = response.Value.ToByteArray() |> Some }
        }

        member this.putValue request = async {
            let req =
                PutValueRequest()
                |> tee (fun req -> 
                    do req.Key <- request.key
                    do req.Value <- ByteString.CopyFrom request.value
                )
            let options = CallOptions().WithCancellationToken(request.cancellationToken)
            let! response = client.PutValueAsync(req, options).ResponseAsync |> Async.AwaitTask
            if not response.Success then
                return invalidOp "Operation failed"
            else
                return ()
        }

        member this.deleteValue request = async {
            let req =
                RemoveValueRequest()
                |> tee (fun req -> req.Key <- request.key)
            let options = CallOptions().WithCancellationToken(request.cancellationToken)
            let! response = client.RemoveValueAsync(req, options).ResponseAsync |> Async.AwaitTask
            if not response.Success then
                return invalidOp "Operation failed"
            else
                return ()
        }
