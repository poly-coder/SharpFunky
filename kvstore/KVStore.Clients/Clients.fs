namespace KVStore.Clients

open SharpFunky
open KVStore
open KVStore.Protocols.BinaryKVStore
open Grpc.Core
open Google.Protobuf

type BinaryKVStoreClient(client: BinaryKVStoreService.BinaryKVStoreServiceClient) =

    interface IKVStoreService<string, byte[]> with
        member this.getValue key = async {
            let request =
                GetValueRequest()
                |> tee (fun req -> req.Key <- key)
            let options = CallOptions()
            let! response = client.GetValueAsync(request, options).ResponseAsync |> Async.AwaitTask
            if not response.Success then
                return invalidOp "Operation failed"
            elif not response.Found then
                return None
            else
                return response.Value.ToByteArray() |> Some
        }

        member this.putValue key value = async {
            let request =
                PutValueRequest()
                |> tee (fun req -> 
                    do req.Key <- key
                    do req.Value <- ByteString.CopyFrom value
                )
            let options = CallOptions()
            let! response = client.PutValueAsync(request, options).ResponseAsync |> Async.AwaitTask
            if not response.Success then
                return invalidOp "Operation failed"
            else
                return ()
        }

        member this.deleteValue key = async {
            let request =
                RemoveValueRequest()
                |> tee (fun req -> req.Key <- key)
            let options = CallOptions()
            let! response = client.RemoveValueAsync(request, options).ResponseAsync |> Async.AwaitTask
            if not response.Success then
                return invalidOp "Operation failed"
            else
                return ()
        }
