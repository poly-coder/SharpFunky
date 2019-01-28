namespace KVStore.Services

open SharpFunky
open KVStore
open KVStore.Protocols.BinaryKVStore
open FSharp.Control.Tasks.V2
open Google.Protobuf

type BinaryKVStoreServiceImpl(kvstore: IKVStoreService<string, byte[]>) =
    inherit BinaryKVStoreService.BinaryKVStoreServiceBase()

    override this.GetValue(request, context) = task {
        let! success, valueOpt = task {
            try
                let! response = kvstore.getValue {
                    key = request.Key
                    cancellationToken = context.CancellationToken
                }
                return true, response.value
            with
            | _ ->
                return false, None
        }
        let response =
            GetValueResponse()
            |> tee (fun resp ->
                do resp.Success <- success
                match valueOpt with
                | None ->
                    do resp.Found <- false
                | Some value ->
                    do resp.Found <- true
                    do resp.Value <- ByteString.CopyFrom value
            )
        return response
    }

    override this.PutValue(request, context) = task {
        let! success = task {
            try
                do! kvstore.putValue {
                    key = request.Key
                    value = (request.Value.ToByteArray())
                    cancellationToken = context.CancellationToken
                }
                return true
            with
            | _ ->
                return false
        }
        let response =
            PutValueResponse()
            |> tee (fun resp -> do resp.Success <- success)
        return response
    }

    override this.RemoveValue(request, context) = task {
        let! success = task {
            try
                do! kvstore.deleteValue {
                    key = request.Key
                    cancellationToken = context.CancellationToken
                }
                return true
            with
            | _ ->
                return false
        }
        let response =
            RemoveValueResponse()
            |> tee (fun resp -> do resp.Success <- success)
        return response
    }
