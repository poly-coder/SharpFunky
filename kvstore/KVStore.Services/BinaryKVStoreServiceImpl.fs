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
                let! valueOpt = kvstore.getValue request.Key
                return true, valueOpt
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
                do! kvstore.putValue request.Key (request.Value.ToByteArray())
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
                do! kvstore.deleteValue request.Key
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
