namespace Topics.Services

open SharpFunky
open Topics
open Topics.Protocols.BinaryDataStore
open FSharp.Control.Tasks.V2
open Google.Protobuf

type BinaryDataStoreServiceImpl(dataStore: IDataStoreService<uint64, byte[]>) =
    inherit BinaryDataStoreService.BinaryDataStoreServiceBase()

    override this.GetNextSequence(request, context) = task {
        let! nextSequence = dataStore.getNextSequence ()
        return
            GetNextSequenceResponse()
            |> tee (fun resp -> resp.NextSequence <- nextSequence)
    }

    override this.Append(request, context) = task {
        let messages =
            request.Messages
            |> Seq.map (fun bs -> bs.ToByteArray())
            |> Seq.toList
        let! result = dataStore.append messages
        return
            AppendResponse()
            |> tee (fun resp ->
                do resp.FirstAssignedSequence <- result.firstAssignedSequence
                do resp.NextSequence <- result.nextSequence)
    }

    override this.Read(request, context) = task {
        let! result = dataStore.read {
            fromSequence = request.FromSequence
            limit = request.Limit
        }

        return
            ReadResponse()
            |> tee (fun resp ->
                do resp.ReachedEnd <- result.reachedEnd
                do resp.NextSequence <- result.nextSequence
                do result.messages
                |> Seq.map (fun bytes -> ByteString.CopyFrom(bytes))
                |> resp.Messages.AddRange
            )
    }
