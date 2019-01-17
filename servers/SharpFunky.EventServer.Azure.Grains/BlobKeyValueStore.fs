namespace SharpFunky.EventServer.Azure.Grains

open Orleans
open FSharp.Control.Tasks
open SharpFunky.EventServer.Interfaces
open System.Threading.Tasks
open Microsoft.WindowsAzure.Storage.Blob
open Microsoft.WindowsAzure.Storage

type BlobKeyValueStoreOptions() =
    member val connectionString = "" with get, set
    member val containerName = "" with get, set

type BlobKeyValueStoreGrain(opts: BlobKeyValueStoreOptions) =
    inherit Grain()

    let mutable container: CloudBlobContainer = Unchecked.defaultof<_>

    override this.OnActivateAsync() =
        task {
            let account = CloudStorageAccount.Parse(opts.connectionString)
            let client = account.CreateCloudBlobClient()
            container <- client.GetContainerReference(opts.containerName)
            let! _ = container.CreateIfNotExistsAsync()
            return ()
        } :> Task

    interface IKeyValueStoreGrain with
        member this.get key = task {
            let blob = container.GetBlockBlobReference(key)
            match! blob.ExistsAsync() with
            | true ->
                do! blob.FetchAttributesAsync()
                let length = blob.Properties.Length
                let bytes = Array.zeroCreate (int length)
                let! _ = blob.DownloadToByteArrayAsync(bytes, 0)
                return Some bytes
            
            | false ->
                return None
        }

        member this.put (key, value) = task {
            let blob = container.GetBlockBlobReference(key)
            do! blob.UploadFromByteArrayAsync(value, 0, value.Length)
        }

        member this.remove key =  task {
            let blob = container.GetBlockBlobReference(key)
            let! _ = blob.DeleteIfExistsAsync()
            return ()
        }
