namespace KVStore.Azure.Storage

open System
open System.IO
open System.Net
open SharpFunky
open SharpFunky.AzureStorage
open KVStore
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob
open Microsoft.WindowsAzure.Storage.Blob.Protocol

type AzureBlobContainerKVStoreOptions() =
    member val StorageConnectionString = "UseDevelopmentStorage=True" with get, set
    member val ContainerName = "kvstore" with get, set

type AzureBlobContainerKVStore(options: AzureBlobContainerKVStoreOptions) =

    let account = Account.parse options.StorageConnectionString
    do NameValidator.ValidateContainerName(options.ContainerName)

    let isBlobNotFound = isErrorCode BlobErrorCodeStrings.BlobNotFound

    let container =
        async {
            try
                let client = account.CreateCloudBlobClient()
                let container = client.GetContainerReference(options.ContainerName)
                let! created = container.CreateIfNotExistsAsync() |> Async.AwaitTask
                return container
            with exn ->
                printfn "%O" exn
                return raise exn
        } |> Async.toPromise

    interface IKVStoreService<string, byte[]> with
        member this.getValue key = async {
            do NameValidator.ValidateBlobName(key)
            let! container = container
            let blob = container.GetBlockBlobReference(key)
            use mem = new MemoryStream()
            try
                do! blob.DownloadToStreamAsync(mem) |> Async.AwaitTask
                return mem.ToArray() |> Some
            with
            | exn when isBlobNotFound exn -> return None
            | exn -> return raise exn
        }

        member this.putValue key value = async {
            do NameValidator.ValidateBlobName(key)
            let! container = container
            let blob = container.GetBlockBlobReference(key)
            do! blob.UploadFromByteArrayAsync(value, 0, value.Length) |> Async.AwaitTask
        }

        member this.deleteValue key = async {
            do NameValidator.ValidateBlobName(key)
            let! container = container
            let blob = container.GetBlockBlobReference(key)
            let! _ = blob.DeleteIfExistsAsync() |> Async.AwaitTask
            return ()
        }
