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
        member this.getValue request = async {
            do NameValidator.ValidateBlobName(request.key)
            let! container = container
            let blob = container.GetBlockBlobReference(request.key)
            use mem = new MemoryStream()
            try
                do! blob.DownloadToStreamAsync(
                        mem,
                        AccessCondition.GenerateEmptyCondition(),
                        BlobRequestOptions(),
                        OperationContext(),
                        request.cancellationToken
                    ) |> Async.AwaitTask
                return {
                    value = mem.ToArray() |> Some
                }
            with
            | exn when isBlobNotFound exn -> return { value = None }
            | exn -> return raise exn
        }

        member this.putValue request = async {
            do NameValidator.ValidateBlobName(request.key)
            let! container = container
            let blob = container.GetBlockBlobReference(request.key)
            do! blob.UploadFromByteArrayAsync(
                    request.value,
                    0,
                    request.value.Length,
                    AccessCondition.GenerateEmptyCondition(),
                    BlobRequestOptions(),
                    OperationContext(),
                    request.cancellationToken
                ) |> Async.AwaitTask
        }

        member this.deleteValue request = async {
            do NameValidator.ValidateBlobName(request.key)
            let! container = container
            let blob = container.GetBlockBlobReference(request.key)
            let! _ =
                blob.DeleteIfExistsAsync(
                    DeleteSnapshotsOption.IncludeSnapshots,
                    AccessCondition.GenerateEmptyCondition(),
                    BlobRequestOptions(),
                    OperationContext(),
                    request.cancellationToken
                ) |> Async.AwaitTask
            return ()
        }
