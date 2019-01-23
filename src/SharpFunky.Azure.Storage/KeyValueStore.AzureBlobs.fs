module SharpFunky.Storage.KeyValueStore.AzureBlobs

//open SharpFunky
//open SharpFunky.Conversion
//open SharpFunky.Storage
//open Microsoft.WindowsAzure.Storage
//open Microsoft.WindowsAzure.Storage.Blob
//open System.IO

//type Options<'a> = {
//    container: CloudBlobContainer
//    blobPrefix: string
//    converter: IAsyncReversibleConverter<'a, (Map<string, string> * byte[])>
//    updateKey: string -> 'a -> 'a
//}

//[<RequireQualifiedAccess>]
//module Options =
//    let from container converter = 
//        {
//            container = container
//            converter = converter
//            blobPrefix = ""
//            updateKey = fun _ v -> v
//        }
//    let withBlobPrefix value = fun opts -> { opts with blobPrefix = value }
//    let withUpdateKey value = fun opts -> { opts with updateKey = value }

//let create opts =
//    let getBlobName key =
//        sprintf "%s%s" opts.blobPrefix key
//        |> tee NameValidator.ValidateBlobName
        
//    let get key =
//        async {
//            let blobName = getBlobName key
//            let blobRef = opts.container.GetBlockBlobReference(blobName)
//            let! exists = blobRef.ExistsAsync() |> Async.ofTask
//            if not exists then
//                return None
//            else
//                do! blobRef.FetchAttributesAsync() |> Async.ofTaskVoid
//                let meta =
//                    (Map.empty, blobRef.Metadata)
//                    ||> Seq.fold (fun m p -> m |> Map.add p.Key p.Value)
//                use mem = new MemoryStream()
//                do! blobRef.DownloadToStreamAsync(mem) |> Async.ofTaskVoid
//                let bytes = mem.ToArray()
//                let! result = opts.converter.convertBack(meta, bytes)
//                return Some result
//        }

//    let put key value =
//        async {
//            let! converted = opts.converter.convert value
//            let meta, bytes = converted
//            let blobName = getBlobName key
//            let blobRef = opts.container.GetBlockBlobReference(blobName)
//            do! blobRef.FetchAttributesAsync() |> Async.ofTaskVoid
//            for k, v in meta |> Map.toSeq do
//                do blobRef.Metadata.[k] <- v
//            do! blobRef.SetMetadataAsync() |> Async.ofTaskVoid
//            do! blobRef.UploadFromByteArrayAsync(bytes, 0, bytes.Length) |> Async.ofTaskVoid
//        }
        
//    let del key =
//        async {
//            let blobName = getBlobName key
//            let blobRef = opts.container.GetBlockBlobReference(blobName)
//            do! blobRef.DeleteIfExistsAsync() |> Async.ofTaskVoid
//        }

//    KeyValueStore.createInstance get put del
