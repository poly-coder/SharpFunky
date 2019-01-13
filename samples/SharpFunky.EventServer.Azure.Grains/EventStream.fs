namespace SharpFunky.EventServer.Azure.Grains

open System 
open Orleans 
open FSharp.Control.Tasks
open SharpFunky.EventServer.Interfaces
open System.Threading.Tasks
open Microsoft.WindowsAzure.Storage.Blob
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

type TableEventStreamOptions() =
    member val connectionString = "" with get, set
    member val tableName = "" with get, set

type TableEventStreamGrain(opts: TableEventStreamOptions) =
    inherit Grain()

    let statusRowKey = "A_STATUS"
    let mutable table: CloudTable = Unchecked.defaultof<_>
    let mutable status: EventStreamStatus option = None
    let mutable partition: string = ""
    let mutable subs: ObserverSubscriptionManager<_> = Unchecked.defaultof<_>

    let forLong name value (entity: DynamicTableEntity) =
        let prop = value |> Nullable |> EntityProperty.GeneratePropertyForLong
        entity.Properties.Add(name, prop)
        entity

    let forString name value (entity: DynamicTableEntity) =
        let prop = value |> EntityProperty.GeneratePropertyForString
        entity.Properties.Add(name, prop)
        entity

    let forByteArray name value (entity: DynamicTableEntity) =
        let prop = value |> EntityProperty.GeneratePropertyForByteArray
        entity.Properties.Add(name, prop)
        entity

    let statusOfEntity (entity: DynamicTableEntity) =
        let nextSequence = entity.Properties.["NextSequence"].Int64Value.GetValueOrDefault(0L)
        { nextSequence = nextSequence }

    let statusToEntity (status: EventStreamStatus) =
        let entity = DynamicTableEntity(partition, statusRowKey)
        entity.ETag <- "*"
        entity
        |> forLong "NextSequence" status.nextSequence

    let setMeta prefix meta (entity: DynamicTableEntity) =
        (entity, Map.toSeq meta)
        ||> Seq.fold (fun e (key, value) ->
            forString (sprintf "%s%s" prefix key) value e)

    let setData prefix (bytes: byte[]) (entity: DynamicTableEntity) =
        let chunkSize = 65536
        (entity, seq { 0 .. bytes.Length / chunkSize })
        ||> Seq.fold (fun e index ->
            let startIndex = index * chunkSize
            let size = min chunkSize (bytes.Length - startIndex)
            let chunk = Array.zeroCreate size
            do Array.Copy(bytes, startIndex, chunk, 0, size)
            forByteArray (sprintf "%s%d" prefix index) chunk e)

    let eventToEntity sequence (event: EventData) =
        let rowKey = sprintf "B_%020d" sequence
        let entity = DynamicTableEntity(partition, rowKey)
        entity.ETag <- "*"
        entity
        |> setMeta "Meta_" event.meta
        |> setData "Data_" event.data


    let getOrLoadStatus() = task {
        match status with
        | Some status -> return status
        | None ->
            let retrieve = TableOperation.Retrieve(partition, statusRowKey)
            let! result = table.ExecuteAsync(retrieve)
            let st =
                match result.Result with
                | :? DynamicTableEntity as entity ->
                    statusOfEntity entity
                | _ -> 
                    { nextSequence = 0L }
            do status <- Some st
            return st
    }

    override this.OnActivateAsync() =
        task {
            partition <- this.GetPrimaryKeyString()
            let account = CloudStorageAccount.Parse(opts.connectionString)
            let client = account.CreateCloudTableClient()
            table <- client.GetTableReference(opts.tableName)
            let! _ = table.CreateIfNotExistsAsync()
            subs <- ObserverSubscriptionManager()
            return ()
        } :> Task

    interface IEventStreamGrain with
        member this.getStatus() = task {
            return! getOrLoadStatus()
        }

        member this.commitEvents events = task {
            if events |> List.length <= 0 then
                return invalidOp "Must commit at least one event"
            if events |> List.length > 99 then
                return invalidOp "Cannot commit more than 99 events"

            let! st = getOrLoadStatus()
            let batch = TableBatchOperation()

            let mutable sequence = st.nextSequence
            for event in events do
                do batch.Insert(eventToEntity sequence event)
                do sequence <- sequence + 1L

            let newStatus = { st with nextSequence = sequence }
            do batch.InsertOrReplace(statusToEntity newStatus)

            let! result = table.ExecuteBatchAsync(batch)

            do status <- Some newStatus

            return ()
        }

        member this.subscribe observer = task {
            if subs.IsSubscribed observer |> not then
                subs.Subscribe observer
        }

        member this.unsubscribe observer = task {
            if subs.IsSubscribed observer then
                subs.Unsubscribe observer
        }
