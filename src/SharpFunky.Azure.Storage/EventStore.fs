module SharpFunky.Storage.EventStore.AzureTables

open System
open SharpFunky
open SharpFunky.Storage
open SharpFunky.AzureStorage.Tables
open Microsoft.WindowsAzure.Storage.Table
open FSharp.Control.Tasks.V2
open SharpFunky.Storage
open System.Diagnostics
open SharpFunky.Services

type Options = {
    table: CloudTable
    partitionKey: string
}

[<RequireQualifiedAccess>]
module Options = 
    let from table partitionKey = 
        {
            table = table
            partitionKey = partitionKey
        }
    let table = Lens.cons' (fun opts -> opts.table) (fun value opts -> { opts with table = value })
    let partitionKey = Lens.cons' (fun opts -> opts.partitionKey) (fun value opts -> { opts with partitionKey = value })


let fromOptions (opts: Options) =
    let statusRowKey = "A_Status"
    let statusRowKey = "A_Status"
    let eventRowKey sequence = sprintf "B_%020i" sequence
    
    let isFrozen = DynamicTableEntity.booleanProperty "IsFrozen"
    let nextSequence = DynamicTableEntity.int64Property "NextSequence"

    let statusToEntity (status: EventStoreStatus) =
        let entity = DynamicTableEntity(opts.partitionKey, statusRowKey)
        entity.ETag <- "*"
        entity
        |> OptLens.setSome isFrozen status.isFrozen
        |> OptLens.setSome nextSequence status.nextSequence

    let statusFromEntity (entity: DynamicTableEntity) =
        EventStoreStatus.empty
        |> Lens.setOpt EventStoreStatus.isFrozen (OptLens.getOpt isFrozen entity)
        |> Lens.setOpt EventStoreStatus.nextSequence (OptLens.getOpt nextSequence entity)

    let eventMetaValueToEntity name (metaValue: MetaValue) (entity: DynamicTableEntity) =
        let name = sprintf "Meta_%s" name
        let prop =
            match metaValue with
            | MetaNull -> None
            | MetaString v ->
                sprintf "S:%s" v
                |> EntityProperty.GeneratePropertyForString
                |> Some
            | MetaStrings vs ->
                String.Join("|", vs |> List.toArray)
                |> sprintf "L:%s"
                |> EntityProperty.GeneratePropertyForString
                |> Some
            | MetaLong v ->
                EntityProperty.GeneratePropertyForLong (Nullable v)
                |> Some
        match prop with
        | Some p -> entity |> DynamicTableEntity.addProperty name p
        | None -> entity
        
    let eventMetaDataToEntity (meta: MetaData) (entity: DynamicTableEntity) =
        do meta
            |> Map.toSeq
            |> Seq.iter (fun (name, value) ->
                entity |> eventMetaValueToEntity name value |> ignore)
        entity

    let eventDataToEntity (event: EventData) =
        eventMetaDataToEntity event.meta

    let eventFromEntity (entity: DynamicTableEntity) =
        PersistedEvent.empty
        // |> Lens.setOpt PersistedEvent.aggregateId (OptLens.getOpt aggregateId entity)

    let status () =
        task {
            match! opts.table |> executeRetrieve opts.partitionKey statusRowKey with
            | Some entity -> return statusFromEntity entity
            | None -> return EventStoreStatus.empty
        } |> AsyncResult.ofTask

    let read (request: GetEventsRequest) =
        task {
            let fromSequence = request.fromSequence |> Option.defaultValue 0L
            let fromSequenceKey = eventRowKey fromSequence
            let takeCount = request.limit |> Option.defaultValue 1000
            let query =
                let where = Query.EntityKey.ge opts.partitionKey fromSequenceKey
                TableQuery().Where(where).Take(Nullable takeCount)
            let! segment = executeQuerySegmented null query opts.table
            let events = segment |> Seq.map eventFromEntity |> Seq.toList
            let nextSequence =
                match events with
                | [] -> fromSequence
                | evs -> (evs |> Seq.map (fun e -> e.sequence) |> Seq.max) + 1L
            return GetEventsResponse.empty
                    |> Lens.set GetEventsResponse.events events
                    |> Lens.set GetEventsResponse.nextSequence nextSequence
        } |> AsyncResult.ofTask

    { new IEventStore with
        member this.status () = status ()
        member this.read request = read request }

let createEventStoreFromPartitions table =
    let create partitionKey =
        let opts: Options = {
            table = table
            partitionKey = partitionKey
        }
        fromOptions opts

    { new IKeyServiceFactory<_, _> with
        member this.create partitionKey = create partitionKey }