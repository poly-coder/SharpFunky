namespace SharpFunky.Orleans.Topics

open Orleans
open SharpFunky
open SharpFunky.EventStorage
open System.Threading.Tasks
open System
open FSharp.Control.Tasks.V2
open Newtonsoft.Json

type TopicPropertyManagerOptions = {
    tableName: string
    eventStreamFactory: IEventStreamFactoryGrain
}

type internal TopicPropertyManagerEvent =
    | PropertyCreated of PropertyCreatedEvent

and internal PropertyCreatedEvent = {
    propertyName: string
    propertyId: Guid
}

type TopicPropertyManagerGrain(options: TopicPropertyManagerOptions) as this =
    inherit Grain()

    [<Literal>]
    let EventTypeKey = "eventtype"
    [<Literal>]
    let PropertyCreatedEventTypeKey = "property-created"

    let getEventText = OptLens.getOpt EventData.dataString
    let setEventText = OptLens.setSome EventData.dataString
    let getEventType = OptLens.getOpt (EventData.metaString EventTypeKey)
    let setEventType = OptLens.setSome (EventData.metaString EventTypeKey)
    let deserializeWith cons text =
        text
        |> JsonConvert.DeserializeObject<_>
        |> cons

    let mutable eventStream = Unchecked.defaultof<IEventStreamGrain>
    let mutable propertiesByName = Map.empty

    let dataToEvent (data: EventData) =
        match getEventText data with
        | Some text -> 
            match getEventType data with
            | Some PropertyCreatedEventTypeKey ->
                text |> deserializeWith PropertyCreated

            | Some eventtype ->
                invalidOp (sprintf "Expected events with known eventtype but found: %s" eventtype)
            | None ->
                invalidOp "Expected events with known eventtype but no type found"
        | None ->
            invalidOp "Expected events with json content but binary data found"

    let eventToData event =
        let eventtype, text = 
            match event with
            | PropertyCreated ev -> PropertyCreatedEventTypeKey, JsonConvert.SerializeObject ev

        EventData.empty
        |> setEventText text
        |> setEventType eventtype

    let applyEvent event =
        match event with
        | PropertyCreated event ->
            let propertyInfo: TopicPropertyInfo = {
                propertyName = event.propertyName
                propertyId = event.propertyId
            }
            do propertiesByName <-
                propertiesByName
                |> Map.add event.propertyName propertyInfo

    let readAllEvents () =
        task {
            let rec loop fromSequence = task {
                let req =
                    ReadEventsRequest.empty
                    |> OptLens.set ReadEventsRequest.fromSequence fromSequence
                let! readResult = eventStream.Read(req)
                do readResult.events
                    |> Seq.map (Lens.get PersistedEvent.event >> dataToEvent)
                    |> Seq.iter applyEvent

                if readResult.hasMore then
                    return! loop (Some readResult.nextSequence)
                else
                    return ()
            }
            return! loop None
        } :> Task

    let getProperty (request: GetPropertyRequest) =
        task {
            match propertiesByName |> Map.tryFind request.propertyName with
            | Some propertyInfo ->
                return GetPropertyResponse.create propertyInfo
            | None ->
                return GetPropertyResponse.failed
        }

    let createProperty (request: CreatePropertyRequest) =
        task {
            let found =
                propertiesByName
                |> Map.containsKey request.propertyName
            if found then
                return CreatePropertyResponse.empty
            else
                let propertyInfo: TopicPropertyInfo = {
                    propertyId = Guid.NewGuid()
                    propertyName = request.propertyName
                }
                let events = [
                    PropertyCreated {
                        propertyId = propertyInfo.propertyId
                        propertyName = propertyInfo.propertyName
                    }
                ]
                let eventsData = events |> List.map eventToData
                let writeRequest = WriteEventsRequest.create eventsData
                do! eventStream.Write writeRequest
                do events |> Seq.iter applyEvent
                return CreatePropertyResponse.create propertyInfo
        }

    let listProperties () =
        task {
            let items =
                propertiesByName
                |> Map.toSeq
                |> Seq.map snd
                |> Seq.toList
            return ListPropertiesResponse.create items
        }

    override this.OnActivateAsync() =
        task {
            let partition = this.GetPrimaryKeyString()
            let! eventStream' = options.eventStreamFactory.Create(options.tableName, partition)
            do eventStream <- eventStream'
            do! readAllEvents()
        } :> Task

    interface ITopicPropertyManager with
        member __.GetProperty request = getProperty request
        member __.CreateProperty request = createProperty request
        member __.ListProperties () = listProperties ()
