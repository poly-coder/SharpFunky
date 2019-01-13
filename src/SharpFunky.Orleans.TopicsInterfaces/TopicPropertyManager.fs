namespace SharpFunky.Orleans.Topics

open Orleans
open SharpFunky
open SharpFunky.EventStorage
open System.Threading.Tasks
open System

type GetPropertyRequest = {
    propertyName: string
}

[<RequireQualifiedAccess>]
module GetPropertyRequest =

    let empty: GetPropertyRequest = {
        propertyName = ""
    }
    let propertyName = Lens.cons' (fun s -> s.propertyName) (fun v s -> { s with propertyName = v })
    let create propertyName': GetPropertyRequest =
        empty
        |> Lens.set propertyName propertyName'

type GetPropertyResponse = {
    topicPropertyInfo: TopicPropertyInfo option
}

[<RequireQualifiedAccess>]
module GetPropertyResponse =

    let empty: GetPropertyResponse  = {
        topicPropertyInfo = None
    }
    let topicPropertyInfo = Lens.cons' (fun s -> s.topicPropertyInfo) (fun v s -> { s with topicPropertyInfo = v })
    let failed = empty
    let create topicPropertyInfo': GetPropertyResponse =
        empty
        |> Lens.set topicPropertyInfo (Some topicPropertyInfo')

type CreatePropertyRequest = {
    propertyName: string
}

[<RequireQualifiedAccess>]
module CreatePropertyRequest =

    let empty: CreatePropertyRequest = {
        propertyName = ""
    }
    let propertyName = Lens.cons' (fun s -> s.propertyName) (fun v s -> { s with propertyName = v })
    let create propertyName': CreatePropertyRequest =
        empty
        |> Lens.set propertyName propertyName'

type CreatePropertyResponse = {
    topicPropertyInfo: TopicPropertyInfo option
}

[<RequireQualifiedAccess>]
module CreatePropertyResponse =

    let empty: CreatePropertyResponse  = {
        topicPropertyInfo = None
    }
    let topicPropertyInfo = Lens.cons' (fun s -> s.topicPropertyInfo) (fun v s -> { s with topicPropertyInfo = v })
    let failed = empty
    let create topicPropertyInfo': CreatePropertyResponse =
        empty
        |> Lens.set topicPropertyInfo (Some topicPropertyInfo')

type ListPropertiesResponse = {
    items: TopicPropertyInfo list
}

[<RequireQualifiedAccess>]
module ListPropertiesResponse =

    let empty: ListPropertiesResponse  = {
        items = []
    }
    let items = Lens.cons' (fun s -> s.items) (fun v s -> { s with items = v })
    let create items': ListPropertiesResponse =
        empty
        |> Lens.set items items'

type ITopicPropertyManager =
    inherit IGrainWithStringKey

    abstract GetProperty: request: GetPropertyRequest -> Task<GetPropertyResponse>
    abstract CreateProperty: request: CreatePropertyRequest -> Task<CreatePropertyResponse>
    abstract ListProperties: unit -> Task<ListPropertiesResponse>
