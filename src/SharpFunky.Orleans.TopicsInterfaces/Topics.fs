namespace SharpFunky.Orleans.Topics

open Orleans
open SharpFunky
open SharpFunky.EventStorage
open System.Threading.Tasks
open System

type TopicPropertyInfo = {
    propertyId: Guid
    propertyName: string
}

type ITopicSubscription =
    inherit IGrainWithGuidKey

type ITopicInstance =
    inherit IGrainWithGuidKey

type ITopicNamespace =
    inherit IGrainWithGuidKey

type ITopicProperty =
    inherit IGrainWithGuidKey

