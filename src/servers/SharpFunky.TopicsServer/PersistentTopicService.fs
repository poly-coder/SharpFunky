namespace SharpFunky.TopicsServer.Persistent

open SharpFunky.Topic.Core

module TopicService =
    open MongoDB
    open MongoDB.Bson
    open MongoDB.Driver
    open STAN.Client
    open Akka.FSharp
    open Akka.Actor

    //let create (parent: IActorRefFactory) () =
    //    spawn parent
