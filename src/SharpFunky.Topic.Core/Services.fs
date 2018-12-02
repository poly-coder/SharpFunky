namespace SharpFunky.Topic.Core

open System
open MsgPack
open SharpFunky
open SharpFunky.Topic.Core
open FSharp.Control

type ITopicService =
    abstract publish: AsyncSeq<Message> -> Async<unit>
    abstract startPublish: AsyncSeq<Message> -> Async<unit>

type ITopicServiceFactory =
    abstract getTopic: topicId: string -> Async<ITopicService option>

module InMemory =
    module TopicService =
        open System.Collections.Generic

        type Options = {
            publishBufferingSize: int
            publishBufferingTimeoutMs: int
        }

        module Options =
            let empty = {
                publishBufferingSize = 100
                publishBufferingTimeoutMs = 100
            }

        type internal Command =
        | PublishRequest of messages: AsyncSeq<Message> * waitDone: bool * ch: AsyncReplyChannel<Result<unit, exn>>
        | PublishMessageNext of Message * ch: AsyncReplyChannel<unit>
        | PublishMessageComplete of ch: AsyncReplyChannel<Result<unit, exn>> option
        | PublishMessageError of exn: exn * ch: AsyncReplyChannel<Result<unit, exn>> option

        type internal State = {
            messages: IList<Message>
            nextSeq: int64
        }

        module internal State =
            let create() = {
                messages = List()
                nextSeq = 1L
            }

        let create () =
            let mailbox = MailboxProcessor.Start(fun mb ->
                let rec loop state = async {
                    match! mb.Receive() with
                    | PublishRequest(messages, wait, ch) ->
                        if not wait then
                            do ch.Reply(Result.ok())
                        
                        Async.Start(async {
                            try
                                do! messages |> AsyncSeq.iterAsync (fun msg -> 
                                    mb.PostAndAsyncReply(fun c -> PublishMessageNext(msg, c)))
                                if wait then do ch.Reply(Result.ok())
                            with exn ->
                                if wait then do ch.Reply(Result.error exn)
                        })

                        return! loop state

                    | PublishMessageNext (message, ch) ->
                        do message
                        |> Message.TopicSequence.set state.nextSeq
                        |> Message.Timestamp.set DateTime.UtcNow
                        |> state.messages.Add
                        let state' = 
                            { state with
                                nextSeq = state.nextSeq + 1L
                            }
                        do ch.Reply ()
                        return! loop state'

                    | PublishMessageComplete ch ->
                        match ch with
                        | Some ch -> do ch.Reply <| Result.ok ()
                        | _ -> do ()
                        return! loop state

                    | PublishMessageError (exn, ch) ->
                        match ch with
                        | Some ch -> do ch.Reply <| Result.error exn
                        | _ -> do ()
                }
                loop (State.create())
            )

            let post fn =
                mailbox.PostAndAsyncReply fn
                |> Async.map Result.getOrRaise
            
            let publish messages = post (fun ch -> PublishRequest(messages, false, ch))
            let startPublish messages = post (fun ch -> PublishRequest(messages, true, ch))

            { new ITopicService with
                member __.publish messages = publish messages
                member __.startPublish messages = startPublish messages
            }

    module TopicServiceFactory =

        type internal Command =
        | GetTopic of topicId: string * ch: AsyncReplyChannel<Result<ITopicService option, exn>>

        type internal State = {
            topics: Map<string, ITopicService>
        }

        module internal State =
            let create() = {
                topics = Map.empty
            }

        let create () =
            let mailbox = MailboxProcessor.Start(fun mb ->
                let rec loop state = async {
                    match! mb.Receive() with
                    | GetTopic(topicId, ch) ->
                        match state.topics |> Map.tryFind topicId with
                        | Some topic ->
                            do ch.Reply(topic |> Some |> Result.ok)
                            return! loop state
                        | None ->
                            try
                                let topic = TopicService.create()
                                let state' =
                                    { state with
                                        topics = state.topics |> Map.add topicId topic }
                                do ch.Reply(topic |> Some |> Result.ok)
                                return! loop state'
                            with exn ->
                                do ch.Reply(exn |> Result.error)
                                return! loop state
                }
                loop (State.create())
            )

            let post fn =
                mailbox.PostAndAsyncReply fn
                |> Async.map Result.getOrRaise
            
            let getTopic topicId = post (fun ch -> GetTopic(topicId, ch))

            { new ITopicServiceFactory with
                member __.getTopic topicId = getTopic topicId
            }
