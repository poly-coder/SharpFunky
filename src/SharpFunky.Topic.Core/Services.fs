namespace SharpFunky.Topic.Core

open System
open SharpFunky
open SharpFunky.Topic.Core
open FSharp.Control
open System.Threading

type TopicId = string

type PublishedMessageStatus = {
    messageId: MessageId
    topicSequence: TopicSequence
    timestamp: Timestamp
}

type TopicStatus = {
    nextSequence: int64
    totalMessageCount: int64
    lastTimestamp: DateTime option
}

type PublishResult =
| MessagePublished of PublishedMessageStatus
| MessagePublishError of exn

type ReadStartPosition =
| ReadFromStart
| ReadFromLast
| ReadFromNext
| ReadFromSequence of TopicSequence

type ReadMessagesOptions = {
    keepOpen: bool
    cancellationToken: CancellationToken
    fromPosition: ReadStartPosition
    // some filtering and starting conditions
}

module ReadStartPosition =
    let empty = {
        keepOpen = false
        cancellationToken = CancellationToken.None
        fromPosition = ReadFromStart
    }

type ReadMessageResult = 
| FoundMessage of Message
| CurrentEndOfTopic

type ITopicService =
    abstract getStatus: unit -> Async<TopicStatus>
    abstract publish: AsyncSeq<Message> -> AsyncSeq<PublishResult>
    abstract read: ReadMessagesOptions -> AsyncSeq<ReadMessageResult>

type ITopicServiceFactory =
    abstract getTopic: TopicId -> Async<ITopicService option>

module AsyncSeq =
    let copyFrom ma src = async {
        try
            let! sequence = ma
            do! AsyncSeq.iter (fun a -> AsyncSeqSrc.put a src) sequence
            do AsyncSeqSrc.close src
        with exn ->
            do AsyncSeqSrc.error exn src
    }

    let ofAsync ma =
        let src = AsyncSeqSrc.create()
        do src |> copyFrom ma |> Async.start
        AsyncSeqSrc.toAsyncSeq src

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

        module Internals =

            type ReaderState = {
                seqSrc: AsyncSeqSrc<ReadMessageResult>
                options: ReadMessagesOptions
                nextIndex: TopicSequence
            }

            type State = {
                messages: IList<Message>
                readers: ReaderState list
                nextSeq: int64
            }

            module State =
                let create() = {
                    messages = List()
                    readers = []
                    nextSeq = 1L
                }

            type Command =
            | PublishRequest of
                messages: AsyncSeq<Message> * 
                ch: AsyncReplyChannel<AsyncSeq<PublishResult>>
            | GetStatus of
                ch: AsyncReplyChannel<TopicStatus>
            | ReadMessages of
                options: ReadMessagesOptions * 
                ch: AsyncReplyChannel<AsyncSeq<ReadMessageResult>>

            // Internals
            | PostSingleMessage of
                Message *
                ch: AsyncReplyChannel<PublishResult>
            | IncludeReader of ReaderState

        open Internals
        let create () =
            let mailbox = MailboxProcessor.Start(fun mb ->
                let rec loop state = async {

                    let publishRequest messages ch = async {
                        let seqSrc = AsyncSeqSrc.create()

                        async {
                            try
                                do! messages
                                    |> AsyncSeq.iterAsync (fun msg ->
                                        mb.PostAndAsyncReply(fun ch -> PostSingleMessage(msg, ch))
                                        |> Async.map (fun result -> AsyncSeqSrc.put result seqSrc))
                                do AsyncSeqSrc.close seqSrc
                            with exn ->
                                do AsyncSeqSrc.error exn seqSrc
                        }
                        |> Async.start

                        do seqSrc
                            |> AsyncSeqSrc.toAsyncSeq
                            |> MBox.replyTo ch

                        return! loop state
                    }

                    let getStatus ch = async {
                        let status = {
                            nextSequence = state.nextSeq
                            lastTimestamp = state.messages |> Seq.tryLast |> Option.bind Message.Timestamp.tryGet
                            totalMessageCount = int64 state.messages.Count
                        }

                        do status |> MBox.replyTo ch

                        return! loop state
                    }

                    let readMessageUntilCurrent startIndex readOptions seqSrc =
                        let rec readLoop index = async {
                            if readOptions.cancellationToken.IsCancellationRequested then
                                seqSrc |> AsyncSeqSrc.error (OperationCanceledException())
                                return None
                            elif index < int64 state.messages.Count then
                                seqSrc |> AsyncSeqSrc.put (state.messages.[int index] |> FoundMessage)
                                return! readLoop (index + 1L)
                            else
                                if not readOptions.keepOpen then
                                    seqSrc |> AsyncSeqSrc.close
                                    return None
                                else
                                    seqSrc |> AsyncSeqSrc.put CurrentEndOfTopic
                                    return Some index
                        }
                        readLoop startIndex

                    let readMessages (readOptions: ReadMessagesOptions) ch = async {
                        let seqSrc = AsyncSeqSrc.create()

                        let startIndex =
                            let count = state.messages.Count |> int64
                            match readOptions.fromPosition with
                            | ReadFromStart -> 0L
                            | ReadFromLast -> count - 1L
                            | ReadFromNext -> count
                            | ReadFromSequence ind -> max 0L (min count ind)
                        
                        async {
                            match! readMessageUntilCurrent startIndex readOptions seqSrc with
                            | Some nextIndex ->
                                let reader: ReaderState = {
                                    seqSrc = seqSrc
                                    options = readOptions
                                    nextIndex = nextIndex
                                }
                                do mb.Post(IncludeReader reader)
                            | None ->
                                do ()
                        }
                        |> Async.start

                        do seqSrc
                            |> AsyncSeqSrc.toAsyncSeq
                            |> MBox.replyTo ch

                        return! loop state
                    }

                    let updateCurrentReaders state = async {
                        let! readers' =
                            state.readers
                            |> AsyncSeq.ofSeq
                            |> AsyncSeq.collect (fun reader ->
                                readMessageUntilCurrent reader.nextIndex reader.options reader.seqSrc
                                |> Async.map (function
                                    | Some nextIndex -> AsyncSeq.singleton { reader with nextIndex = nextIndex }
                                    | None -> AsyncSeq.empty)
                                |> AsyncSeq.ofAsync)
                            |> AsyncSeq.toListAsync
                        let state' = { state with readers = readers' }
                        return! loop state'
                    }

                    let postSingleMessage message ch = async {
                        let state' = 
                            try
                                let message' =
                                    message
                                    |> Message.MessageId.generateIfMissing
                                    |> Message.TopicSequence.set state.nextSeq
                                    |> Message.Timestamp.set DateTime.UtcNow

                                let info = {
                                    messageId = Message.MessageId.tryGet message' |> Option.get
                                    topicSequence = Message.TopicSequence.tryGet message' |> Option.get
                                    timestamp = Message.Timestamp.tryGet message' |> Option.get
                                }

                                do message'|> state.messages.Add

                                do MessagePublished info |> MBox.replyTo ch
                            
                                { state with nextSeq = state.nextSeq + 1L }
                            with exn ->
                                do MessagePublishError exn |> MBox.replyTo ch
                                state

                        return! updateCurrentReaders state'
                    }

                    let includeReader reader = async {
                        let state' = { state with readers = reader :: state.readers }
                        return! loop state'
                    }

                    match! mb.Receive() with
                    | PublishRequest (messages, ch) ->
                        return! publishRequest messages ch

                    | ReadMessages (readOptions, ch) ->
                        return! readMessages readOptions ch

                    | GetStatus ch ->
                        return! getStatus ch

                    // Internals

                    | PostSingleMessage (message, ch) ->
                        return! postSingleMessage message ch

                    | IncludeReader reader ->
                        return! includeReader reader
                }
                loop (State.create())
            )

            let getStatus () =
                mailbox.PostAndAsyncReply GetStatus

            let publish messages =
                mailbox.PostAndAsyncReply (fun ch -> PublishRequest(messages, ch))
                |> AsyncSeq.ofAsync

            let read options =
                mailbox.PostAndAsyncReply (fun ch -> ReadMessages(options, ch))
                |> AsyncSeq.ofAsync

            { new ITopicService with
                member __.getStatus() = getStatus()
                member __.publish messages = publish messages
                member __.read options = read options
            }

module Delegated =
    module TopicServiceFactory =

        type Options = {
            createTopic: TopicId -> Async<ITopicService option>
        }

        module Options =
            let create createTopic = {
                createTopic = createTopic
            }

        type internal Command =
        | GetTopic of TopicId * ch: AsyncReplyChannel<Result<ITopicService option, exn>>
        
        | ResetTopicCache of TopicId * Guid

        type internal State = {
            topics: Map<string, Async<ITopicService option> * Guid>
        }

        module internal State =
            let create() = {
                topics = Map.empty
            }

        let create options =
            let mailbox = MailboxProcessor.Start(fun mb ->
                let rec loop state = async {
                    match! mb.Receive() with
                    | GetTopic(topicId, ch) ->
                        let asyncTopic, state' = 
                            // Look for cached topic service
                            match state.topics |> Map.tryFind topicId with
                            | Some (topic, _) -> topic, state
                            | None ->
                                let guid = Guid.NewGuid()

                                let topic =
                                    options.createTopic topicId
                                    |> Async.toPromise

                                let state' =
                                    { state with
                                        topics = state.topics |> Map.add topicId (topic, guid) }
                                
                                // If topic creation fails or returns nothing then reset topic key
                                topic
                                    |> AsyncResult.catchedAsync
                                    |> Async.map Option.ofResultOption
                                    |> Async.map (function 
                                        | Some _ -> ()
                                        | None -> do mb.Post(ResetTopicCache(topicId, guid)))
                                    |> Async.start

                                topic, state'

                        // Respond to reply channel
                        asyncTopic 
                            |> AsyncResult.catchedAsync
                            |> Async.map ch.Reply
                            |> Async.start

                        return! loop state'

                    | ResetTopicCache(topicId, guid) ->
                        // Only reset if given guid still match with cached guid
                        let state' =
                            match state.topics |> Map.tryFind topicId with
                            | Some (_, guid') when guid = guid' ->
                                { state with topics = state.topics |> Map.remove topicId }
                            | _ -> state

                        return! loop state'
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
