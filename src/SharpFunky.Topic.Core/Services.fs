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

