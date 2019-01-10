namespace SharpFunky

module MBox =

    let replyTo (ch: AsyncReplyChannel<_>) = ch.Reply
    let inline reply value = value |> flip replyTo
    
    let receiveFrom (ch: MailboxProcessor<_>) = ch.Receive()
    let postTo (ch: MailboxProcessor<_>) = ch.Post
    let inline post msg = msg |> flip postTo
    let postReplyTo (ch: MailboxProcessor<_>) msgFn = ch.PostAndAsyncReply(msgFn)
    let inline postReply msgFn = msgFn |> flip postReplyTo
