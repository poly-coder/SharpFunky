namespace SharpFunky

module MBox =

    let reply value (ch: AsyncReplyChannel<_>) = ch.Reply value
    let replyTo (ch: AsyncReplyChannel<_>) value = ch.Reply value

