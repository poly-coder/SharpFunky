module SharpFunky.Messaging.MessagePublisher.AzureServiceBus

open SharpFunky
open SharpFunky.Conversion
open SharpFunky.Messaging
open Microsoft.Azure.ServiceBus

type Options<'a> = {
    queue: IQueueClient
    converter: IAsyncConverter<'a, (Map<string, obj> * byte[])>
}

[<RequireQualifiedAccess>]
module Options =
    let from queue converter = 
        {
            queue = queue
            converter = converter
        }

let create opts =
    
    let publish message = async {
        let! meta, data = opts.converter.convert message
        let msg = Message(data)
        for k, v in Map.toSeq meta do
            msg.UserProperties.[k] <- v
        do! opts.queue.SendAsync(msg) |> Async.ofTaskVoid
    }

    MessagePublisher.createInstance publish

