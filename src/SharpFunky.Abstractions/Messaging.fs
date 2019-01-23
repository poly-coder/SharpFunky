namespace SharpFunky
//namespace SharpFunky.Messaging

//open SharpFunky
//open System

//type IMessagePublisher<'m, 'r> =
//    abstract publish: 'm -> Async<'r>

//type IMessageSubscriber<'m> =
//    abstract subscribe: AsyncFn<'m, unit> -> IDisposable

//module MessagePublisher =

//    let createInstance publish =
//        { new IMessagePublisher<'m, 'r> with
//            member __.publish message = publish message }

//    let toFun (converter: IMessagePublisher<'a, 'b>) =
//        fun a -> converter.publish a

//    let convertInput converter (publisher: IMessagePublisher<'b, 'r>) =
//        converter |> AsyncFn.bind (toFun publisher) |> createInstance

//    let convertOutput converter (publisher: IMessagePublisher<'b, 'r>) =
//        toFun publisher |> AsyncFn.bind converter |> createInstance

//module MessageSubscriber =

//    let createInstance subscribe =
//        { new IMessageSubscriber<'m> with
//            member __.subscribe handler = subscribe handler }
