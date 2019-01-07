namespace SharpFunky.Services

open SharpFunky

type IServiceFactory<'t> =
    abstract create: unit -> 't

type IServiceFactoryAsync<'t> =
    abstract create: unit -> Async<'t>

type IKeyServiceFactory<'k, 't> =
    abstract create: 'k -> 't

type IKeyServiceFactoryAsync<'k, 't> =
    abstract create: 'k -> Async<'t>

module ServiceFactory =
    let ofFunc create =
        { new IServiceFactory<_> with
            member __.create() = create() }

    let asFunc (factory: IServiceFactory<_>): Fn<_, _> =
        fun () -> factory.create()

module ServiceFactoryAsync =
    let ofFunc create =
        { new IServiceFactoryAsync<_> with
            member __.create() = create() }

    let asFunc (factory: IServiceFactoryAsync<_>): AsyncFn<_, _> =
        fun () -> factory.create()

    let ofSync syncFactory =
        syncFactory
        |> ServiceFactory.asFunc
        |> AsyncFn.ofFn
        |> ofFunc
