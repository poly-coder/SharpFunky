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
