namespace KVStore

open SharpFunky
open System.Threading

type GetValueReq<'k> = {
    key: 'k
    cancellationToken: CancellationToken
}

module GetValueReq =
    let create key = {
        key = key
        cancellationToken = CancellationToken.None
    }

    let setKey v req = { req with key = v }
    let key<'k> : Lens<GetValueReq<'k>, _> =
        Lens.cons' (fun r -> r.key) setKey

    let setCancellationToken v req = { req with cancellationToken = v }
    let cancellationToken<'k> : Lens<GetValueReq<'k>, _> =
        Lens.cons' (fun r -> r.cancellationToken) setCancellationToken

type GetValueRes<'v> = {
    value: 'v option
}

type PutValueReq<'k, 'v> = {
    key: 'k
    value: 'v
    cancellationToken: CancellationToken
}

module PutValueReq =
    let create key value = {
        key = key
        value = value
        cancellationToken = CancellationToken.None
    }

    let setKey v req: PutValueReq<_, _> = { req with key = v }
    let key<'k, 'v> : Lens<PutValueReq<'k, 'v>, _> =
        Lens.cons' (fun r -> r.key) setKey

    let setValue v req: PutValueReq<_, _> = { req with value = v }
    let value<'k, 'v> : Lens<PutValueReq<'k, 'v>, _> =
        Lens.cons' (fun r -> r.value) setValue

    let setCancellationToken v req: PutValueReq<_, _> = { req with cancellationToken = v }
    let cancellationToken<'k, 'v> : Lens<PutValueReq<'k, 'v>, _> =
        Lens.cons' (fun r -> r.cancellationToken) setCancellationToken

type DeleteValueReq<'k> = {
    key: 'k
    cancellationToken: CancellationToken
}

module DeleteValueReq =
    let create key = {
        key = key
        cancellationToken = CancellationToken.None
    }

    let setKey v req: DeleteValueReq<_> = { req with key = v }
    let key<'k> : Lens<DeleteValueReq<'k>, _> =
        Lens.cons' (fun r -> r.key) setKey

    let setCancellationToken v req: DeleteValueReq<_> = { req with cancellationToken = v }
    let cancellationToken<'k> : Lens<DeleteValueReq<'k>, _> =
        Lens.cons' (fun r -> r.cancellationToken) setCancellationToken

type IKVStoreService<'k, 'v> =
    abstract getValue: GetValueReq<'k> -> Async<GetValueRes<'v>>
    abstract putValue: PutValueReq<'k, 'v> -> Async<unit>
    abstract deleteValue: DeleteValueReq<'k> -> Async<unit>


[<AutoOpen>]
module KVStoreExtensions =
    let convertedService
        (keyConverter: Converter<'k, 'K>)
        (valueConverter: Converter<'v, 'V>)
        (service: IKVStoreService<'K, 'V>) =

        { new IKVStoreService<_, _> with
            member this.getValue request = async {
                let request': GetValueReq<_> = {
                    key = request.key |> Converter.forward keyConverter
                    cancellationToken = request.cancellationToken
                }
                let! response' = service.getValue request'
                return {
                    value = response'.value |> Option.map (Converter.backward valueConverter)
                }
            }

            member this.putValue request = async {
                let request' = {
                    key = request.key |> Converter.forward keyConverter
                    value = request.value |> Converter.forward valueConverter
                    cancellationToken = request.cancellationToken
                }
                return! service.putValue request'
            }

            member this.deleteValue request = async {
                let request': DeleteValueReq<_> = {
                    key = request.key |> Converter.forward keyConverter
                    cancellationToken = request.cancellationToken
                }
                return! service.deleteValue request'
            }
        }
