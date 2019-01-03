namespace SharpFunky.Storage

open SharpFunky

type IKeyValueGetter<'k, 't> =
    abstract get: 'k -> Async<'t option>

type IKeyValuePutter<'k, 't> =
    abstract put: 'k -> 't -> Async<unit>
    abstract del: 'k -> Async<unit>

type IKeyValueStore<'k, 't> =
    inherit IKeyValueGetter<'k, 't>
    inherit IKeyValuePutter<'k, 't>

module KeyValueStore =
    open System

    let createInstance get put del =
        { new IKeyValueStore<'k, 't> with
            member __.get key = get key
            member __.put key value = put key value
            member __.del key = del key
        }

    let empty () =
        createInstance
            (fun _ -> Async.return' None)
            (fun _ _ -> async { return raise (NotSupportedException("")) })
            (fun _ -> Async.return' ())

    module InMemory =
        type Options<'k, 't when 'k: comparison> = {
            initMap: Map<'k, 't>
            updateValue: 'k -> 't -> 't
        }

        [<RequireQualifiedAccess>]
        module Options =
            let empty<'k, 't when 'k: comparison> : Options<'k, 't> = 
                {
                    initMap = Map.empty
                    updateValue = fun _ v -> v
                }
            let withInitMap value = fun opts -> { opts with initMap = value }
            let withUpdateValue value = fun opts -> { opts with updateValue = value }

        type internal Command<'k, 't> =
        | GetCmd of 'k * AsyncReplyChannel<'t option>
        | PutCmd of 'k * 't * AsyncReplyChannel<unit>
        | DelCmd of 'k * AsyncReplyChannel<unit>

        let create opts =
            let updateValue = opts.updateValue
            let mailbox = MailboxProcessor.Start(fun mb -> 
                let rec loop map = async {
                    let! msg = mb.Receive()
                    match msg with
                    | GetCmd (key, rep) ->
                        let found = map |> Map.tryFind key
                        do rep.Reply <| found
                        return! loop map

                    | PutCmd (key, value, rep) ->
                        let value' = updateValue key value
                        let map' = map |> Map.add key value'
                        do rep.Reply ()
                        return! loop map'

                    | DelCmd (key, rep) ->
                        let map' = map |> Map.remove key
                        do rep.Reply ()
                        return! loop map'
                }
                loop opts.initMap
            )
            let get key =
                mailbox.PostAndAsyncReply(fun r -> GetCmd(key, r))
            let put key value =
                mailbox.PostAndAsyncReply(fun r -> PutCmd(key, value, r))
            let del key =
                mailbox.PostAndAsyncReply(fun r -> DelCmd(key, r))
            createInstance get put del

        let from map = Options.empty |> Options.withInitMap map |> create
        
        let empty() = from Map.empty

    module Validated =
        type Options<'k, 't when 'k: comparison> = {
            validateKey: 'k -> Async<unit>
            validateValue: 't -> Async<unit>
            store: IKeyValueStore<'k, 't>
        }

        [<RequireQualifiedAccess>]
        module Options =
            let fromStore store = 
                {
                    store = store
                    validateKey = fun _ -> Async.return' ()
                    validateValue = fun _ -> Async.return' ()
                }
            let withValidateKey value = fun opts -> { opts with validateKey = value }
            let withValidateValue value = fun opts -> { opts with validateValue = value }
        
        let create opts =
            let get key = async {
                do! opts.validateKey key
                return! opts.store.get key
            }
            let put key value = async {
                do! opts.validateKey key
                do! opts.validateValue value
                return! opts.store.put key value
            }
            let del key = async {
                do! opts.validateKey key
                return! opts.store.del key
            }
            createInstance get put del

    module ConvertedValue =
        open SharpFunky.Conversion

        type Options<'k, 'a, 'b when 'k: comparison> = {
            converter: IAsyncReversibleConverter<'a, 'b>
            store: IKeyValueStore<'k, 'b>
        }

        [<RequireQualifiedAccess>]
        module Options =
            let fromStore store converter = 
                {
                    store = store
                    converter = converter
                }
        
        let create opts =
            let get key = async {
                let! b = opts.store.get key
                match b with
                | Some b ->
                    let! a = opts.converter.convertBack b
                    return Some a
                | None ->
                    return None
            }
            let put key value = async {
                let! b = opts.converter.convert value
                return! opts.store.put key b
            }
            let del key = async {
                return! opts.store.del key
            }
            createInstance get put del
