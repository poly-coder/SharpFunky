namespace SharpFunky
//namespace SharpFunky.Storage

//open SharpFunky

//type IKeyValueGetter<'k, 't> =
//    abstract get: 'k -> Async<'t option>

//type IKeyValuePutter<'k, 't> =
//    abstract put: 'k -> 't -> Async<unit>
//    abstract del: 'k -> Async<unit>

//type IKeyValueStore<'k, 't> =
//    inherit IKeyValueGetter<'k, 't>
//    inherit IKeyValuePutter<'k, 't>

//module KeyValueStore =
//    open System

//    let createInstance get put del =
//        { new IKeyValueStore<'k, 't> with
//            member __.get key = get key
//            member __.put key value = put key value
//            member __.del key = del key
//        }

//    let empty () =
//        createInstance
//            (fun _ -> Async.return' None)
//            (fun _ _ -> async { return raise (NotSupportedException("")) })
//            (fun _ -> Async.return' ())

//    module InMemory =
//        type Options<'k, 't when 'k: comparison> = {
//            initMap: Map<'k, 't>
//            updateValue: 'k -> 't -> 't
//        }

//        [<RequireQualifiedAccess>]
//        module Options =
//            let empty<'k, 't when 'k: comparison> : Options<'k, 't> = 
//                {
//                    initMap = Map.empty
//                    updateValue = fun _ v -> v
//                }
//            let withInitMap value = fun opts -> { opts with initMap = value }
//            let withUpdateValue value = fun opts -> { opts with updateValue = value }

//        type internal Command<'k, 't> =
//        | GetCmd of 'k * AsyncReplyChannel<'t option>
//        | PutCmd of 'k * 't * AsyncReplyChannel<unit>
//        | DelCmd of 'k * AsyncReplyChannel<unit>

//        let create opts =
//            let updateValue = opts.updateValue
//            let mailbox = MailboxProcessor.Start(fun mb -> 
//                let rec loop map = async {
//                    let! msg = mb.Receive()
//                    match msg with
//                    | GetCmd (key, rep) ->
//                        let found = map |> Map.tryFind key
//                        do rep.Reply <| found
//                        return! loop map

//                    | PutCmd (key, value, rep) ->
//                        let value' = updateValue key value
//                        let map' = map |> Map.add key value'
//                        do rep.Reply ()
//                        return! loop map'

//                    | DelCmd (key, rep) ->
//                        let map' = map |> Map.remove key
//                        do rep.Reply ()
//                        return! loop map'
//                }
//                loop opts.initMap
//            )
//            let get key =
//                mailbox.PostAndAsyncReply(fun r -> GetCmd(key, r))
//            let put key value =
//                mailbox.PostAndAsyncReply(fun r -> PutCmd(key, value, r))
//            let del key =
//                mailbox.PostAndAsyncReply(fun r -> DelCmd(key, r))
//            createInstance get put del

//        let from map = Options.empty |> Options.withInitMap map |> create
        
//        let empty() = from Map.empty

//    module Validated =
//        type Options<'k, 't when 'k: comparison> = {
//            validateKey: 'k -> Async<unit>
//            validateValue: 't -> Async<unit>
//            store: IKeyValueStore<'k, 't>
//        }

//        [<RequireQualifiedAccess>]
//        module Options =
//            let fromStore store = 
//                {
//                    store = store
//                    validateKey = fun _ -> Async.return' ()
//                    validateValue = fun _ -> Async.return' ()
//                }
//            let withValidateKey value = fun opts -> { opts with validateKey = value }
//            let withValidateValue value = fun opts -> { opts with validateValue = value }
        
//        let create opts =
//            let get key = async {
//                do! opts.validateKey key
//                return! opts.store.get key
//            }
//            let put key value = async {
//                do! opts.validateKey key
//                do! opts.validateValue value
//                return! opts.store.put key value
//            }
//            let del key = async {
//                do! opts.validateKey key
//                return! opts.store.del key
//            }
//            createInstance get put del

//    module ConvertedValue =
//        open SharpFunky.Conversion

//        type Options<'k, 'a, 'b when 'k: comparison> = {
//            converter: IAsyncReversibleConverter<'a, 'b>
//            store: IKeyValueStore<'k, 'b>
//        }

//        [<RequireQualifiedAccess>]
//        module Options =
//            let fromStore store converter = 
//                {
//                    store = store
//                    converter = converter
//                }
        
//        let create opts =
//            let get key = async {
//                let! b = opts.store.get key
//                match b with
//                | Some b ->
//                    let! a = opts.converter.convertBack b
//                    return Some a
//                | None ->
//                    return None
//            }
//            let put key value = async {
//                let! b = opts.converter.convert value
//                return! opts.store.put key b
//            }
//            let del key = async {
//                return! opts.store.del key
//            }
//            createInstance get put del

//namespace SharpFunky.Storage
//open SharpFunky
//open System.Threading

//module KeyValue =

//    type KeyValueCommand<'k, 't> =
//        | GetValue of key: 'k * cToken: CancellationToken * replyCh: Sink<Result<'t option, exn>>
//        | PutValue of key: 'k * value: 't * cToken: CancellationToken * replyCh: Sink<Result<unit, exn>>
//        | DelValue of key: 'k * cToken: CancellationToken * replyCh: Sink<Result<unit, exn>>

//    type KeyValueStore<'k, 't> = Sink<KeyValueCommand<'k, 't>>

//    module InMemory =
//        type Options<'k, 't when 'k: comparison> = {
//            initMap: Map<'k, 't>
//            updateValueKey: LensSetter<'t, 'k>
//        }

//        [<RequireQualifiedAccess>]
//        module Options =
//            let empty<'k, 't when 'k: comparison> : Options<'k, 't> = 
//                {
//                    initMap = Map.empty
//                    updateValueKey = fun _ v -> v
//                }
//            let initMap<'k, 't when 'k: comparison> : Lens<_, Map<'k, 't>> =
//                Lens.cons' (fun o -> o.initMap) (fun v o -> { o with initMap = v })
//            let updateValueKey<'k, 't when 'k: comparison> : Lens<_, LensSetter<'t, 'k>> =
//                Lens.cons' (fun o -> o.updateValueKey) (fun v o -> { o with updateValueKey = v })

//        let create opts: KeyValueStore<_, _> =
//            let initMap = opts |> Lens.get Options.initMap
//            let updateValueKey = opts |> Lens.get Options.updateValueKey

//            let mapState repl fn map =
//                try
//                    let map', result = fn map
//                    do Result.ok result |> repl
//                    map'
//                with
//                | exn ->
//                    try Error exn |> repl with _ -> ()
//                    map

//            let fromState repl fn =
//                mapState repl (fun map -> map, fn map)

//            let getValue repl key = fromState repl (Map.tryFind key)

//            let putValue repl key value = mapState repl (fun map ->
//                let value' = updateValueKey key value
//                let map' = map |> Map.add key value'
//                map', ()
//            )

//            let delValue repl key = mapState repl (fun map ->
//                let map' = map |> Map.remove key
//                map', ()
//            )
            
//            let mailbox = MailboxProcessor.Start(fun mb ->
//                let rec loop map = async {
//                    let! cmd = MBox.receiveFrom mb
//                    let transform =
//                        match cmd with
//                        | GetValue(key, _, repl) ->
//                            getValue repl key
//                        | PutValue(key, value, _, repl) ->
//                            putValue repl key value
//                        | DelValue(key, _, repl) ->
//                            delValue repl key
//                    return! map |> transform |> loop
//                }
//                loop initMap
//            )

//            fun cmd -> mailbox |> MBox.post cmd

//    module Validated =
//        type Options<'k, 't> = {
//            validateKey: AsyncFn<'k, unit>
//            validateValue: AsyncFn<'t, unit>
//            store: KeyValueStore<'k, 't>
//        }

//        [<RequireQualifiedAccess>]
//        module Options =
//            let create store : Options<'k, 't> = 
//                {
//                    store = store
//                    validateKey = AsyncFn.return' ()
//                    validateValue = AsyncFn.return' ()
//                }
//            let store<'k, 't> : Lens<_, KeyValueStore<'k, 't>> =
//                Lens.cons' (fun o -> o.store) (fun v o -> { o with store = v })
//            let validateKey<'k, 't> : Lens<_, AsyncFn<'k, unit>> =
//                Lens.cons' (fun o -> o.validateKey) (fun v o -> { o with validateKey = v })
//            let validateValue<'k, 't> : Lens<_, AsyncFn<'t, unit>> =
//                Lens.cons' (fun o -> o.validateValue) (fun v o -> { o with validateValue = v })

//        let create opts: KeyValueStore<_, _> =
//            let store = opts |> Lens.get Options.store
//            let validateKey = opts |> Lens.get Options.validateKey
//            let validateValue = opts |> Lens.get Options.validateValue

//            fun cmd ->
//                let runCmd repl fn =
//                    async {
//                        try
//                            do! fn()
//                            store cmd
//                        with exn ->
//                            try Error exn |> repl with _ -> ()
//                    } |> Async.start

//                match cmd with
//                | GetValue(key, _, repl) ->
//                    runCmd repl (fun() -> validateKey key)
//                | PutValue(key, value, _, repl) ->
//                    runCmd repl (fun() -> async {
//                        do! validateKey key
//                        do! validateValue value
//                    })
//                | DelValue(key, _, repl) ->
//                    runCmd repl (fun() -> validateKey key)


//namespace SharpFunky.Storage.CSharp.KeyValue

//open System.Threading
//open System.Threading.Tasks
//open FSharp.Control.Tasks.V2
//open SharpFunky.Storage

//type GetValue<'k>(key: 'k) =
//    member val Key = key with get

//type GetValueResult<'t>(found: bool, value: 't) =
//    new(value) = GetValueResult(true, value)
//    new() = GetValueResult(false, Unchecked.defaultof<'t>)
//    member val Found = found with get
//    member val Value = value with get

//type PutValue<'k, 't>(key: 'k, value: 't) =
//    member val Key = key with get
//    member val Value = value with get

//type PutValueResult(success: bool) =
//    new() = PutValueResult(true)
//    member val Success = success with get

//type RemoveValue<'k>(key: 'k) =
//    member val Key = key with get

//type RemoveValueResult(success: bool) =
//    new() = RemoveValueResult(true)
//    member val Success = success with get

//type IKeyValueStore<'k, 't> =
//    abstract GetValueAsync: GetValue<'k> * CancellationToken -> Task<GetValueResult<'t>>
//    abstract PutValueAsync: PutValue<'k, 't> * CancellationToken -> Task<PutValueResult>
//    abstract RemoveValueAsync: RemoveValue<'k> * CancellationToken -> Task<RemoveValueResult>

//[<AbstractClass; Sealed>]
//type KeyValueStore() =
//    static member FromFSharp<'k, 't> (store: KeyValue.KeyValueStore<'k, 't>) =
//        let makeTask cToken =
//            let ts = TaskCompletionSource(cToken: CancellationToken)
//            let fn = function
//            | Ok v -> ts.TrySetResult v |> ignore
//            | Error exn -> ts.TrySetException(exn: exn) |> ignore
//            ts.Task, fn

//        { new IKeyValueStore<'k, 't> with
//            member __.GetValueAsync(req, cToken) = task {
//                let result, sink = makeTask cToken
//                let cmd = KeyValue.GetValue(req.Key, cToken, sink)
//                do store cmd
//                match! result with
//                | Some value ->
//                    return GetValueResult<'t>(true, value)
//                | None ->
//                    return GetValueResult<'t>()
//            }

//            member __.PutValueAsync(req, cToken) = task {
//                let result, sink = makeTask cToken
//                let cmd = KeyValue.PutValue(req.Key, req.Value, cToken, sink)
//                do store cmd
//                do! result
//                return PutValueResult(true)
//            }

//            member __.RemoveValueAsync(req, cToken) = task {
//                let result, sink = makeTask cToken
//                let cmd = KeyValue.DelValue(req.Key, cToken, sink)
//                do store cmd
//                do! result
//                return RemoveValueResult(true)
//            }
//        }
