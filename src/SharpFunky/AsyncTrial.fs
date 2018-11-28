namespace SharpFunky

type AsyncTrial<'t, 'e> = Async<Trial<'t, 'e>>

module AsyncTrial =
    open System.Threading.Tasks
    
    let warns es a = Trial.warns es a |> async.Return
    let warnsSeq es a = Trial.warnsSeq es a |> async.Return
    let warn e a = Trial.warn e a |> async.Return
    let failures es = Trial.failures es |> async.Return
    let failuresSeq es = Trial.failuresSeq es |> async.Return
    let failure e = Trial.failure e |> async.Return
    let success a = Trial.success a |> async.Return

    let matches fWarns fFailures (ma: AsyncTrial<_, _>) =
        Async.bind (Trial.matches fWarns fFailures) ma
    let matchesSync fWarns fFailures =
        matches (fun es a -> fWarns es a |> Async.return') (fFailures |> AsyncFn.ofFn)
    let inline internal matchesAsTrial fWarns fFailures ma : AsyncTrial<_, _> =
        matches fWarns fFailures ma

    let matchesSuccess fWarns fFailures (ma: AsyncTrial<_, _>) =
        Async.bind (Trial.matchesSuccess fWarns fFailures) ma
    let matchesSuccessSync fWarns fFailures =
        matchesSuccess (fWarns |> AsyncFn.ofFn) (fFailures |> AsyncFn.ofFn)
    let matchesSuccessAsTrial fWarns fFailures ma : AsyncTrial<_, _> =
        matchesSuccess fWarns fFailures ma

    let bind f = matchesSuccessAsTrial f (failures)
    let bindSync f = f |> AsyncFn.ofFn |> bind
    let map f = f |> AsyncFn.bind success |> bind
    let mapSync f = f |> AsyncFn.ofFn |> map
    let mapWarns f = matchesAsTrial (fun es a -> f es |> Async.bind (fun es' -> warns es' a)) failures
    let mapWarnsSync f = f |> AsyncFn.ofFn |> mapWarns
    let mapWarn f = mapWarns (Seq.map f >> Async.ofAsyncSeq >> Async.map Seq.toList)
    let mapWarnSync f = f |> AsyncFn.ofFn |> mapWarn
    let mapErrors f = matchesAsTrial (fun es a -> warns es a) (f |> AsyncFn.bind failures)
    let mapErrorsSync f = f |> AsyncFn.ofFn |> mapErrors
    let mapError f = mapErrors (Seq.map f >> Async.ofAsyncSeq >> Async.map Seq.toList)
    let mapErrorSync f = f |> AsyncFn.ofFn |> mapError
    let mapMessages f = matchesAsTrial (fun es a -> f es |> Async.bind (fun es' -> warns es' a)) (f |> AsyncFn.bind failures)
    let mapMessagesSync f = f |> AsyncFn.ofFn |> mapMessages
    let mapMessage f = mapMessages (Seq.map f >> Async.ofAsyncSeq >> Async.map Seq.toList)
    let mapMessageSync f = f |> AsyncFn.ofFn |> mapMessage

    let catch fn x: AsyncTrial<_, _> = async {
        try
            let! a = fn x
            return! success a
        with exn ->
            return! failure exn
    }

    let ofTrial ma: AsyncTrial<_, _> = ma |> Async.return'
    
    let ofResult ma = ma |> Async.map Trial.ofResult
    let ofOption ma = ma |> Async.map Trial.ofOption
    let ofChoice ma = ma |> Async.map Trial.ofChoice

    let getOrInvalidOp ma = matchesSuccess Async.return' (fun _ -> invalidOp "Unexpected errors") ma

    let toSeq a = a |> matchesSuccessSync Seq.singleton (konst Seq.empty)
    let toMessages a = a |> matchesSync fstArg id
    let toWarns a = a |> matchesSync fstArg (konst [])
    let toErrors a = a |> matchesSync (konst2 []) id
    let toList a = a |> matchesSuccessSync List.singleton (konst [])

    let mergeAndWith f (ma: AsyncTrial<_, _>) (mb: AsyncTrial<_, _>): AsyncTrial<_, _> = async {
        match! ma with
        | Success(a, es1) ->
            match! mb with
            | Success(b, es2) ->
                let! c = f a b
                return Trial.warns (es1 @ es2) c
            | Failure es -> return Trial.failures es
        | Failure es -> return Trial.failures es
    }

    let mergeAndFst ma mb = mergeAndWith (fun a _ -> Async.return' a) ma mb
    let mergeAndSnd ma mb = mergeAndWith (fun _ b -> Async.return' b) ma mb

    let mergeOrWith fBoth fFst fSnd (ma: AsyncTrial<_, _>) (mb: AsyncTrial<_, _>): AsyncTrial<_, _> = async {
        match! ma with
        | Success(a, es1) ->
            match! mb with
            | Success(b, es2) ->
                let! c = fBoth a b
                return Trial.warns (es1 @ es2) c
            | Failure _ ->
                let! c = fFst a
                return Trial.warns es1 c
        | Failure es1 ->
            match! mb with
            | Success(b, es2) ->
                let! c = fSnd b
                return Trial.warns es2 c
            | Failure es2 ->
                return Trial.failures (es1 @ es2)
    }

    let mergeOrFst ma mb = mergeOrWith (fun a _ -> Async.return' a) Async.return' Async.return' ma mb
    let mergeOrSnd ma mb = mergeOrWith (fun _ b -> Async.return' b) Async.return' Async.return' ma mb

    module TrialAndBuilder =
        let zero<'e> : AsyncTrial<_, 'e> = success()
        let inline delay f = Monads.delay f
        let inline run f = Monads.run f
        let inline return' a = success a
        let inline returnFrom ma = ma
        let inline tryWith handler ma = Monads.tryWith returnFrom handler ma
        let inline tryFinally compensation ma = Monads.tryFinally returnFrom compensation ma
        let inline using body res = Monads.using tryFinally body res
        let inline while' body guard = Monads.while' zero body guard
        let inline for' body sequence = Monads.for' using while' delay body sequence

        type Builder() =
            member this.Delay f = delay f
            member this.Run f = run f
            member this.Zero() = zero
            member this.Return(a) = return' a
            member this.ReturnFrom ma = returnFrom ma
            member this.Bind(ma, f) = bind f ma
            member this.Combine(mu, mb) = mergeAndSnd mb mu
            member this.TryWith(ma, handler) = tryWith handler ma
            member this.TryFinally(ma, compensation) = tryFinally compensation ma
            member this.Using(res, body) = using body res
            member this.While(guard, body) = while' body guard
            member this.For(sequence, body) = for' body sequence

    let asyncTrial = TrialAndBuilder.Builder()

    module TrialOrBuilder =
        let zero<'a> : AsyncTrial<'a, _> = failure()
        let inline delay f = Monads.delay f
        let inline run f = Monads.run f
        let inline return' a = success a
        let inline returnFrom ma = ma
        let inline tryWith handler ma = Monads.tryWith returnFrom handler ma
        let inline tryFinally compensation ma = Monads.tryFinally returnFrom compensation ma
        let inline using body res = Monads.using tryFinally body res
        let inline while' body guard = Monads.while' zero body guard
        let inline for' body sequence = Monads.for' using while' delay body sequence

        type Builder() =
            member this.Delay f = delay f
            member this.Run f = run f
            member this.Zero() = zero
            member this.Return(a) = return' a
            member this.ReturnFrom ma = returnFrom ma
            member this.Bind(ma, f) = bind f ma
            member this.Combine(mu, mb) = mergeOrSnd mb mu
            member this.TryWith(ma, handler) = tryWith handler ma
            member this.TryFinally(ma, compensation) = tryFinally compensation ma
            member this.Using(res, body) = using body res
            member this.While(guard, body) = while' body guard
            member this.For(sequence, body) = for' body sequence

    let asyncTrialOr = TrialOrBuilder.Builder()
