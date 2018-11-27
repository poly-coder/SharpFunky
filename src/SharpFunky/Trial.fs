namespace SharpFunky

type Trial<'a, 'e> = 
    | Success of 'a * 'e list
    | Failure of 'e list

module Trial =
    let warns es a = Success(a, es)
    let warnsSeq es a = warns (es |> List.ofSeq) a
    let warn e a = warns [e] a
    let failures es: Trial<_, _> = Failure es
    let failuresSeq es = failures (es |> List.ofSeq)
    let failure e = failures [e]
    let success a = warns [] a

    let matches fWarns fFailures (ma: Trial<_, _>) = 
        match ma with
        | Success (a, es) -> fWarns es a
        | Failure es -> fFailures es
    
    let matchesSuccess fSuccess = matches (fun _ -> fSuccess)
    
    let bind f = matches (fun _ -> f) failures
    let map f = bind (f >> success)
    let mapWarns f = matches (f >> warns) failures
    let mapWarn f = mapWarns (List.map f)
    let mapErrors f = matches warns (f >> failures)
    let mapError f = mapErrors (List.map f)
    let mapMessages f = matches (f >> warns) (f >> failures)
    let mapMessage f = mapMessages (List.map f)

    let warnsAsErrors ma =
        matches 
            (fun es a -> match es with [] -> success a | _ -> failures es)
            failures
            ma

    let catch fn x = try fn x |> success with exn -> failure exn

    let ofResult a = a |> Result.matches success failure
    let toResult a = a |> matchesSuccess Result.ok Result.error

    let ofOption a = a |> Option.matches success failure
    let toOption a = a |> matchesSuccess Some (konst None)

    let ofChoice = function Choice1Of2 a -> success a | Choice2Of2 e -> failure e
    let toChoice a = a |> matchesSuccess Choice1Of2 Choice2Of2

    let toSeq a = a |> matchesSuccess Seq.singleton (konst Seq.empty)
    let toMessages a = a |> matches (fun es _ -> es) id
    let toWarns a = a |> matches (fun es _ -> es) (konst [])
    let toErrors a = a |> matches (konst2 []) id
    let toList a = a |> matchesSuccess List.singleton (konst [])

    let mergeAndWith f ma mb =
        match ma, mb with
        | Success(a, es1), Success(b, es2) -> f a b |> warns (es1 @ es2)
        | Success _, Failure es -> failures es
        | Failure es, Success _ -> failures es
        | Failure es1, Failure es2 -> failures (es1 @ es2)

    let mergeAndFst ma mb = mergeAndWith fstArg ma mb
    let mergeAndSnd ma mb = mergeAndWith fstArg ma mb

    let mergeOrWith fBoth fFst fSnd ma mb =
        match ma, mb with
        | Success(a, es1), Success(b, es2) -> fBoth a b |> warns (es1 @ es2)
        | Success(a, es), Failure _ -> fFst a |> warns es
        | Failure _, Success(b, es) -> fSnd b |> warns es
        | Failure es1, Failure es2 -> failures (es1 @ es2)

    let mergeOrFst ma mb = mergeOrWith fstArg id id ma mb
    let mergeOrSnd ma mb = mergeOrWith fstArg id id ma mb

    let inline internal reduceWith init merger ms = (init, ms) ||> Seq.fold merger
    let reduceAndFst unit ms = ms |> reduceWith (success unit) mergeAndFst
    let reduceAndLast unit ms = ms |> reduceWith (success unit) mergeAndSnd
    let reduceOrFst ms = ms |> reduceWith (success()) mergeOrFst
    let reduceOrLast ms = ms |> reduceWith (success()) mergeOrSnd

    module TrialAndBuilder =
        let zero<'e> : Trial<_, 'e> = success()
        let inline return' a = success a
        let inline returnFrom ma = ma
        let inline delay f = Monads.delay f
        let inline run f = Monads.run f
        let inline tryWith handler ma = Monads.tryWith returnFrom handler ma
        let inline tryFinally compensation ma = Monads.tryFinally returnFrom compensation ma
        let inline using body res = Monads.using tryFinally body res
        let inline while' body guard = Monads.while' zero body guard
        let inline for' body sequence = Monads.for' using while' delay body sequence

        type Builder() =
            member __.Delay f = delay f
            member __.Run f = run f

            member __.Zero() = zero
            member __.Return(a) = return' a
            member __.ReturnFrom ma = returnFrom ma
            member __.Bind(ma, f) = bind f ma

            member __.Combine(mu, mb) = mergeAndSnd mu mb

            member __.TryWith(ma, handler) = tryWith handler ma

            member __.TryFinally(ma, compensation) = tryFinally compensation ma

            member __.Using(res, body) = using body res

            member __.While(guard, body) = while' body guard

            member __.For(sequence, body) = for' body sequence

    let trial = TrialAndBuilder.Builder()

    module TrialOrBuilder =
        let zero<'e> : Trial<_, 'e> = success()
        let inline return' a = success a
        let inline returnFrom ma = ma
        let inline delay f = Monads.delay f
        let inline run f = Monads.run f
        let inline tryWith handler ma = Monads.tryWith returnFrom handler ma
        let inline tryFinally compensation ma = Monads.tryFinally returnFrom compensation ma
        let inline using body res = Monads.using tryFinally body res
        let inline while' body guard = Monads.while' zero body guard
        let inline for' body sequence = Monads.for' using while' delay body sequence

        type Builder() =
            member __.Delay f = delay f
            member __.Run f = run f

            member __.Zero() = zero
            member __.Return(a) = return' a
            member __.ReturnFrom ma = returnFrom ma
            member __.Bind(ma, f) = bind f ma

            member __.Combine(mu, mb) = mergeOrSnd mu mb

            member __.TryWith(ma, handler) = tryWith handler ma

            member __.TryFinally(ma, compensation) = tryFinally compensation ma

            member __.Using(res, body) = using body res

            member __.While(guard, body) = while' body guard

            member __.For(sequence, body) = for' body sequence

    let trialOr = TrialOrBuilder.Builder()

