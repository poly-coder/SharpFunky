namespace SharpFunky

type AsyncTrialFn<'a, 'b, 'e> = 'a -> AsyncTrial<'b, 'e>

module AsyncTrialFn =
    let inline internal applyFn (f: AsyncTrialFn<_, _, _>) fma: AsyncTrialFn<_, _, _> = fma >> f
    let inline internal konstFn f: AsyncTrialFn<_, _, _> = konst f

    let warns es a = konst (AsyncTrial.warns es a)
    let warnsSeq es a = konst (AsyncTrial.warnsSeq es a)
    let warn e a = konst (AsyncTrial.warn e a)
    let failures es = konst (AsyncTrial.failures es)
    let failuresSeq es = konst (AsyncTrial.failuresSeq es)
    let failure e = konst (AsyncTrial.failure e)
    let success a = konst (AsyncTrial.success a)

    let bind f = applyFn (AsyncTrial.bind f)
    let bindSync f = applyFn (AsyncTrial.bindSync f)
    let map f = applyFn (AsyncTrial.map f)
    let mapSync f = applyFn (AsyncTrial.mapSync f)
    let mapWarns f = applyFn (AsyncTrial.mapWarns f)
    let mapWarnsSync f = applyFn (AsyncTrial.mapWarnsSync f)
    let mapWarn f = applyFn (AsyncTrial.mapWarn f)
    let mapWarnSync f = applyFn (AsyncTrial.mapWarnSync f)
    let mapErrors f = applyFn (AsyncTrial.mapErrors f)
    let mapErrorsSync f = applyFn (AsyncTrial.mapErrorsSync f)
    let mapError f = applyFn (AsyncTrial.mapError f)
    let mapErrorSync f = applyFn (AsyncTrial.mapErrorSync f)
    let mapMessages f = applyFn (AsyncTrial.mapMessages f)
    let mapMessagesSync f = applyFn (AsyncTrial.mapMessagesSync f)
    let mapMessage f = applyFn (AsyncTrial.mapMessage f)

    let ofTrial ma = applyFn AsyncTrial.ofTrial ma
    let ofResult ma = applyFn AsyncTrial.ofResult ma
    let ofOption ma = applyFn AsyncTrial.ofOption ma
    let ofChoice ma = applyFn AsyncTrial.ofChoice ma

    let getOrInvalidOp ma = ma >> AsyncTrial.getOrInvalidOp

    let toSeq a = a >> AsyncTrial.toSeq
    let toMessages a = a >> AsyncTrial.toMessages
    let toWarns a = a >> AsyncTrial.toWarns
    let toErrors a = a >> AsyncTrial.toErrors
    let toList a = a >> AsyncTrial.toList
