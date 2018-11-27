namespace SharpFunky

type TrialFn<'a, 'b, 'e> = 'a -> Trial<'b, 'e>

module TrialFn =
    let warns es a = fun _ -> Trial.warns es a
    let warnsSeq es a = fun _ -> Trial.warnsSeq es a
    let warn e = fun _ -> Trial.warn e
    let failures es = fun _ -> Trial.failures es
    let failuresSeq es = fun _ -> Trial.failuresSeq es
    let failure e = fun _ -> Trial.failure e
    let success v = fun _ -> Trial.success v

    let inline internal applyFn (f: TrialFn<_, _, _>) fma: TrialFn<_, _, _> = fma >> f
    let inline internal asTrialFn f fma : TrialFn<_, _, _> = fma >> f

    let matches fWarns fFailures = applyFn (Trial.matches fWarns fFailures)
    let matchesSuccess fSuccess fFailures = applyFn (Trial.matchesSuccess fSuccess fFailures)

    let bind f = applyFn (Trial.bind f)
    let map f = applyFn (Trial.map f)
    let mapWarns f = applyFn (Trial.mapWarns f)
    let mapWarn f = applyFn (Trial.mapWarn f)
    let mapErrors f = applyFn (Trial.mapErrors f)
    let mapError f = applyFn (Trial.mapError f)
    let mapMessages f = applyFn (Trial.mapMessages f)
    let mapMessage f = applyFn (Trial.mapMessage f)

    let catch fn x = Trial.catch fn x

    let ofResult fn = asTrialFn Trial.ofResult fn
    let ofOption fn = asTrialFn Trial.ofOption fn
    let ofChoice fn = asTrialFn Trial.ofChoice fn

    let toSeq fn = fn >> Trial.toSeq
    let toMessages fn = fn >> Trial.toMessages
    let toWarns fn = fn >> Trial.toWarns
    let toErrors fn = fn >> Trial.toErrors
    let toList fn = fn >> Trial.toList
