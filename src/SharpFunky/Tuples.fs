namespace SharpFunky

module Tup2 =
    let setFst value (_, b) = value, b
    let setSnd value (a, _) = a, value
    let withFst value b = value, b
    let withSnd value a = a, value

    let fstLens<'a, 'b> : Lens<'a * 'b, 'a> = Lens.cons' fst setFst
    let sndLens<'a, 'b> : Lens<'a * 'b, 'b> = Lens.cons' snd setSnd
