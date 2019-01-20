namespace SharpFunky

type Converter<'a, 'b> = Converter of Fn<'a, 'b> * Fn<'b, 'a>

[<RequireQualifiedAccess>]
module Converter =
    let cons forward backward = Converter(forward, backward)

    let forward (Converter(forward, _)) = forward
    let backward (Converter(_, backward)) = backward

    let identity<'a> : Converter<'a, 'a> = cons id id

    let compose l1 l2 =
        cons (forward l1 >> forward l2) (backward l2 >> backward l1)

    let rev (Converter(f, b)) = cons b f

    module Infix =
        let inline (>=>) l1 l2 = compose l1 l2
