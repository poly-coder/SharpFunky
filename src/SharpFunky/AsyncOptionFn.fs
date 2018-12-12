namespace SharpFunky

type AsyncOptionFn<'a, 'b> = 'a -> AsyncOption<'b>

module AsyncOptionFn =
    let some v = fun _ -> AsyncOption.some v
    let none = fun _ -> AsyncOption.none

    let inline internal applyFn (f: AsyncOptionFn<_, _>) fma: AsyncOptionFn<_, _> = fma >> f

    let matches f fe (mab: AsyncOptionFn<_, _>) = mab >> Async.bind (Option.matches f fe)
    let matchesSync f fe = matches (f >> Async.return') (fe >> Async.return')
    let matches' f fe (mab: AsyncOptionFn<_, _>) = mab |> applyFn (AsyncOption.matches f fe)
    
    let bind f = applyFn (AsyncOption.bind f)
    let map f = applyFn (AsyncOption.map f)

    let ofAsync ma = ma |> applyFn AsyncOption.ofAsync
    let ofTask ma = ma |> applyFn (Async.ofTask >> AsyncOption.ofAsync)
    let ofOption ma = ma |> applyFn AsyncOption.ofOption
    let ofFn ma = ma |> applyFn AsyncOption.some

    let bindAsync f = ofAsync >> bind f
    let mapAsync f = ofAsync >> map f
    let bindTask f = ofTask >> bind f
    let mapTask f = ofTask >> map f
    let bindOption f = ofOption >> bind f
    let mapOption f = ofOption >> map f

    let getOrFail ma = ma |> Monads.applyFn AsyncOption.get

    let chooseFirst handlers = fun a ->
        let rec loop hs = async {
            match hs with
            | head :: hs'->
                match! head a with
                | Some b -> return Some b
                | None -> return! loop hs'
            | [] -> return None
        }
        loop handlers

    let chooseFirstSeq handlers = fun a ->
        let e = (handlers: _ seq).GetEnumerator()
        let rec loop moved = async {
            match moved with
            | true->
                match! e.Current a with
                | Some b ->
                    e.Dispose()
                    return Some b
                | None -> return! loop (e.MoveNext())
            | false ->
                e.Dispose()
                return None
        }
        loop <| e.MoveNext()

    let compose handler1 handler2 = handler1 |> bind handler2
