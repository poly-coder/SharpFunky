module SharpFunky.Task

//open System
//open System.Threading
//open System.Threading.Tasks

//let none = CancellationToken.None
//let return' a = Task.FromResult a
//let yield' () = Task.Yield()
//let completed = Task.CompletedTask
//let fromExn exn = Task.FromException<_> exn
//let fromExnVoid exn = Task.FromException exn
//let fromCancelled tok = Task.FromCanceled<_> tok
//let fromCancelledVoid tok = Task.FromCanceled tok

//// TODO: Implement using Tasks directly instead of passing through Async
//let bind f (ma: Task<'a>) = Async.ofTask ma |> Async.bind (f >> Async.ofTask) |> Async.toTask
//let map f = bind (f >> return')

//let sleep (ts: TimeSpan) = Task.Delay(ts)
//let sleepMs (ms: int) = Task.Delay(ms)

//let ofAction (action: unit -> unit) = Task.Run(action)
//let ofFuncVoid (func: unit -> Task) = Task.Run(func)
//let ofFunc (func: unit -> 'a) = Task.Run<'a>(func)
//let ofFuncTask (func: unit -> Task<'a>) = Task.Run<'a>(func)

//let waitAll (tasks: Task array) = Task.WaitAll(tasks)
//let waitAllTimed (timeout: TimeSpan) (tasks: Task array) = Task.WaitAll(tasks, timeout)
//let waitAllTimedMs (timeoutMs: int) (tasks: Task array) = Task.WaitAll(tasks, timeoutMs)

//let waitAny (tasks: Task array) = Task.WaitAny(tasks)
//let waitAnyTimed (timeout: TimeSpan) (tasks: Task array) = Task.WaitAny(tasks, timeout)
//let waitAnyTimedMs (timeoutMs: int) (tasks: Task array) = Task.WaitAny(tasks, timeoutMs)

//let whenAll (tasks: Task<'a> seq) = Task.WhenAll(tasks)
//let whenAllVoid (tasks: Task seq) = Task.WhenAll(tasks)

//let whenAny (tasks: Task<'a> seq) = Task.WhenAny(tasks)
//let whenAnyVoid (tasks: Task seq) = Task.WhenAny(tasks)

//let zero = return' ()
//let zeroVoid = completed
//let delay f = ofFuncTask f
//let run f = f
//let raise exn = delay <| fun () -> raise exn
//let failwith msg = delay <| fun () -> failwith msg
//let invalidArg name msg = delay <| fun () -> invalidArg name msg
//let nullArg msg = delay <| fun () -> nullArg msg
//let invalidOp msg = delay <| fun () -> invalidOp msg
//let notImpl() = delay notImpl

//module Cancellable =
//    let sleep token (ts: TimeSpan) = Task.Delay(ts, token)
//    let sleepMs token (ms: int) = Task.Delay(ms, token)

//    let ofAction token (action: unit -> unit) = Task.Run(action, token)
//    let ofFuncVoid token (func: unit -> Task) = Task.Run(func, token)
//    let ofFunc token (func: unit -> 'a) = Task.Run<'a>(func, token)
//    let ofFuncTask token (func: unit -> Task<'a>) = Task.Run<'a>(func, token)

//    let waitAll token (tasks: Task array) = Task.WaitAll(tasks, (token: CancellationToken))
//    let waitAllTimedMs token (timeoutMs: int) (tasks: Task array) = Task.WaitAll(tasks, timeoutMs, (token: CancellationToken))

//    let waitAny token (tasks: Task array) = Task.WaitAny(tasks, (token: CancellationToken))
//    let waitAnyTimedMs token (timeoutMs: int) (tasks: Task array) = Task.WaitAny(tasks, timeoutMs, (token: CancellationToken))

//module Builder =
//    let zero = return' ()
//    let inline returnFrom ma = ma
//    let inline run f = Monads.run f
        
//    //let inline tryFinally compensation ma = Monads.tryFinally returnFrom compensation ma
//    //let inline using body res = Monads.using tryFinally body res
//    //let inline while' body guard = Monads.while' zero body guard
//    //let inline for' body (sequence: _ seq) =
//    //    sequence.GetEnumerator() |> using (fun enum ->
//    //        (fun () -> enum.MoveNext()) |> while' (delay (fun () -> 
//    //            body enum.Current)))

//    type ResultBuilder() =
//        member this.Delay f = delay f
//        member this.Run f = run f

//        member this.Zero() = zero
//        member this.Return(a) = return' a
//        member this.ReturnFrom ma = returnFrom ma
//        member this.Bind(ma, f) = bind f ma

//        member this.Combine(mu, mb) = bind mb mu

//        member this.TryWith(ma, handler) = tryWith handler ma

//        member this.TryFinally(ma, compensation) = tryFinally compensation ma

//        member this.Using(res, body) = using body res

//        member this.While(guard, body) = while' body guard

//        member this.For(sequence, body) = for' body sequence

//let result = Builder.ResultBuilder()
