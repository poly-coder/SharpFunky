namespace SharpFunky.EventServer.Grains

module Say =
    let hello name =
        printfn "Hello %s" name
