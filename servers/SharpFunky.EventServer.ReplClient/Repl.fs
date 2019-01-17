module SharpFunky.Repl

open System
open System.Linq
open System.Threading.Tasks
open System.Text.RegularExpressions
open FSharp.Control.Tasks.V2

type CommandResult =
    | PushContext of string * CommandRun
    | PopContext
    | ReplaceContext of CommandRun
    | KeepContext
    | UnknownCommand of helpMessage: string

and CommandRun = CommandRunContext -> Task<CommandResult>

and CommandRunContext = {
    services: IServiceProvider
    command: string
    parameters: string list
}

type ContextRuntime = {
    prompt: string
    run: CommandRun
}

let splitCommandLine commandLine =
    let m = Regex.Match(commandLine, """^\s*((("(?<token>[^"]+)")|('(?<token>[^']+)')|(?<token>[^\s'"]+))\s*)*$""")
    if m.Success then
        let captures = m.Groups.["token"].Captures.Cast<Capture>()
        captures |> Seq.map (fun c -> c.Value) |> Seq.toList
    else []

let runCommandRepl services rootRun =
    let rec loop stack = task {
        match stack with
        | current :: stack' ->
            Console.Write(current.prompt)
            Console.Write(" > ")
            match Console.ReadLine() |> splitCommandLine with
            | [] -> return! loop stack

            | command :: parameters ->
                try
                    let context = {
                        services = services
                        command = command
                        parameters = parameters
                    }
                    match! current.run context with
                    | PushContext (newPrompt, newRun) ->
                        let newContext = {
                            prompt = sprintf "%s/%s" current.prompt newPrompt
                            run = newRun
                        }
                        let stack'' = newContext :: stack
                        return! loop stack''
                    | PopContext ->
                        return! loop stack'
                    | KeepContext ->
                        return! loop stack
                    | ReplaceContext run' ->
                        let next = { current with run = run' }
                        return! loop (next :: stack')
                    | UnknownCommand helpMessage ->
                        match command with
                        | "exit" | "x" ->
                            return! loop stack'
                        | "quit" | "q" ->
                            return! loop []
                        | _ ->
                            printfn "Unknown command: %s" helpMessage
                        return! loop stack
                with exn ->
                    printfn "Error executing command:\n%O" exn
                    return! loop stack

        | [] ->
            return ()
    }
    loop [{ run = rootRun; prompt = "" }]

let subCommands commandMap =
    let commandMap = commandMap |> Map.ofSeq
    fun context -> task {
        match commandMap |> Map.tryFind context.command with
        | Some sub -> return! sub context
        | None ->
            let keys = commandMap |> Map.toSeq |> Seq.map fst |> String.joinWith ", "
            return UnknownCommand (sprintf "try the following commands: %s" keys)
    }

//let defineCommand parametersFormat matchParameters runCommand (ctx: CommandRunContext) = task {
//    match matchParameters ctx.parameters with
//    | Some parameters ->
//        return! runCommand parameters
//    | None ->
//        printfn "Invalid parameters. Try %s %s" ctx.command parametersFormat
//}

