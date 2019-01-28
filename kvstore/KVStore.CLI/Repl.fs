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
                        if command = "q" || command = "quit" then
                            return()
                        elif command = "x" || command = "exit" then
                            return! loop stack'
                        else
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


let (|OneParameter|_|) parameters =
    match parameters with
    | [value] -> Some value
    | _ -> None

let (|OneParsedParameter|_|) tryParse parameters =
    match parameters with
    | [value] -> tryParse value
    | _ -> None

let (|OneOptionalParameter|_|) defaultValue parameters =
    match parameters with
    | [] -> Some defaultValue
    | [value] -> Some value
    | _ -> None

let (|OneParsedOptionalParameter|_|) defaultValue tryParse parameters =
    match parameters with
    | [] -> Some defaultValue
    | [value] -> tryParse value
    | _ -> None

let (|MapAndBytesParameters|_|) parameters =
    let paramMap =
        parameters
        |> Seq.bindOpt (fun p ->
            let pos = p |> String.indexOf "="
            if pos < 0 then None
            else Some (String.substring 0 pos p, String.substringFrom (pos+1) p))
        |> Map.ofSeq
    let content =
        parameters
        |> Seq.filter (fun p -> p.Contains("=") |> not)
        |> Seq.toList
        |> function [ c ] -> Some c | _ -> None
    match content with
    | None -> None
    | Some content ->
        let bytes =
            if content.StartsWith "base64:" then
                content.Substring(7) |> String.fromBase64
            else
                content |> String.toUtf8
        Some (paramMap, bytes)
