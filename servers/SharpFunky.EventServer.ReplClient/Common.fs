module Common

open SharpFunky

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
