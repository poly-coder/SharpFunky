namespace SharpFunky.TopicsServer.Controllers

open System
open System.IO
open SharpFunky
open SharpFunky.Topic.Core
open Microsoft.AspNetCore.Mvc


module HttpHelpers =
    open Microsoft.AspNetCore.Mvc.ModelBinding
    open System.Net

    type HttpError =
    | ModelError of string * string
    | NotFound of string
    | ExnError of exn

    let findErrorStatus errors =
        (HttpStatusCode.OK, errors)
        ||> Seq.fold (fun curr err ->
            match curr, err with
            | HttpStatusCode.InternalServerError, _ -> curr
            | _, ExnError _ -> HttpStatusCode.InternalServerError
            | HttpStatusCode.NotFound, _ -> curr
            | _, NotFound _ -> HttpStatusCode.NotFound
            | HttpStatusCode.BadRequest, _ -> curr
            | _, ModelError _ -> HttpStatusCode.BadRequest)

    let aggregateErrors (modelState: ModelStateDictionary) errors =
        errors
        |> Seq.iter (function 
            | ModelError(k,v) -> do modelState.AddModelError(k, v)
            | NotFound reason -> do modelState.AddModelError("not-found", reason)
            | ExnError exn -> do modelState.AddModelError(exn.GetType().Name, exn.Message))

    let asActionResult (modelState: ModelStateDictionary) result: IActionResult =
        match result with
        | Success (a, _) ->
            OkObjectResult(a) :> IActionResult
        | Failure errors ->
            do aggregateErrors modelState errors
            let status = findErrorStatus errors
            ObjectResult(modelState)
            |> tee (fun res -> res.StatusCode <- int status |> Nullable)
            |> fun res -> res :> IActionResult

open HttpHelpers
open FSharp.Control

[<Route("api/[controller]")>]
[<ApiController>]
type TopicController (factory: ITopicServiceFactory) =
    inherit ControllerBase()

    let readStream (stream: Stream) = async {
        use mem = new MemoryStream()
        do! stream.CopyToAsync(mem) |> Async.ofTaskVoid
        return mem.ToArray()
    }

    member private this.ExtractMessageId(msg: Message) = Trial.trial {
        match this.Request.Headers.TryGetValue("X-MessageID") |> Option.ofTryOp with
        | Some vals when vals.Count = 1 ->
            return msg |> Message.MessageId.set vals.[0]
        | Some _ ->
            return! Trial.failure (ModelError("X-MessageID", "Must be a single value"))
        | None ->
            return msg |> Message.MessageId.generate
    }

    member private this.ExtractContentType(msg: Message) = 
        match this.Request.ContentType with
        | ct when isNotNull ct ->
            msg |> Message.MetaString.put "Content-Type" this.Request.ContentType
        | _ -> msg

    member private this.ExtractCustomHeaders (prefix: string) (msg: Message) = 
        this.Request.Headers.Keys
        |> Seq.filter (fun k -> k <> prefix && k.StartsWith(prefix))
        |> Seq.map (fun k -> k.Substring(prefix.Length), this.Request.Headers.Item(k))
        |> Seq.fold (fun m (k, vs) -> 
            if vs.Count = 1
            then m |> Message.MetaString.put k vs.[0]
            else m |> Message.MetaStrings.put k (Seq.toList vs)
        ) msg

    member private this.ExtractExtraMeta(msg: Message) =
        msg |> Message.MetaDate.put "ReceivedAt" (DateTime.UtcNow)

    member private this.ExtractData(msg: Message) = AsyncTrial.asyncTrial {
        match this.Request.ContentLength |> Option.ofNullable with
        | None -> return msg |> Message.Data.clear
        | Some l when l <= 0L -> return msg |> Message.Data.clear
        | Some _ ->
            let! data =
                AsyncTrial.catch readStream this.Request.Body
                |> AsyncTrial.mapMessagesSync (fun _ -> 
                    [ModelError("Body", "Error reading request body")])
            return msg |> Message.Data.put data 
    }

    member private this.FindTopic topicId = async {
        match! factory.getTopic topicId with
        | Some topic -> return Trial.success topic
        | None -> return Trial.failure (NotFound (sprintf "Topic %s not found" topicId))
    }

    [<HttpPost("{id}")>]
    member this.Post(id: string) =
        this.FindTopic id
        |> AsyncTrial.bind (fun topic ->
            Message.empty
                |> this.ExtractMessageId
                |> Trial.map this.ExtractContentType
                |> Trial.map this.ExtractExtraMeta
                |> Trial.map (this.ExtractCustomHeaders "X-Message-")
                |> AsyncTrial.ofTrial
                |> AsyncTrial.bind this.ExtractData
                |> AsyncTrial.bind (fun msg ->
                    let msgSeq = AsyncSeq.singleton msg
                    topic.publish msgSeq
                    |> Async.map (konst msg)
                    |> AsyncTrial.ofAsync
                )
        )
        |> Async.map (asActionResult this.ModelState)
        |> Async.toTask
