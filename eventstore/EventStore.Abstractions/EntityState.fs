namespace EventStore.Abstractions

open EventStore.Abstractions
open SharpFunky

module EntityState =
    type IEntityStateDefinition<'state, 'message> =
        abstract newState: unit -> 'state
        abstract applyMessage: 'state -> 'message -> 'state
        abstract deserializeMessage: Message -> 'message list
        abstract serializeMessage: 'message -> Message

    let buildFrom
        initSequence
        initState
        (stateDef: #IEntityStateDefinition<_, _>)
        (messageStore: #IMessageStore) =
        let rec loop count state nextSequence = async {
            let! segment = messageStore.readMessages nextSequence
            let messages =
                segment.messages
                |> Seq.bind stateDef.deserializeMessage
                |> Seq.toArray
            let state' = messages |> Seq.fold stateDef.applyMessage state
            let count' = count + messages.Length

            match segment.nextSequence with
            | Some value ->
                return! loop count' state' value
            | None ->
                return state', count'
        }
        loop 0 initState initSequence

    let readEntity
        (stateDef: #IEntityStateDefinition<'state, 'message>)
        (messageStore: #IMessageStore) = async {
        let initState = stateDef.newState()
        let initSequence = 0UL
        return! buildFrom initSequence initState stateDef messageStore
    }

    let writeMessages 
        (stateDef: #IEntityStateDefinition<'state, 'message>)
        (messageStore: #IMessageStore)
        (messages: 'message list) = async {
            let msgs = messages |> List.map stateDef.serializeMessage
            let! result = messageStore.appendMessages msgs
            return ()
        }

module SnapshottedEntityState =
    type ISnapshottedEntityStateDefinition<'state, 'message> =
        inherit EntityState.IEntityStateDefinition<'state, 'message>

        abstract deserializeSnapshot: Snapshot -> 'state option
        abstract serializeSnapshot: 'state -> Snapshot

    let readEntity
        (stateDef: #ISnapshottedEntityStateDefinition<_, _>)
        (snapshotStore: #ISnapshotStore)
        (messageStore: #IMessageStore) = async {
        let! snapshot = snapshotStore.getSnapshot()

        let initState, initSequence =
            snapshot
            |> Option.bind (fun snap ->
                stateDef.deserializeSnapshot snap
                |> Option.map (fun st -> st, snap.sequence + 1UL))
            |> Option.matches 
                (fun a -> a)
                (fun () ->
                    let initState' = stateDef.newState()
                    initState', 0UL
                )

        return! EntityState.buildFrom initSequence initState stateDef messageStore
    }
