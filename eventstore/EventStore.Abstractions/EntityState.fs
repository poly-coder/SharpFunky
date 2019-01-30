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
        let rec buildState state nextSequence = async {
            let! segment = messageStore.readMessages nextSequence
            let state' =
                segment.messages
                |> Seq.bind stateDef.deserializeMessage
                |> Seq.fold stateDef.applyMessage state

            match segment.nextSequence with
            | Some value ->
                return! buildState state' value
            | None ->
                return state'
        }
        buildState initState initSequence

    let readEntity
        (stateDef: #IEntityStateDefinition<'state, 'message>)
        (messageStore: #IMessageStore) = async {
        let initState = stateDef.newState()
        let initSequence = 0UL
        return! buildFrom initSequence initState stateDef messageStore
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
