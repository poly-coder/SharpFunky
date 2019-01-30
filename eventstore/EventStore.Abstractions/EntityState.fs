module EventStore.EntityState

open EventStore.Abstractions
open SharpFunky

type SnapshottedEntityStateDefinition<'state, 'message> = {
    initState: unit -> 'state
    applyMessage: 'state -> 'message -> 'state
    snapshotVersion: string
    snapshotSerializer: Converter<'state, Snapshot>
    messageSerializer: Converter<'message, Message>
}

let readSnapshotted (snapshotStore: ISnapshotStore) (messageStore: IMessageStore) stateDef = async {
    let! snapshot = snapshotStore.getSnapshot stateDef.snapshotVersion
    let initState, initSequence =
        match snapshot with
        | Some snapshot ->
            let state = snapshot |> Converter.backward stateDef.snapshotSerializer
            let nextSequence = snapshot.sequence + 1UL
            state, nextSequence
        | None ->
            stateDef.initState(), 0UL
    
    let rec buildState state nextSequence = async {
        let! segment = messageStore.readMessages nextSequence
        let state' =
            (state, segment.messages)
            ||> Seq.fold (fun st msg -> 
                let message = msg |> Converter.backward stateDef.messageSerializer
                stateDef.applyMessage st message)

        match segment.nextSequence with
        | Some value ->
            return! buildState state' value
        | None ->
            return state'
    }

    let! state = buildState initState initSequence

    return state
}

