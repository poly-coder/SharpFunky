namespace EventStore.Abstractions

type Snapshot = {
    data: byte[]
    metadata: Map<string, string>
    sequence: uint64
}

type ISnapshotStore =
    abstract getSnapshot: unit -> Async<Snapshot option>
    abstract saveSnapshot: Snapshot -> Async<unit>
    abstract removeSnapshot: unit -> Async<unit>

type ISnapshotStoreLocator<'key> =
    abstract getSnapshotStore: key: 'key -> Async<ISnapshotStore>
