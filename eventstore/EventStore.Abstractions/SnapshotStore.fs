namespace EventStore.Abstractions

type Snapshot = {
    data: byte[]
    metadata: Map<string, string>
    sequence: uint64
}

type ISnapshotStore =
    abstract getSnapshot: version: string -> Async<Snapshot option>
    abstract saveSnapshot: version: string * byte[] -> Async<unit>
    abstract removeSnapshot: version: string -> Async<unit>
    abstract clearSnapshots: unit -> Async<unit>

type ISnapshotStoreLocator =
    abstract getSnapshotStore: entityId: string -> Async<ISnapshotStore>
