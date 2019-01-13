namespace SharpFunky.EventServer.Interfaces

open System.Threading.Tasks
open Orleans

type IKeyValueStoreGrain =
    inherit IGrainWithStringKey

    abstract get: key: string -> Task<byte[] option>

    abstract put: key: string * value: byte[] -> Task<unit>

    abstract remove: key: string -> Task<unit>
