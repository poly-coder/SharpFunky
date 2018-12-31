module SharpFunky.Storage.EventStore.AzureTables

open SharpFunky
open SharpFunky.Conversion
open SharpFunky.Storage
open Microsoft.WindowsAzure.Storage.Table

type Options = {
    table: CloudTable
}

[<RequireQualifiedAccess>]
module Options = 
    let from table = 
        {
            table = table
        }
    let table = Lens.cons' (fun opts -> opts.table) (fun value opts -> { opts with table = value })

