module WebApp

open Giraffe
open System

let webApp (services: IServiceProvider) =
    Core.choose [
        route "/ping"   >=> text "pong"
        route "/"       >=> htmlString """<a href="/ping">Go to PING</a>""" ]
