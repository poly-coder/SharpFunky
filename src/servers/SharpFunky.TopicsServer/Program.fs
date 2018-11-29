open System
open Microsoft.AspNetCore
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Configuration
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Giraffe

let configureServices (services: IServiceCollection) =
    do services.AddGiraffe() |> ignore

let configureApp (app: IApplicationBuilder) =
    let env = app.ApplicationServices.GetService<IHostingEnvironment>()
    if env.IsDevelopment() then
        app.UseDeveloperExceptionPage() |> ignore
        
    do app.UseGiraffe <| WebApp.webApp app.ApplicationServices

let runServer args =
    let hostConfig =
        ConfigurationBuilder()
            .AddJsonFile("hosting.json", optional=true, reloadOnChange=false)
            .AddCommandLine(args: string[])
            .Build()
    do WebHost
        .CreateDefaultBuilder(args)
        .UseConfiguration(hostConfig)
        .ConfigureServices(configureServices)
        .Configure(Action<_> configureApp)
        .Build()
        .Run()

[<EntryPoint>]
let main argv =
    do runServer argv
    0
