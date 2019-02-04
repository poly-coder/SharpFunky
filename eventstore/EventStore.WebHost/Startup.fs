namespace EventStore.WebHost

open System
open System.Collections.Generic
open System.Linq
open System.Threading.Tasks
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.HttpsPolicy;
open Microsoft.AspNetCore.Mvc
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Logging
open Orleans
open Orleans.Hosting
open EventStore.GrainInterfaces
open FSharp.Control.Tasks.V2

type Startup private () =
    new (configuration: IConfiguration) as this =
        Startup() then
        this.Configuration <- configuration

    // This method gets called by the runtime. Use this method to add services to the container.
    member this.ConfigureServices(services: IServiceCollection) =
        // Add framework services.
        services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_1) |> ignore

        services.AddSingleton<IClusterClient>(fun svc ->
            let builder = new ClientBuilder()
            let client = 
                builder
                    .UseLocalhostClustering()
                    .ConfigureApplicationParts(fun parts ->
                        parts
                            .AddApplicationPart(typeof<IPropertiesManagerGrain>.Assembly)
                            .WithCodeGeneration()
                        |> ignore)
                    .ConfigureLogging(fun logging -> logging.AddConsole() |> ignore)
                    .Build()
            // TODO: Can be done without blocking thread!?
            client.Connect(fun exn -> task {
                do! Task.Delay(1000)
                return true
            }) |> fun t -> t.Wait()
            client
        ) |> ignore

    // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
    member this.Configure(app: IApplicationBuilder, env: Microsoft.AspNetCore.Hosting.IHostingEnvironment) =
        if (env.IsDevelopment()) then
            app.UseDeveloperExceptionPage() |> ignore
        else
            app.UseHsts() |> ignore

        app.UseHttpsRedirection() |> ignore
        app.UseMvc() |> ignore

    member val Configuration : IConfiguration = null with get, set