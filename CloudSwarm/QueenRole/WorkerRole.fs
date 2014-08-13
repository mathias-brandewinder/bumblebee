namespace QueenRole

open System
open System.Collections.Generic
open System.Diagnostics
open System.Linq
open System.Net
open System.Threading
open Microsoft.WindowsAzure
open Microsoft.WindowsAzure.Diagnostics
open Microsoft.WindowsAzure.ServiceRuntime

type WorkerRole() =
    inherit RoleEntryPoint() 

    let log message (kind : string) = Trace.TraceInformation(message, kind)

    override wr.Run() =

        log "QueenRole entry point called" "Information"
        while(true) do 
            Thread.Sleep(10000)
            log "Working" "Information"

    override wr.OnStart() = 

        ServicePointManager.DefaultConnectionLimit <- 12
        base.OnStart()