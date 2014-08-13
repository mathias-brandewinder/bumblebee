(*
Copyright (c) 2014 Mathias Brandewinder
Licensed under The MIT License (MIT)
*)

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
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
type WorkerRole() =
    inherit RoleEntryPoint() 

    let log message (kind : string) = Trace.TraceInformation(message, kind)

    let connString = 
        "Microsoft.ServiceBus.ConnectionString"
        |> CloudConfigurationManager.GetSetting

    let namespaceManager = 
        connString
        |> NamespaceManager.CreateFromConnectionString

    let pingQueue = 
        if not (namespaceManager.QueueExists "swarmqueue")
        then namespaceManager.CreateQueue "swarmqueue" |> ignore
        QueueClient.CreateFromConnectionString(connString, "swarmqueue", ReceiveMode.ReceiveAndDelete)

    override wr.Run() =

        log "QueenRole entry point called" "Information"

        let rec pingListener () =
            async {
                let msg = pingQueue.Receive ()
                match msg with
                | null -> ignore ()
                | msg  ->
                    let hiveName = msg.Properties.["HiveName"] |> string
                    log (sprintf "Queen: ping from %s" hiveName) "Information"
                return! pingListener () }

        pingListener () |> Async.Start

        while(true) do 
            Thread.Sleep(10000)
            log "Working" "Information"

    override wr.OnStart() = 

        ServicePointManager.DefaultConnectionLimit <- 12
        base.OnStart()