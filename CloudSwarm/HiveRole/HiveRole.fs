(*
Copyright (c) 2014 Mathias Brandewinder
Licensed under The MIT License (MIT)
*)

namespace HiveRole

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

    let hiveId = Guid.NewGuid ()
    let hiveName = string hiveId
    let pingInterval = 1000 * 5 // 5 secs between pings
    let pairInterval = 1000 * 5 // 5 secs between pair reads

    let connString = 
        "Microsoft.ServiceBus.ConnectionString"
        |> CloudConfigurationManager.GetSetting

    let namespaceManager = 
        connString
        |> NamespaceManager.CreateFromConnectionString

    // assume the queue is created by the Queen
    // need to handle the disconnected case!
    let pingQueue = 
        QueueClient.CreateFromConnectionString(connString, "swarmqueue", ReceiveMode.ReceiveAndDelete)

    // assume the topic is created by the Queen
    // need to handle the disconnected case!
    let subscription =
        let topicFilter = SqlFilter(hiveName |> sprintf "HiveName = '%s'")
        namespaceManager.CreateSubscription("hivepairs", hiveName, topicFilter) |> ignore
        SubscriptionClient.CreateFromConnectionString(connString, "hivepairs", hiveName)

    override wr.Run() =

        log "HiveRole entry point called" "Information"

        let rec ping () =
            async {
                let msg = new BrokeredMessage ()
                msg.Properties.["HiveName"] <- hiveName
                pingQueue.Send msg
                do! Async.Sleep pingInterval
                return! ping () }

        ping () |> Async.Start

        let rec pairListener () =
            async {
                let msg = subscription.Receive ()
                match msg with
                | null -> ignore ()
                | msg  ->
                    let hiveName = msg.Properties.["HiveName"] |> string
                    log (sprintf "Hive: pair from %s" hiveName) "Information"
                    msg.Complete ()
                do! Async.Sleep pairInterval
                return! pairListener () }

        pairListener () |> Async.Start

        while(true) do 
            Thread.Sleep(10000)
            log "Working" "Information"

    override wr.OnStart() = 

        ServicePointManager.DefaultConnectionLimit <- 12
        base.OnStart()
