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
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob
open Problem.MonaLisa

type Agent<'a> = MailboxProcessor<'a>

type HivePartnership = 
    | Isolated 
    | ShareWith of HiveName:string

type HiveMessage =
    | SharingRequest of hiveName:string
    | ReturningBee of evaluation:Evaluation

type HiveWorkerRole() =
    inherit RoleEntryPoint() 

    let log message (kind : string) = Trace.TraceInformation(message, kind)

    let hiveId = Guid.NewGuid ()
    let hiveName = string hiveId

    let pingInterval = 1000 * 5 // 5 secs between pings
    let pairInterval = 1000 * 5 // 5 secs between pair reads

    let pingQueueName = "pings"
    let pairQueueName = "pairs"
    let workQueueName = "work"

    let workInterval = 1000 * 5 // 5 secs between work reads

    let evalsContainerName = "evaluations"

    let connString = 
        "Microsoft.ServiceBus.ConnectionString"
        |> CloudConfigurationManager.GetSetting

    let namespaceManager = 
        connString
        |> NamespaceManager.CreateFromConnectionString

    // assume the queue already exists
    let pingQueue = 
        QueueClient.CreateFromConnectionString(connString, pingQueueName, ReceiveMode.ReceiveAndDelete)

    // assume the topic already exists
    let pairSubscription =
        let topicFilter = SqlFilter(hiveName |> sprintf "HiveName = '%s'")
        namespaceManager.CreateSubscription(pairQueueName, hiveName, topicFilter) |> ignore
        SubscriptionClient.CreateFromConnectionString(connString, pairQueueName, hiveName)

    // work queue
    let workTopic = TopicClient.CreateFromConnectionString(connString, workQueueName)

    let workSubscription =
        let topicFilter = SqlFilter(hiveName |> sprintf "HiveName = '%s'")
        namespaceManager.CreateSubscription(workQueueName, hiveName, topicFilter) |> ignore
        SubscriptionClient.CreateFromConnectionString(connString, pairQueueName, hiveName)
        
    // evaluations container

    let storageAccount = 
        "StorageConnectionString"
        |> CloudConfigurationManager.GetSetting
        |> CloudStorageAccount.Parse

    let blobClient = storageAccount.CreateCloudBlobClient ()
    let evalsContainer = blobClient.GetContainerReference evalsContainerName

    override hive.Run() =

        log "HiveRole entry point called" "Information"

        // TODO flesh out. Save to blob, send message
        let share (evaluation:Evaluation) (partner:string) =
            
            let fileName = Guid.NewGuid () |> string
            let blob = evalsContainer.GetBlockBlobReference fileName
            
            let msg = new BrokeredMessage ()
            msg.Properties.["HiveName"] <- partner
            msg.Properties.["FileName"] <- fileName

            workTopic.Send msg

        let (|SharedEvaluation|_|) (msg:BrokeredMessage) =
            match msg with
            | null -> None
            | msg ->
                try
                    msg.Properties.["FileName"]
                    |> string
                    |> Some
                with _ -> None

        let rec workListener () =
            async {
                let msg = workSubscription.Receive ()
                match msg with
                | SharedEvaluation(fileName) ->
                    // TODO read blob, send to worker
                    let blob = evalsContainer.GetBlockBlobReference fileName
                    ignore ()
                | _ -> ignore ()
                do! Async.Sleep workInterval
                return! workListener () }

        let work = new Agent<HiveMessage>(fun inbox ->
            let rec loop (evaluations,sharing) = async {
                let! msg = inbox.Receive ()
                let evaluations,sharing =
                    match msg with
                    | SharingRequest(partner) ->
                        evaluations,ShareWith(partner)
                    | ReturningBee(evaluation) ->
                        match sharing with
                        | Isolated -> ignore ()
                        | ShareWith(partner) -> share evaluation partner
                        evaluations,Isolated
                return! loop (evaluations,sharing) }

            let evaluations = [||]
            loop (evaluations,Isolated))

        // Send pings on regular basis
        // TODO make it actually async
        let rec ping () =
            async {
                let msg = new BrokeredMessage ()
                msg.Properties.["HiveName"] <- hiveName
                pingQueue.Send msg
                do! Async.Sleep pingInterval
                return! ping () }

        let (|HivePartner|_|) (msg:BrokeredMessage) =
            match msg with
            | null -> None
            | msg ->
                try
                    msg.Properties.["Partner"]
                    |> string
                    |> Some
                with _ -> None
                    
        // listens to messages suggesting
        // new hive to pair up with
        let rec pairListener () =
            async {
                let msg = pairSubscription.Receive ()
                match msg with
                | HivePartner partner -> 
                    log (sprintf "Hive %s pairs with %s" hiveName partner) "Information"
                    msg.Complete () // TODO figure out if I can avoid this when creating sub
                    SharingRequest(partner) |> work.Post 
                | _ -> ignore ()

                do! Async.Sleep pairInterval
                return! pairListener () }

        // start everything
        ping () |> Async.Start
        pairListener () |> Async.RunSynchronously

    override hive.OnStart() = 
        base.OnStart()

    override hive.OnStop () =
        // TODO remove subscription
        base.OnStop ()