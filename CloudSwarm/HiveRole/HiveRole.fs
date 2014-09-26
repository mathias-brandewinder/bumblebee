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
open System.Threading.Tasks
open System.IO
open Microsoft.WindowsAzure
open Microsoft.WindowsAzure.Diagnostics
open Microsoft.WindowsAzure.ServiceRuntime
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob
open Microsoft.WindowsAzure.Storage.Table

open Nessos.FsPickler

open Problem.MonaLisa

type Agent<'a> = MailboxProcessor<'a>

type HivePartnership = 
    | Isolated 
    | ShareWith of HiveName:string

type HiveMessage =
    | SharingRequest of hiveName:string
    | ReturningBee of evaluation:Evaluation
    | External of evaluation:Evaluation

type Tuner = 
    {   Load:int;
        Estimates:Estimates;
        LastReceived:DateTime; }

type BestSolution () =
    inherit TableEntity ()
    member val Blob = "" with get,set
    member val Quality = 0. with get,set
    member val HiveName = "" with get,set
    member val Timestamp = DateTime() with get,set

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
    let rootContainerName = "root"

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
        SubscriptionClient.CreateFromConnectionString(connString, workQueueName, hiveName)
        
    // evaluations container

    let storageAccount = 
        "StorageConnectionString"
        |> CloudConfigurationManager.GetSetting
        |> CloudStorageAccount.Parse

    let blobClient = storageAccount.CreateCloudBlobClient ()
    let evalsContainer = blobClient.GetContainerReference evalsContainerName

    let tableClient = storageAccount.CreateCloudTableClient ()
    let bestSolutions = 
        let table = tableClient.GetTableReference "best"
        table.CreateIfNotExists () |> ignore
        table

    let binary = FsPickler.CreateBinary ()

    let root =
        let text = 
            let container = blobClient.GetContainerReference rootContainerName
            let blob = container.GetBlockBlobReference "root"
            blob.DownloadText ()

        text.Split ('\n')
        |> Seq.skip 6
        |> Seq.take 100000
        |> Seq.map (fun line -> 
            let fields = line.Split(' ')
            Point(float fields.[1],float fields.[2]))
        |> Seq.toArray       
        
    let bankSize = DefaultConfig.HiveSize

    override hive.Run() =

        log "HiveRole entry point called" "Information"

        // TODO flesh out. Save to blob, send message
        let share (evaluation:Evaluation) (partner:string) =
            
            let fileName = Guid.NewGuid () |> string
            let blob = evalsContainer.GetBlockBlobReference fileName
            
            let asByteArray = evaluation |> binary.Pickle
            
            use stream = new MemoryStream(asByteArray)
            blob.UploadFromStream stream
            
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

        let saveBest (evaluation:Evaluation) =
            // save blob
            // TODO obvious duplication with shareWork
            // TODO save as JSON, or other human-palatable format?
            let fileName = Guid.NewGuid () |> string
            let blob = evalsContainer.GetBlockBlobReference fileName
            
            let asByteArray = evaluation |> binary.Pickle
            
            use stream = new MemoryStream(asByteArray)
            blob.UploadFromStream stream

            // log entry
            let entry = BestSolution ()
            entry.Blob <- fileName
            entry.HiveName <- hiveName
            entry.Quality <- evaluation.Value
            entry.PartitionKey <- hiveName
            entry.RowKey <- Guid.NewGuid () |> string

            TableOperation.Insert(entry)
            |> bestSolutions.Execute
            |> ignore

        let work = new Agent<HiveMessage>(fun inbox ->
            
            let rng = Random ()

            let rec loop (evaluations:Evaluation[],best:Evaluation,sharing,tuner:Tuner) = async {
                
                let! msg = inbox.Receive ()

                let evaluations,best,sharing,tuner =
                    match msg with
                    | SharingRequest(partner) ->
                        // simply set marker so that
                        // next incoming will be shared
                        evaluations,best,ShareWith(partner),tuner
                    | ReturningBee(evaluation) ->
                        match sharing with
                        | Isolated -> ignore ()
                        | ShareWith(partner) -> share evaluation partner

                        let best = 
                            if evaluation.Value > best.Value
                            then 
                                log (sprintf "New best: %.0f" evaluation.Value) "Information"
                                saveBest evaluation
                                evaluation
                            else best
                        
                        let now = DateTime.Now
                        let elapsed = (now - tuner.LastReceived).TotalMilliseconds
                        let estimates = learn tuner.Estimates (tuner.Load,elapsed)
                        let newload = decide rng estimates tuner.Load
                        let tasks = 1 + newload - tuner.Load
                        log (sprintf "load %i" newload) "Information"

                        // waggle
                        for i in 0 .. bankSize - 1 do
                            if evaluations.[i].Value < evaluation.Value && rng.NextDouble () < DefaultConfig.ProbaConvince
                            then evaluations.[i] <- evaluation
                        
                        for t in 1 .. tasks do
                            let rng = Random(rng.Next())
                            let task = new Task(fun _ -> 
                                let candidate =
                                    let scout = rng.NextDouble ()
                                    if scout < DefaultConfig.ProbaScout
                                    then
                                        root |> shuffle rng
                                    else 
                                        let index = rng.Next(bankSize)
                                        let candidate = evaluations.[index]
                                        let copy = candidate.Solution |> localSearch rng  
                                        copy                                  
                                let value = quality candidate
                                Evaluation(value,candidate) |> ReturningBee |> inbox.Post)

                            task.Start ()

                        evaluations,best,Isolated,{ Load = newload; Estimates = estimates; LastReceived = now }

                    | External(evaluation) ->    
                        // TODO obvious code duplication here
                        let best = 
                            if evaluation.Value > best.Value
                            then evaluation
                            else best
                        let changed = rng.Next(bankSize)
                        evaluations.[changed] <- evaluation                                       
                        evaluations,best,sharing,tuner
                return! loop (evaluations,best,sharing,tuner) }

            let rootSolution = Evaluation(quality root,root)

            let evaluations = Array.init DefaultConfig.HiveSize (fun _ -> rootSolution)

            inbox.Post (ReturningBee(rootSolution))

            let tuner = { Load = 1; Estimates = Map.empty; LastReceived = DateTime.Now }
            loop (evaluations,rootSolution,Isolated,tuner))

        let rec workListener () =
            async {
                let msg = workSubscription.Receive ()
                match msg with
                | SharedEvaluation(fileName) ->

                    let blob = evalsContainer.GetBlockBlobReference fileName
                    use stream = new MemoryStream()
                    blob.DownloadToStream(stream)
                    let asArray = stream.ToArray () 
                    let evaluation = binary.UnPickle<Evaluation> asArray
                    External(evaluation) |> work.Post 

                | _ -> ignore ()
                do! Async.Sleep workInterval
                return! workListener () }

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
        workListener () |> Async.Start
        work.Start ()
        pairListener () |> Async.RunSynchronously

    override hive.OnStart() = 
        base.OnStart()

    override hive.OnStop () =
        // TODO remove subscription
        base.OnStop ()