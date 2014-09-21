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
        
    let bankSize = 10

    override hive.Run() =

        log "HiveRole entry point called" "Information"

        // TODO flesh out. Save to blob, send message
        let share (evaluation:Evaluation) (partner:string) =
            
            let fileName = Guid.NewGuid () |> string
            let blob = evalsContainer.GetBlockBlobReference fileName
            
            let asByteArray = evaluation |> binary.Pickle
            
            use stream = new MemoryStream(asByteArray)
            blob.UploadFromStream stream
            
            let blob = evalsContainer.GetBlockBlobReference "TEST"
            blob.UploadText (DateTime.Now.ToString ())

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

        let work = new Agent<HiveMessage>(fun inbox ->
            let rng = Random ()
            let rec loop (evaluations:Evaluation[],best:Evaluation,sharing) = async {
                let! msg = inbox.Receive ()
                // TODO actual work with evaluation
                let evaluations,best,sharing =
                    match msg with
                    | SharingRequest(partner) ->
                        // simply set marker so that
                        // next incoming will be shared
                        evaluations,best,ShareWith(partner)
                    | ReturningBee(evaluation) ->
                        match sharing with
                        | Isolated -> ignore ()
                        | ShareWith(partner) -> share evaluation partner
                        // TODO incorrect but good enough for now
                        let best = 
                            if evaluation.Value > best.Value
                            then 
                                log (sprintf "Value %f" evaluation.Value) "Information"
                                evaluation
                            else best
                        
                        for i in 0 .. bankSize - 1 do
                            if evaluations.[i].Value < evaluation.Value
                            then evaluations.[i] <- evaluation
                        
                        let task = new Task(fun _ -> 
                            let index = rng.Next(bankSize)
                            let candidate = evaluations.[index]
                            let copy = candidate.Solution |> shuffle rng
                            let value = - length copy
                            Evaluation(value,copy) |> ReturningBee |> inbox.Post)
                        task.Start ()

                        evaluations,best,Isolated

                    | External(evaluation) ->    
                        // TODO this is very, very primitive 
                        // TODO incorrect but good enough for now
                        let best = 
                            if evaluation.Value > best.Value
                            then 
                                log (sprintf "Value %f" evaluation.Value) "Information"
                                evaluation
                            else best
                        let changed = rng.Next(bankSize)
                        evaluations.[changed] <- evaluation                                       
                        evaluations,best,sharing
                return! loop (evaluations,best,sharing) }

            let size = root.Length
            let rootSolution = 
                let value = - length root
                Evaluation(value,root)

            let evaluations = Array.init bankSize (fun _ -> rootSolution)
            inbox.Post (ReturningBee(rootSolution))
            loop (evaluations,rootSolution,Isolated))

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