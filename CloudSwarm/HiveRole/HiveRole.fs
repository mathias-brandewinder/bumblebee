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

open fszmq
open fszmq.Context
open fszmq.Socket

open Nessos.FsPickler

type HivePartnership = | Isolated | TalkTo of Address:string
type SolutionRequest = | SolutionRequest
type Solution = string // TODO: temporary

type WorkerRole() =
    inherit RoleEntryPoint() 

    let log message (kind : string) = Trace.TraceInformation(message, kind)
    
//    let binary = FsPickler.CreateBinary ()
//    let encode = binary.Pickle
//    let decode = binary.UnPickle

    let encode (x:string) = x |> System.Text.Encoding.ASCII.GetBytes
    let decode (x:byte[]) = x |> System.Text.Encoding.ASCII.GetString

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

    let serverEndpoint () =
        let instance = RoleEnvironment.CurrentRoleInstance
        instance.InstanceEndpoints.["SolutionsServer"].IPEndpoint

    override wr.Run() =

        log "HiveRole entry point called" "Information"
        
        // when created in constructor, context gets disposed?
        let server = 
            let context = new Context ()
            let server = rep context
            let endpoint = serverEndpoint ()
            let address = endpoint |> string |> sprintf "tcp://%s"
            bind server address
            server 

        // Send pings on regular basis
        let rec ping () =
            async {
                let msg = new BrokeredMessage ()
                msg.Properties.["HiveName"] <- hiveName
                let address = serverEndpoint () |> string
                msg.Properties.["Address"] <- string address
                pingQueue.Send msg
                do! Async.Sleep pingInterval
                return! ping () }

        let solutionsRequest (address:string) =
            async {
                let client = new Context () |> req
                address |> connect client
                hiveName 
                |> System.Text.Encoding.ASCII.GetBytes
                |> send client
                let reply = (recv >> decode) client
                address |> disconnect client
                return reply }

        // listens to messages suggesting
        // new hive to pair up with
        let rec pairListener (state) =
            async {
                log (sprintf "%A" state) "Information"
                let msg = subscription.Receive ()
                let state = 
                    match msg with
                    | null -> state
                    | msg  ->
                        let hiveName = msg.Properties.["HiveName"] |> string
                        let partnerAddress = msg.Properties.["PartnerAddress"] |> string
                        log (sprintf "Hive %s pairs with %s" hiveName partnerAddress) "Information"
                        msg.Complete () // TODO figure out if I can avoid this when creating sub
                        let address = partnerAddress |> sprintf "tcp://%s"
                        let solution = solutionsRequest (partnerAddress) |> Async.RunSynchronously

                        TalkTo(partnerAddress)
                do! Async.Sleep pairInterval
                return! pairListener (state) }

        let rec solutionsServer (server) =
            async {
                let request = server |> recv |> decode
                request + "," + hiveName
                |> encode
                |>> server   
                return! solutionsServer (server) }

        // start everything
        server |> solutionsServer |> Async.Start
        ping () |> Async.Start
        pairListener (Isolated) |> Async.RunSynchronously

    override wr.OnStart() = 

        ServicePointManager.DefaultConnectionLimit <- 12
        base.OnStart()
