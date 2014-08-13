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

type Agent<'a> = MailboxProcessor<'a>
type PhonebookMessage =
    | Ping of HiveName:string
    | Pair of AsyncReplyChannel<string Option>

type WorkerRole() =
    inherit RoleEntryPoint() 

    let log message (kind : string) = Trace.TraceInformation(message, kind)

    let pairInterval = 1000 * 5 // 5 secs between pairs

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
    
    let pairTopic = 
        if not (namespaceManager.TopicExists "hivepairs")
        then namespaceManager.CreateTopic "hivepairs" |> ignore
        TopicClient.CreateFromConnectionString(connString, "hivepairs")

    override wr.Run() =

        log "QueenRole entry point called" "Information"

        let phonebook = new Agent<PhonebookMessage>(fun inbox ->            
            let hives = ResizeArray<string>()
            let rng = Random ()

            let rec loop () = async {
                let! msg = inbox.Receive ()
                match msg with
                | Ping(name) -> 
                    log "Name in phonebook" "Information"
                    if hives.Contains name 
                    then ignore ()
                    else hives.Add name
                | Pair(channel) -> 
                    if hives.Count > 0
                    then
                        hives.[rng.Next(hives.Count)]
                        |> Some
                        |> channel.Reply 
                    else 
                        None |> channel.Reply 
                return! loop () }
            loop ())

        phonebook.Start ()

        let rec pingListener () =
            async {
                let msg = pingQueue.Receive ()
                match msg with
                | null -> ignore ()
                | msg  ->
                    let hiveName = msg.Properties.["HiveName"] |> string
                    log (sprintf "Queen: ping from %s" hiveName) "Information"
                    Ping(hiveName) |> phonebook.Post 
                return! pingListener () }

        pingListener () |> Async.Start

        let rec pairs () =
            async {
                let reply = phonebook.PostAndReply(fun channel -> 
                    Pair(channel))
                match reply with
                | None -> ignore ()
                | Some(hive) ->
                    let msg = new BrokeredMessage ()
                    msg.Properties.["HiveName"] <- hive
                    pairTopic.Send msg
                do! Async.Sleep pairInterval
                return! pairs () }

        pairs () |> Async.Start

        while(true) do 
            Thread.Sleep(10000)
            log "Working" "Information"

    override wr.OnStart() = 

        ServicePointManager.DefaultConnectionLimit <- 12
        base.OnStart()