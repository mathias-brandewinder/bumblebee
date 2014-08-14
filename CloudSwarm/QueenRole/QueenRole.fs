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
    | Ping of HiveName:string * Address:string
    | Pair of AsyncReplyChannel<(string*string) Option>

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

        // maintains the current known hives
        let phonebook = new Agent<PhonebookMessage>(fun inbox ->            
            let hives = ResizeArray<string*string>()
            let rng = Random ()

            let rec loop () = async {
                let! msg = inbox.Receive ()
                match msg with
                | Ping(name,address) -> 
                    log "Name in phonebook" "Information"
                    if hives.Contains((name,address)) 
                    then ignore ()
                    else hives.Add (name,address)
                | Pair(channel) -> 
                    let count = hives.Count
                    if count > 0
                    then
                        let i,j = rng.Next(count), rng.Next(count)
                        if (i<>j) 
                        then
                            (fst hives.[i], snd hives.[j])
                            |> Some
                            |> channel.Reply 
                        else
                            None |> channel.Reply 
                    else 
                        None |> channel.Reply 
                return! loop () }
            loop ())


        // listens to pings from hives
        let rec pingListener () =
            async {
                let msg = pingQueue.Receive ()
                match msg with
                | null -> ignore ()
                | msg  ->
                    let hiveName = msg.Properties.["HiveName"] |> string
                    let address = msg.Properties.["Address"] |> string
                    log (sprintf "Queen: ping from %s %s" hiveName address) "Information"
                    Ping(hiveName,address) |> phonebook.Post 
                return! pingListener () }


        // send regularly random pairing of hives
        // that will now work together
        let rec pairs () =
            async {
                let reply = phonebook.PostAndReply(fun channel -> 
                    Pair(channel))
                match reply with
                | None -> ignore ()
                | Some(name,address) ->
                    let msg = new BrokeredMessage ()
                    msg.Properties.["HiveName"] <- name
                    msg.Properties.["PartnerAddress"] <- address
                    pairTopic.Send msg
                do! Async.Sleep pairInterval
                return! pairs () }

        // start everything
        phonebook.Start ()
        pingListener () |> Async.Start
        pairs () |> Async.RunSynchronously

    override wr.OnStart() = 

        ServicePointManager.DefaultConnectionLimit <- 12
        base.OnStart()