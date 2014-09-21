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
    | PairRequest

type QueenWorkerRole() =
    inherit RoleEntryPoint() 

    let log message (kind : string) = Trace.TraceInformation(message, kind)

    let pairInterval = 1000 * 5 // 5 secs between pairs

    let pingQueueName = "pings"
    let pairQueueName = "pairs"

    let connString = 
        "Microsoft.ServiceBus.ConnectionString"
        |> CloudConfigurationManager.GetSetting

    let namespaceManager = 
        connString
        |> NamespaceManager.CreateFromConnectionString

    // Assumes queue exists
    let pingQueue = 
        QueueClient.CreateFromConnectionString(connString, pingQueueName, ReceiveMode.ReceiveAndDelete)
    
    let pairTopic = 
        TopicClient.CreateFromConnectionString(connString, pairQueueName)

    // Assumes topic exists
    override queen.Run() =

        log "QueenRole entry point called" "Information"

        let test () =
            ignore ()

        // maintains the current known hives
        let phonebook = new Agent<PhonebookMessage>(fun inbox ->            
            let rng = Random ()
            let rec loop (book) = async {
                let! msg = inbox.Receive ()
                match msg with
                | Ping(name) -> 
                    log "Name in phonebook" "Information"
                    let book = book |> Set.add name
                    return! loop (book)
                | PairRequest ->
                    test ()
                    log "Pair requested" "Information"
                    let count = book |> Set.count
                    if (count < 2)
                    then return! loop (book)
                    else
                        // Ugly as hell but small collection 
                        let array = book |> Set.toArray
                        let first = rng.Next(count)
                        let second = rng.Next(count)
                        if (first <> second)
                        then
                            let pair = new BrokeredMessage()
                            pair.Properties.["HiveName"] <- array.[first]
                            pair.Properties.["Partner"] <- array.[second]
                            pairTopic.Send pair
                        return! loop (book) }
            let hives = Set.empty
            loop (hives))

        let (|PingMessage|_|) (msg:BrokeredMessage) =
            match msg with
            | null -> None
            | msg ->
                try
                    msg.Properties.["HiveName"]
                    |> string
                    |> Some
                with _ -> None

        // listens to pings from hives
        // TODO handle "old" names
        let rec pingListener () =
            async {
                let! msg = pingQueue.ReceiveAsync () |> Async.AwaitTask
                match msg with
                | PingMessage(name) ->
                    log (sprintf "Queen: ping from %s" name) "Information"
                    Ping(name) |> phonebook.Post 
                | _ -> ignore ()
                return! pingListener () }

        let rec pairsGenerator () =
            async {
                phonebook.Post PairRequest
                do! Async.Sleep pairInterval
                return! pairsGenerator () }

        // start everything
        phonebook.Start ()
        pairsGenerator () |> Async.Start
        pingListener () |> Async.RunSynchronously

    override queen.OnStart () = 
        base.OnStart ()

    override queen.OnStop () =
        base.OnStop ()