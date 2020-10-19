open System
open Confluent.Kafka
open Confluent.SchemaRegistry
open FSharp.Control.Tasks.V2 
open Examples.KafkaSerde

type Mode =  Producer | Consumer

type Options = {
    Mode : Mode option
    Topic : string
    BootstrapServers : string
    Username: string
    Password: string
    SchemaRegistryUrl : string
    SchemaRegistryCreds : string
}

let parseArgs argv =
    let defaultOptions = {
        Mode = None
        Topic = "example_topic"
        BootstrapServers = ""; Username = ""; Password = ""
        SchemaRegistryUrl = ""; SchemaRegistryCreds = ""}

    let rec parseCommandLine args optionsSoFar =
        match args with
        | [] -> optionsSoFar
        | "-t" :: v :: xs -> parseCommandLine xs {optionsSoFar with Topic = v}
        | "-b" :: v :: xs -> parseCommandLine xs {optionsSoFar with BootstrapServers = v}
        | "-u" :: v :: xs -> parseCommandLine xs {optionsSoFar with Username = v}
        | "-p" :: v :: xs -> parseCommandLine xs {optionsSoFar with Password = v}
        | "-r" :: v :: xs -> parseCommandLine xs {optionsSoFar with SchemaRegistryUrl = v}
        | "-rcreds" :: v :: xs -> parseCommandLine xs {optionsSoFar with SchemaRegistryCreds = v}
        | "producer" :: xs -> parseCommandLine xs {optionsSoFar with Mode = Some Producer}
        | "consumer" :: xs -> parseCommandLine xs {optionsSoFar with Mode = Some Consumer}
        | _ -> failwithf "Wrong arguments %A" args

    parseCommandLine (List.ofSeq argv) defaultOptions

let schemaRegestryCfg opt = 
    SchemaRegistryConfig(Url = opt.SchemaRegistryUrl, BasicAuthUserInfo = opt.SchemaRegistryCreds)

let startProducer opt =
    let cfg = 
        ProducerConfig(
            BootstrapServers = opt.BootstrapServers,
            SaslMechanism = Nullable SaslMechanism.Plain,
            SecurityProtocol = Nullable SecurityProtocol.SaslSsl,
            SslCaLocation = "./cacert.pem",
            SaslUsername = opt.Username,
            SaslPassword = opt.Password)

    use schemaRegistry = new CachedSchemaRegistryClient(schemaRegestryCfg opt)
    use producer = 
        ProducerBuilder<string, Domain.Msg>(cfg)
            .SetValueSerializer(
                ExampleSerializer<Domain.Msg>(
                    SubjectNameStrategy.Topic.ConstructValueSubjectName(opt.Topic), schemaRegistry))
            .SetErrorHandler(fun _ err -> printfn "PRODUCER ERROR: %A " err)
            .Build()

    while true do
        Console.Write("Enter message: ")
        let txt = Console.ReadLine()

        (task {
            try
                let msg:Domain.Msg = {Id = 123; Cmd = Domain.Text txt} 
                let! res = producer.ProduceAsync(opt.Topic, Message(Key="123", Value=msg))
                printfn "produced to: %s" (res.TopicPartitionOffset.ToString())
            with
            | ex -> printfn "error during producing a message: %s" ex.Message
        }).Wait()

let startConsumer opt =
    let cfg = 
        ConsumerConfig(
            BootstrapServers = opt.BootstrapServers,
            SaslMechanism = Nullable SaslMechanism.Plain,
            SecurityProtocol = Nullable SecurityProtocol.SaslSsl,
            SslCaLocation = "./cacert.pem",
            SaslUsername = opt.Username,
            SaslPassword = opt.Password,
            GroupId = "Avro.Fsharp",
            AutoOffsetReset = Nullable AutoOffsetReset.Earliest)
    
    use schemaRegistry = new CachedSchemaRegistryClient(schemaRegestryCfg opt)

    use consumer = 
        ConsumerBuilder<string, Domain.Msg>(cfg)
            .SetValueDeserializer(ExampleDeserializer<Domain.Msg>(schemaRegistry))
            .SetErrorHandler(fun _ err -> printfn "CONSUMER ERROR: %s" (err.ToString()))
            .Build()

    consumer.Subscribe(opt.Topic)

    while true do
        try
            let res = consumer.Consume()
            printfn "consumed: %A" res.Message.Value
        with
        | ex -> printfn "consume error: %s" ex.Message

[<EntryPoint>]
let main argv =

    match argv with
    | [||] ->
        printfn """Kafka Serialization/Deserialization Example (confluent cloud version)
        USAGE:
            KafkaSerde.dll MODE -t <topic name> -b <bootstrap servers> -u <username> -p <password> -r <schema registry url> -rcreds <user:password>
            Possible MODEs:
                producer
                consumer
        """
    | _ -> 
        let options = parseArgs argv
        
        printfn "Starting KafkaSerde with such options: %A" options

        match options with
        | {Mode = Some Producer} -> startProducer options
        | {Mode = Some Consumer} -> startConsumer options
        | _ -> failwith "Mode is not specified"

    0 // return an integer exit code