namespace Examples.KafkaSerde

open System.IO
open System.Threading.Tasks
open Confluent.Kafka
open Confluent.SchemaRegistry
open Avro.FSharp

type ExampleSerializer<'T> (schemaSubject:string, schemaRegistryClient: ISchemaRegistryClient) =
    let schema =
        match Schema.generate Schema.defaultOptions typeof<'T> with
        | Ok schema -> schema
        | Error err -> failwithf "Schema error: %A" err

    let serializer = Serde.binarySerializer Serde.defaultSerializerOptions typeof<'T> schema

    let kafkaSchema = Schema(schema |> Schema.toString, SchemaType.Avro)
    let schemaId = schemaRegistryClient.RegisterSchemaAsync(schemaSubject, kafkaSchema).Result

    let prefix = [|
        0uy // magic Byte
        let i = System.Net.IPAddress.HostToNetworkOrder(schemaId)
        byte i; byte (i >>> 8); byte (i >>> 16); byte (i >>> 24) // shema id
    |]

    interface IAsyncSerializer<'T> with
        member _.SerializeAsync(data: 'T, _: SerializationContext): Task<byte []> =
            Task.Run(fun () ->
                use stream = new MemoryStream(1024)
                stream.Write(prefix, 0, 5)
                serializer data stream
                stream.ToArray())