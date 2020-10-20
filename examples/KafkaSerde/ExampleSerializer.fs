namespace Examples.KafkaSerde

open System.IO
open System.Threading.Tasks
open Confluent.Kafka
open Confluent.SchemaRegistry
open Avro.FSharp

type ExampleSerializer<'T> (schemaSubject:string, schemaRegistryClient: ISchemaRegistryClient) =
    let writer,schema = 
        match Schema.generateWithReflector [] typeof<'T> with
        | Ok (schema,reflector) -> FSharpWriter<'T>(Avro.Schema.Parse(schema |> Schema.toString), reflector),schema
        | Error err -> failwithf "SchemaError: %A" err

    let schemaId = schemaRegistryClient.RegisterSchemaAsync(schemaSubject, Schema(schema |> Schema.toString, SchemaType.Avro)).Result

    let prefix = [|
        0uy // magic Byte
        let i = System.Net.IPAddress.HostToNetworkOrder(schemaId)
        byte i; byte (i >>> 8); byte (i >>> 16); byte (i >>> 24) // shema id 
    |]

    interface IAsyncSerializer<'T> with
        member _.SerializeAsync(data: 'T, _: SerializationContext): Task<byte []> = 
            Task.Run(fun () -> 
                use writerStream = new MemoryStream(1024)
                writerStream.Write(prefix, 0, 5)                
                writer.Write(data, Avro.IO.BinaryEncoder(writerStream))                
                writerStream.ToArray())