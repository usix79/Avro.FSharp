namespace Examples.KafkaSerde

#nowarn "0051"

open System
open System.IO
open System.Collections.Concurrent
open Confluent.Kafka
open Confluent.SchemaRegistry
open Avro.FSharp

type ExampleDeserializer<'T> (schemaRegistryClient: ISchemaRegistryClient) =
    let readerSchema, reflector = 
        match Schema.generateSchemaAndReflector [||] typeof<'T> with
        | Ok (schema,reflector) -> Avro.Schema.Parse(schema.ToString()), reflector
        | Error err -> failwithf "SchemaError: %A" err

    let readersCache = ConcurrentDictionary<int, FSharpReader<'T>>()

    let readSchemaId (stream:Stream) =
        let magicByte = stream.ReadByte()
        if  magicByte <> 0 then
            failwithf "Expecting data with Confluent Schema Registry framing. Magic byte was %d" magicByte

        let b1 = stream.ReadByte()
        let b2 = stream.ReadByte()
        let b3 = stream.ReadByte()
        let b4 = stream.ReadByte()
        System.Net.IPAddress.NetworkToHostOrder(b1 ||| (b2 <<< 8) ||| (b3 <<< 16) ||| (b4 <<< 24))

    let getWriterSchema(writerSchemaId:int) =
        let writerSchemaResult = schemaRegistryClient.GetSchemaAsync(writerSchemaId).Result
        if writerSchemaResult.SchemaType <> SchemaType.Avro then
            failwithf "Expecting writer schema to have type Avro, not %A" writerSchemaResult.SchemaType
        
        let writerSchema = Avro.Schema.Parse(writerSchemaResult.SchemaString)
        FSharpReader<'T>(writerSchema, readerSchema, reflector)

    interface IDeserializer<'T> with
        member _.Deserialize(data:ReadOnlySpan<byte>, isNull:bool, context:SerializationContext): 'T =             
            try
                use stream = new UnmanagedMemoryStream(&&data.GetPinnableReference(), int64 data.Length)
                let writerSchemaId = readSchemaId stream
                let reader = readersCache.GetOrAdd(writerSchemaId, getWriterSchema)
                reader.Read(Unchecked.defaultof<'T>, Avro.IO.BinaryDecoder(stream))
            with 
            | ex -> 
                printfn "EXCEPTION %A" ex
                raise ex