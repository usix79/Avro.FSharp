namespace Examples.KafkaSerde

#nowarn "0051"

open System
open System.IO
open System.Collections.Concurrent
open Confluent.Kafka
open Confluent.SchemaRegistry
open Avro.FSharp

type ExampleDeserializer<'T> (schemaRegistryClient: ISchemaRegistryClient) =
    let factory = InstanceFactory(typeof<'T>, [])
    let director = BinaryDirector()

    let schemasCache = ConcurrentDictionary<int, Schema>()

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

        Schema.ofString writerSchemaResult.SchemaString

    interface IDeserializer<'T> with
        member _.Deserialize(data:ReadOnlySpan<byte>, isNull:bool, context:SerializationContext): 'T =
            try
                use stream = new UnmanagedMemoryStream(&&data.GetPinnableReference(), int64 data.Length)
                let writerSchemaId = readSchemaId stream
                let writerSchema = schemasCache.GetOrAdd(writerSchemaId, getWriterSchema)
                use reader = new BinaryReader(stream)
                let builder = InstanceBuilder(factory)
                director.Construct(reader, writerSchema, builder)

                builder.Instance :?> 'T
            with
            | ex ->
                printfn "EXCEPTION %A" ex
                raise ex