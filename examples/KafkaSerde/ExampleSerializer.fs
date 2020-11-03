namespace Examples.KafkaSerde

open System.IO
open System.Threading.Tasks
open Confluent.Kafka
open Confluent.SchemaRegistry
open Avro.FSharp

type ExampleSerializer<'T> (schemaSubject:string, schemaRegistryClient: ISchemaRegistryClient) =
    let factory = InstanceFactory(typeof<'T>, []) :> IInstanceFactory
    let director = InstanceDirector(factory)

    let schema = Schema(factory.TargetSchema |> Schema.toString, SchemaType.Avro)
    let schemaId = schemaRegistryClient.RegisterSchemaAsync(schemaSubject, schema).Result

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
                use writer = new BinaryWriter(stream, System.Text.Encoding.UTF8)
                let builder = BinaryBuilder(writer)
                director.Construct(data, builder)
                stream.ToArray())