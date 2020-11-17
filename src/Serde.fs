module Avro.FSharp.Serde

open System
open System.IO
open System.Text
open System.Text.Json

let defaultSerializerOptions:SerializationOptions = {CustomRules = CustomRule.buidInRules}

let defaultDeserializerOptions:DeserializationOptions = {CustomRules = CustomRule.buidInRules; EvolutionTolerantMode = true}

let jsonSerializer (options:SerializationOptions) (type':Type) (schema:Schema) =
    let tr = TypeRecord.create type'
    let factory = TypeFactory(tr, schema, (fun _ _ _ -> null))
    let director = InstanceDirector.create options tr schema factory
    fun (instance:obj) (writer) ->
        let builder = JsonBuilder(writer)
        director instance builder

let binarySerializer (options:SerializationOptions) (type':Type) (schema:Schema) =
    let tr = TypeRecord.create type'
    let factory = TypeFactory(tr, schema, (fun _ _ _ -> null))
    let director = InstanceDirector.create options tr schema factory
    fun (instance:obj) (stream:Stream) ->
        use writer = new BinaryWriter(stream, Text.Encoding.UTF8)
        let builder = BinaryBuilder(writer)
        director instance builder

let createDefaultValue options (tr:TypeRecord) (schema:Schema) (el:JsonElement)  =
        let director = JsonDirector.create()
        let factory = TypeFactory(tr, schema, (fun _ _ _ -> null))

        use stream = new MemoryStream()
        use jsonWriter = new Utf8JsonWriter(stream)
        match schema with
        | Union schemas ->
            match schemas.[0] with
            | Record schema ->
                jsonWriter.WriteStartObject()
                jsonWriter.WritePropertyName(schema.Name)
                el.WriteTo jsonWriter
                jsonWriter.WriteEndObject()
            | _ -> el.WriteTo jsonWriter
        | _ -> el.WriteTo jsonWriter
        jsonWriter.Flush()
        stream.Position <- 0L

        let builder = InstanceBuilder(options, factory)
        director stream builder
        builder.Instance

let jsonDeserializer (options:DeserializationOptions) (type':Type) (schema:Schema)  =
    let tr = TypeRecord.create type'
    let director = JsonDirector.create()
    let factory = TypeFactory(tr, schema, createDefaultValue options)
    fun (stream:Stream) ->
        let builder = InstanceBuilder(options, factory)
        director stream builder
        builder.Instance

let binaryDeserializer (options:DeserializationOptions) (type':Type) (readerSchema:Schema)   =
    let tr = TypeRecord.create type'
    let director = BinaryDirector.create()
    let factory = TypeFactory(tr, readerSchema, createDefaultValue options)
    fun (writerSchema: Schema) (stream:Stream) ->
        let builder = InstanceBuilder(options, factory)
        use reader = new BinaryReader(stream, Text.Encoding.UTF8)
        director reader writerSchema builder
        builder.Instance