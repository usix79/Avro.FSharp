namespace Avro.FSharp

open System
open System.IO
open System.Text
open System.Text.Json

module JsonHelpers =
    let avroTypeName(schema:Schema) =
        match schema with
        | Record schema -> schema.Name
        | Array _ -> "array"
        | Map _ -> "map"
        | Enum schema -> schema.Name
        | Fixed schema -> schema.Name
        | Null -> "null"
        | Boolean -> "boolean"
        | Int -> "int"
        | Long -> "long"
        | Float -> "float"
        | Double -> "double"
        | Bytes -> "bytes"
        | String -> "string"
        | Decimal _ -> "bytes"
        | Union _ -> "union"

type JsonDirector() =

    member this.Construct(stream:Stream, builder:IAvroBuilderWithSchema) =
        builder.Start()
        match builder.ExpectedValueSchema with
        | Record _ | Array _ | Map _ | Union _ ->
            let doc = JsonDocument.Parse(stream)
            this.ConstructObject(doc.RootElement, builder)
        | _ ->
            let reader = new StreamReader(stream, Encoding.UTF8)
            this.ConstructPrimitive(reader.ReadToEnd(), builder)
        builder.End()

    member this.Construct(doc:JsonDocument, builder:IAvroBuilderWithSchema) =
        builder.Start()
        this.ConstructObject(doc.RootElement, builder)
        builder.End()

    member private _.ConstructPrimitive(value':string, builder:IAvroBuilderWithSchema) =
        match builder.ExpectedValueSchema with
        | Null -> builder.Null()
        | Boolean -> builder.Boolean(Boolean.Parse value')
        | Int -> builder.Int(Int32.Parse value')
        | Long -> builder.Long(Int64.Parse value')
        | Float -> builder.Float(Single.Parse(value', Globalization.NumberStyles.Any))
        | Double -> builder.Double(Double.Parse(value', Globalization.NumberStyles.Any))
        | Bytes -> builder.Bytes(Convert.FromBase64String(value'.Trim([|'"'|])))
        | String -> builder.String(value'.Trim([|'"'|]))
        | Enum schema ->
            let symbol = value'.Trim([|'"'|])
            let idx = Array.IndexOf(schema.Symbols, symbol)
            builder.Enum(idx,symbol)
        | Decimal schema -> builder.Decimal(Decimal.Parse(value', Globalization.NumberStyles.Any), schema)
        | Fixed schema -> builder.Bytes(Convert.FromBase64String(value'.Trim([|'"'|])))
        | schema -> failwithf "Unexpected primitive schema: %A" schema

    member private _.ConstructObject(el:JsonElement, builder:IAvroBuilderWithSchema) =

        let rec write (el:JsonElement) (schema:Schema) =
            match el.ValueKind, schema with
            | JsonValueKind.Null, Null -> builder.Null()
            | JsonValueKind.True, Boolean -> builder.Boolean true
            | JsonValueKind.False, Boolean -> builder.Boolean false
            | JsonValueKind.Number, Int -> el.GetInt32() |> builder.Int
            | JsonValueKind.Number, Long -> el.GetInt64() |> builder.Long
            | JsonValueKind.Number, Float -> el.GetSingle() |> builder.Float
            | JsonValueKind.Number, Double -> el.GetDouble() |> builder.Double
            | JsonValueKind.Number, Decimal schema -> builder.Decimal(el.GetDecimal(), schema)
            | JsonValueKind.String, String -> el.GetString() |> builder.String
            | JsonValueKind.String, Bytes -> el.GetBytesFromBase64() |> builder.Bytes
            | JsonValueKind.String, Fixed _ -> el.GetBytesFromBase64() |> builder.Fixed
            | JsonValueKind.String, Enum schema ->
                let symbol = el.GetString()
                let idx = Array.IndexOf(schema.Symbols, symbol)
                builder.Enum(idx,symbol)
            | JsonValueKind.Object, Record _ ->
                builder.StartRecord()
                for field in el.EnumerateObject() do
                    if builder.Field field.Name then
                        write field.Value builder.ExpectedValueSchema
                builder.EndRecord()
            | JsonValueKind.Array, Array _ ->
                builder.StartArray()
                builder.StartArrayBlock(int64 (el.GetArrayLength()))
                for el in el.EnumerateArray() do write el builder.ExpectedValueSchema
                builder.EndArray()
            | JsonValueKind.Object, Map _ ->
                builder.StartMap()
                builder.StartMapBlock(int64 (el.EnumerateObject() |> Seq.length))
                for field in el.EnumerateObject() do
                    builder.Key field.Name
                    write field.Value builder.ExpectedValueSchema
                builder.EndMap()
            | JsonValueKind.Null, Union [|Null;_|] -> builder.NoneCase()
            | _, Union [|Null;someSchema|] ->
                builder.StartSomeCase(someSchema)
                let field = el.EnumerateObject() |> Seq.head
                write field.Value someSchema
                builder.EndSomeCase()
            | JsonValueKind.Object, Union schemas ->
                let field = el.EnumerateObject() |> Seq.head
                let idx = Array.FindIndex(schemas, (fun schema -> JsonHelpers.avroTypeName(schema) = field.Name))
                if builder.StartUnionCase(idx,field.Name) then
                    match field.Value.ValueKind with
                    | JsonValueKind.Object ->
                        for field in field.Value.EnumerateObject() do
                            if builder.Field field.Name then
                                write field.Value builder.ExpectedValueSchema
                    | wrong -> failwithf "An union should consist of objects where each object is mapped to a record, but not: %A" wrong
                    builder.EndUnionCase()
            | wrong -> failwithf "not supported combination %A" wrong

        write el builder.ExpectedValueSchema

type JsonBuilder(writer:Utf8JsonWriter) =
    interface IAvroBuilder with

        member _.Start() = ()
        member _.Null() = writer.WriteNullValue()
        member _.Boolean(v: bool) = writer.WriteBooleanValue v
        member _.Int(v: int) = writer.WriteNumberValue v
        member _.Long(v: int64) = writer.WriteNumberValue v
        member _.Float(v: float32) = writer.WriteNumberValue v
        member _.Double(v: float) = writer.WriteNumberValue v
        member _.String(v: string) = writer.WriteStringValue v
        member _.Bytes(v: byte array) = writer.WriteBase64StringValue(ReadOnlySpan<byte>(v))
        member _.Decimal(v: decimal, _: DecimalSchema) = writer.WriteNumberValue v

        member _.Enum(idx:int, v: string) = writer.WriteStringValue v
        member _.StartRecord() = writer.WriteStartObject()
        member _.Field(name: string) = writer.WritePropertyName name; true
        member _.EndRecord() = writer.WriteEndObject()
        member _.StartArray() = writer.WriteStartArray()
        member _.StartArrayBlock(size:int64) = ()
        member _.EndArray() = writer.WriteEndArray()
        member _.StartMap() = writer.WriteStartObject()
        member _.StartMapBlock(size:int64) = ()
        member _.Key(key: string) = writer.WritePropertyName key
        member _.EndMap() = writer.WriteEndObject()

        member _.StartUnionCase(idx:int,caseName:string) =
            writer.WriteStartObject()
            writer.WritePropertyName caseName
            true
        member _.EndUnionCase() = writer.WriteEndObject()

        member _.NoneCase() = writer.WriteNullValue()
        member _.StartSomeCase(schema:Schema) =
            writer.WriteStartObject()
            writer.WritePropertyName(JsonHelpers.avroTypeName schema)
        member _.EndSomeCase() = writer.WriteEndObject()

        member _.Fixed(v: byte array) = writer.WriteBase64StringValue(ReadOnlySpan<byte>(v))

        member _.End() = writer.Flush()