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
        member _.Bytes(v: byte array) =
            let buffer = Array.zeroCreate (v.Length * 2)
            v |> Array.iteri (fun idx b -> buffer.[idx*2] <- b)
            Encoding.Unicode.GetString buffer
            |> writer.WriteStringValue
        member _.Decimal(v: decimal, schema: DecimalSchema) = failwith "decimal is not supported in Json Encoding"
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

module JsonDirector =

    let create () =
        fun (stream:Stream) (builder:IAvroBuilderWithSchema) ->

            let rec write schema (el:JsonElement)  =
                match el.ValueKind, schema with
                | JsonValueKind.Null, Null -> builder.Null()
                | JsonValueKind.True, Boolean -> builder.Boolean true
                | JsonValueKind.False, Boolean -> builder.Boolean false
                | JsonValueKind.Number, Int -> el.GetInt32() |> builder.Int
                | JsonValueKind.Number, Long -> el.GetInt64() |> builder.Long
                | JsonValueKind.Number, Float -> el.GetSingle() |> builder.Float
                | JsonValueKind.Number, Double -> el.GetDouble() |> builder.Double
                | JsonValueKind.String, String -> el.GetString() |> builder.String
                | JsonValueKind.String, Bytes ->
                    let utf16Bytes = Encoding.Unicode.GetBytes(el.GetString())
                    let output = Array.zeroCreate (utf16Bytes.Length / 2)
                    utf16Bytes |> Array.iteri (fun idx b -> if idx % 2 = 0 then output.[idx / 2] <- b)
                    builder.Bytes output
                | JsonValueKind.String, Enum _ -> builder.Enum(-1,el.GetString())
                | JsonValueKind.Array, Array schema ->
                    builder.StartArray()
                    builder.StartArrayBlock(int64 (el.GetArrayLength()))
                    for el in el.EnumerateArray() do write schema.Items el
                    builder.EndArray()
                | JsonValueKind.Object, Map schema ->
                    builder.StartMap()
                    builder.StartMapBlock(int64 (el.EnumerateObject() |> Seq.length))
                    for field in el.EnumerateObject() do
                        builder.Key field.Name
                        write schema.Values field.Value
                    builder.EndMap()
                | JsonValueKind.Object, Record schema ->
                    builder.StartRecord()
                    el.EnumerateObject()
                    |> Seq.iteri (fun idx field ->
                        if builder.Field field.Name then
                            write builder.ExpectedValueSchema field.Value)
                    builder.EndRecord()
                | JsonValueKind.Null, Union [|Null;_|] -> builder.NoneCase()
                | JsonValueKind.Object, Union [|Null;someSchema|] ->
                    builder.StartSomeCase(someSchema)
                    let field = el.EnumerateObject() |> Seq.head
                    write someSchema field.Value
                    builder.EndSomeCase()
                | JsonValueKind.Object, Union schemas ->
                    let field = el.EnumerateObject() |> Seq.head
                    let idx = Array.FindIndex(schemas, (fun schema -> JsonHelpers.avroTypeName(schema) = field.Name))
                    if builder.StartUnionCase(idx,field.Name) then
                        match field.Value.ValueKind with
                        | JsonValueKind.Object ->
                            for field in field.Value.EnumerateObject() do
                                if builder.Field field.Name then
                                    write builder.ExpectedValueSchema field.Value
                        | wrong -> failwithf "An union should consist of objects where each object is mapped to a record, but not: %A" wrong
                        builder.EndUnionCase()

                | wrong -> failwithf "wrong combination: %A" wrong

            builder.Start()

            match builder.ExpectedValueSchema with
            | Record _ | Array _ | Map _ | Union _ -> JsonDocument.Parse(stream).RootElement
            | _ ->
                let reader = new StreamReader(stream, Encoding.UTF8)
                JsonDocument.Parse(sprintf "{\"element\": %s}" (reader.ReadToEnd())).RootElement.GetProperty("element")
            |> write builder.ExpectedValueSchema

            builder.End()