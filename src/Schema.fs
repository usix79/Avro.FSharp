namespace Avro.FSharp

open System
open System.Text.Json
open System.Collections.Generic
open System.IO
open System.Text

type Schema =
    | Null
    | Boolean
    | Int
    | Long
    | Float
    | Double
    | Bytes
    | String
    | Record of RecordSchema
    | Enum of EnumSchema
    | Array of ArraySchema
    | Map of MapSchema
    | Union of Schema array
    | Fixed of FixedSchema
    | Decimal of DecimalSchema
and RecordSchema = {Name: string; Aliases: string list; Fields: List<RecordFieldSchema>}
and RecordFieldSchema = {Name: string; Aliases: string list; Type: Schema; Default: JsonElement option}
and EnumSchema = {Name: string; Aliases: string list; Symbols: string array; Default: JsonElement option}
and ArraySchema = {Items: Schema; Default: JsonElement option}
and MapSchema = {Values: Schema; Default: JsonElement option}
and FixedSchema = {Name: string; Aliases: string list; Size: int}
and DecimalSchema = {Precision: int; Scale: int}

type SchemaError =
    | AggregateError of SchemaError list
    | NotSupportedType of Type
    | NotSupportedTypeInfo of TypeInfo

type SchemaOptions = {
    CustomRules: CustomRule list
    Annotations: String
    TreatDecimalAsDouble: bool
    TreatBigIntAsString: bool
    TreatGuidAsString: bool
    StubDefaultValues: bool}

module Schema =
    let defaultOptions = {
            CustomRules = CustomRule.buidInRules
            Annotations = ""
            TreatDecimalAsDouble = false
            TreatBigIntAsString = false
            TreatGuidAsString = false
            StubDefaultValues = false}

    let private canonicalName ns (name:string) =
        match name.LastIndexOf "." with
        | -1 -> ns, if ns <> "" then ns + "." + name else name
        | idx -> name.Substring(0, idx), name

    let tryGetProp (name:string) (el:JsonElement) =
        match el.TryGetProperty name with true, el -> Some el | _ -> None

    let asArray (el:JsonElement) =
         [| for i in 0 .. el.GetArrayLength() - 1 do el.[i] |]

    let toStringSeq (el:JsonElement) =
        el |> asArray |> Seq.map (fun el -> el.GetString())

    let getName ns (el:JsonElement) =
        let ns = match tryGetProp "namespace" el with Some el -> el.GetString() | _ -> ns
        canonicalName ns (el.GetProperty("name").GetString())

    let private getFullName (el:JsonElement) =
        getName "" el |> snd

    let getAliases ns (el:JsonElement) =
        tryGetProp "aliases" el
        |> Option.map (fun el -> toStringSeq el |> List.ofSeq |> List.map ((canonicalName ns) >> snd))
        |> Option.defaultValue []

    let getDefault (el:JsonElement) =
        tryGetProp "default" el

    let getSymbols (el:JsonElement) =
        match el.TryGetProperty "symbols" with true, el -> toStringSeq el |> Array.ofSeq | _ -> [||]

    type internal Annotator(jsonString:String) =
        let doc = JsonDocument.Parse (if jsonString = "" then "{}" else jsonString)

        let field (el:JsonElement) =
            {| Name = getFullName el; Aliases = getAliases "" el; Default = getDefault el |}

        let records =
            tryGetProp "records" doc.RootElement
            |> Option.map ( fun el ->
                asArray el
                |> Array.map (fun el ->
                    {|  Name = getFullName el
                        Aliases = getAliases "" el
                        Fields =
                            tryGetProp "fields" el
                            |> Option.map (asArray >> Array.map field) |}))

        let enums =
            tryGetProp "enums" doc.RootElement
            |> Option.map ( fun el ->
                asArray el
                |> Array.map (fun el ->
                    {|  Name = getFullName el
                        Aliases = getAliases "" el
                        Default = getDefault el |}))

        let decimals =
            tryGetProp "decimals" doc.RootElement
            |> Option.map ( fun el ->
                asArray el
                |> Array.map (fun el ->
                    {|  RecordName = el.GetProperty("record").GetString()
                        FieldName = el.GetProperty("field").GetString()
                        Scale = el.GetProperty("scale").GetInt32() |}))

        member _.Enum name =
            enums |> Option.bind (Array.tryFind (fun r -> r.Name = name))

        member _.Record name =
            records |> Option.bind (Array.tryFind (fun r -> r.Name = name))

        member this.Field recordName fieldName =
            this.Record recordName
            |> Option.bind (fun r ->
                r.Fields
                |> Option.bind (Array.tryFind (fun fr -> fr.Name = fieldName)))

        member _.Decimal recordName fieldName =
            decimals |> Option.bind (Array.tryFind (fun r -> r.RecordName = recordName && r.FieldName = fieldName))

    let rec nameFromTypeInfo = function
        | TypeInfo.String -> "String"
        | TypeInfo.Bool -> "Boolean"
        | TypeInfo.Int32 -> "Int32"
        | TypeInfo.Long -> "Int64"
        | TypeInfo.Float32 -> "Float"
        | TypeInfo.Float -> "Double"
        | TypeInfo.Byte -> "Byte"
        | TypeInfo.Short -> "Short"
        | TypeInfo.UInt16 -> "UInt16"
        | TypeInfo.UInt32 -> "UInt32"
        | TypeInfo.UInt64 -> "UInt64"
        | TypeInfo.Array tr
        | TypeInfo.ResizeArray tr
        | TypeInfo.HashSet tr
        | TypeInfo.Set tr
        | TypeInfo.Seq tr
        | TypeInfo.List tr -> "Array_Of_" + nameFromTypeInfo tr.TypeInfo
        | TypeInfo.Map (_,valueTypeRecord)
        | TypeInfo.Dictionary (_, valueTypeRecord) -> "Map_Of_" + nameFromTypeInfo valueTypeRecord.TypeInfo
        | TypeInfo.Enum (type', _) -> nameFromType type'
        | TypeInfo.Record (type', _) -> nameFromType type'
        | TypeInfo.Tuple itemsRecords -> "Tuple_Of_" + ((itemsRecords |> Array.map (fun r -> nameFromTypeInfo r.TypeInfo) ) |> (String.concat "_And_"))
        | TypeInfo.Option tr -> "Nullable_" + nameFromTypeInfo tr.TypeInfo
        | TypeInfo.Union (type', _) -> nameFromType type'
        | TypeInfo.Guid -> "Guid"
        | TypeInfo.DateTime -> "DateTime"
        | TypeInfo.DateTimeOffset -> "DateTimeOffset"
        | TypeInfo.TimeSpan -> "TimeSpan"
        | TypeInfo.Decimal -> "Decimal"
        | TypeInfo.BigInt -> "BigInt"
        | TypeInfo.Any type' -> nameFromType type'
        | wrongTypeInfo -> failwithf "Name for the type is not supported: %A" wrongTypeInfo
    and nameFromType (type':Type) : string =
        let name = type'.FullName.Replace('+','.')
        let name = if name.StartsWith "System." then name.Substring("System.".Length) else name
        let name =
            match type' with
            | t when t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<Result<_,_>> -> "Result`"
            | _ -> name
        match name.IndexOf('`') with
        | -1 -> name
        | idx ->
            type'.GetGenericArguments()
            |> Array.map (fun t ->  nameFromTypeInfo (TypeRecord.create t).TypeInfo)
            |> String.concat "_And_"
            |> (sprintf "%s_Of_%s" (name.Substring(0, idx)))

    let internal stub options customRules ti =

        let rec stub = function
            | TypeInfo.String _ -> "\"\""
            | TypeInfo.Bool _ -> "false"
            | TypeInfo.Byte _
            | TypeInfo.Short _
            | TypeInfo.UInt16 _
            | TypeInfo.Int32 _
            | TypeInfo.UInt32 _
            | TypeInfo.Long _
            | TypeInfo.UInt64 _
            | TypeInfo.Float32 _
            | TypeInfo.Float _ -> "0"
            | TypeInfo.Array {TypeInfo = TypeInfo.Byte} -> "\"\""
            | TypeInfo.Array _
            | TypeInfo.ResizeArray _
            | TypeInfo.HashSet _
            | TypeInfo.Set _
            | TypeInfo.Seq _
            | TypeInfo.List _ -> "[]"
            | TypeInfo.Map _
            | TypeInfo.Dictionary _ -> "{}"
            | TypeInfo.Enum (type', _) ->
                seq {for obj in Enum.GetNames type' do obj.ToString()}
                |> Seq.head
                |> fun symbol -> "\"" + symbol + "\""
            | TypeInfo.Record (_, fieldsInfo) ->
                "{" + (
                    fieldsInfo
                    |> Seq.map (fun fi -> sprintf "\"%s\": %s" fi.FieldName (stub fi.FieldType.TypeInfo))
                    |> String.concat ","
                ) + "}"
            | TypeInfo.Tuple tr ->
                "{" + (
                    tr
                    |> Array.mapi (fun idx tr -> sprintf"\"Item%d\": %s" (idx+1) (stub tr.TypeInfo))
                    |> String.concat ","
                ) + "}"
            | TypeInfo.Option _ -> "null"
            | TypeInfo.Union (unionType, casesInfo) ->
                let case = casesInfo.[0]
                "{" + (
                    Array.zip (case.Info.GetFields()) case.CaseTypes
                    |> Array.map (fun (pi,tr) -> sprintf "\"%s\": %s" pi.Name (stub tr.TypeInfo))
                    |> String.concat ","
                ) + "}"
            | TypeInfo.Any type' ->
                match customRules |> List.tryFind (fun t -> type' = t.InstanceType) with
                | Some rule -> rule.StubValue
                | None -> failwithf "Can not resolve stub for: %A" type'
            | TypeInfo.Decimal _ -> if options.TreatDecimalAsDouble then "0" else "\"\""
            | TypeInfo.BigInt _ ->  if options.TreatBigIntAsString then "\"0\"" else "\"\""
            | TypeInfo.Guid _ ->
                if options.TreatGuidAsString then "\"00000000-0000-0000-0000-000000000000\""
                else "\"\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\""
            | TypeInfo.DateTime _ -> "\"1970-01-01T00:00:00.000Z\""
            | TypeInfo.DateTimeOffset _ -> "\"1970-01-01T00:00:00.000+00:00\""
            | TypeInfo.TimeSpan _ -> "0"
            | wrong -> failwithf "Can not resolve stub for: %A" wrong

        stub ti |> Avro.FSharp.Utils.toJsonElement

    let ofString (jsonString:string) =
        let doc = JsonDocument.Parse jsonString
        let cache = Dictionary<string,Schema>()

        let rec parse ns (el:JsonElement) =
            match el.ValueKind with
            | JsonValueKind.Null -> Null
            | JsonValueKind.String ->
                match el.GetString() with
                | "string" -> String
                | "boolean" -> Boolean
                | "int" -> Int
                | "long" -> Long
                | "float" -> Float
                | "double" -> Double
                | "bytes" -> Bytes
                | typeName ->
                    match cache.TryGetValue(canonicalName ns typeName |> snd) with
                    | true, schema -> schema
                    | _ -> failwithf "Unknown type: %s" typeName
            | JsonValueKind.Object ->
                let typeEl = el.GetProperty("type")
                match typeEl.ValueKind with
                | JsonValueKind.String ->
                    match typeEl.GetString() with
                    | "array" -> Array { Items = el.GetProperty("items") |> parse ns; Default = getDefault el}
                    | "map" -> Map { Values = el.GetProperty("values") |> parse ns; Default = getDefault el}
                    | "enum" ->
                        let ns, name = getName ns el
                        let schema =
                            Enum {  Name = name
                                    Aliases = getAliases ns el
                                    Symbols = getSymbols el
                                    Default = getDefault el}
                        cache.[name] <- schema
                        schema
                    | "record" ->
                        let ns, name = getName ns el
                        let fields = List<RecordFieldSchema>()
                        let schema =
                            Record {    Name = name
                                        Aliases = getAliases ns el
                                        Fields = fields}
                        cache.[name] <- schema
                        let fieldsEl = el.GetProperty("fields")
                        for i in 0 .. fieldsEl.GetArrayLength() - 1 do
                            let el = fieldsEl.[i]
                            {   Name = el.GetProperty("name").GetString()
                                Aliases = getAliases "" el
                                Type = el.GetProperty("type") |> parse ns
                                Default = getDefault el}
                            |> fields.Add
                        schema
                    | "fixed" ->
                        let ns, name = getName ns el
                        let schema =
                            Fixed { Name = name
                                    Aliases = getAliases ns el
                                    Size = el.GetProperty("size").GetInt32()}
                        cache.[name] <- schema
                        schema
                    | _ ->
                        match el.TryGetProperty "logicalType" with
                        | true, logicalTypeEl when logicalTypeEl.GetString() = "decimal" ->
                            Decimal {Precision = el.GetProperty("precision").GetInt32(); Scale = el.GetProperty("scale").GetInt32()}
                        | _ -> parse ns typeEl
                | _ -> parse ns typeEl
            | JsonValueKind.Array ->
                seq{for i in 0 .. el.GetArrayLength() - 1 do parse ns el.[i]} |> Array.ofSeq |> Union
            | wrong -> failwithf "not supported kind%A" wrong

        parse "" doc.RootElement

    let generate (options:SchemaOptions) (type':Type) : Result<Schema,SchemaError> =
        let cache = Dictionary<string, Schema>()
        let annotator = Annotator(options.Annotations)
        let typeRecord = TypeRecord.create type'
        let customRules = options.CustomRules

        let rec gen (ti:TypeInfo) : Result<Schema,SchemaError> =
            match ti with
            | TypeInfo.String _ -> String |> Ok
            | TypeInfo.Bool _ -> Boolean |> Ok
            | TypeInfo.Byte _ -> Int |> Ok
            | TypeInfo.Short _ -> Int |> Ok
            | TypeInfo.UInt16 _ -> Int |> Ok
            | TypeInfo.Int32 _ -> Int |> Ok
            | TypeInfo.UInt32 _ -> Int |> Ok
            | TypeInfo.Long _ -> Long |> Ok
            | TypeInfo.UInt64 _ -> Long |> Ok
            | TypeInfo.Float32 _ -> Float |> Ok
            | TypeInfo.Float _ -> Double |> Ok
            | TypeInfo.Array {TypeInfo = TypeInfo.Byte} -> Bytes |> Ok
            | TypeInfo.Array tr
            | TypeInfo.ResizeArray tr
            | TypeInfo.Set tr
            | TypeInfo.HashSet tr
            | TypeInfo.Seq tr
            | TypeInfo.List tr ->
                gen tr.TypeInfo |> Result.map (fun schema -> Array {Items = schema; Default = None })
            | TypeInfo.Map (_, valueTypeRecord)
            | TypeInfo.Dictionary (_, valueTypeRecord) ->
                gen valueTypeRecord.TypeInfo |> Result.map (fun schema ->  Map {Values = schema; Default = None })
            | TypeInfo.Enum (type', _) ->
                let name = nameFromTypeInfo ti
                let schema =
                    Enum {  Name = name
                            Aliases = annotator.Enum name |> Option.map (fun r -> r.Aliases) |> Option.defaultValue []
                            Symbols = [|for obj in Enum.GetNames type' do obj.ToString()|]
                            Default =
                                annotator.Enum name
                                |> Option.bind (fun r -> r.Default)
                                |> Option.orElseWith(fun () -> if options.StubDefaultValues then stub options customRules ti |> Some else None)}
                cache.[name] <- schema
                schema |> Ok
            | TypeInfo.Record (type', fieldsInfo) ->
                genRecord
                    (nameFromTypeInfo ti)
                    (fieldsInfo
                        |> Seq.map (fun fi -> fi.FieldName,fi.FieldType.TypeInfo)
                        |> List.ofSeq)
            | TypeInfo.Tuple itemsTypeRecord ->
                genRecord
                    (nameFromTypeInfo ti)
                    (itemsTypeRecord
                        |> Array.mapi (fun idx tr -> sprintf"Item%d" (idx+1),tr.TypeInfo)
                        |> List.ofArray)
            | TypeInfo.Option tr ->
                gen tr.TypeInfo
                |> Result.bind (fun someSchema -> Union [|Null; someSchema|] |> Ok)
            | TypeInfo.Union (type', casesInfo) ->
                casesInfo
                |> List.ofSeq
                |> Avro.FSharp.Utils.traverse (fun case ->
                    genRecord
                        (nameFromType type' + "." + case.CaseName)
                        (Array.zip (case.Info.GetFields()) case.CaseTypes
                            |> Array.map (fun (pi,tr) -> pi.Name,tr.TypeInfo)
                            |> List.ofArray))
                |> Result.map (Array.ofList >> Union)
                |> Result.mapError AggregateError
            | TypeInfo.Decimal _ -> Ok (if options.TreatDecimalAsDouble then Double else Decimal {Precision=29; Scale=14})
            | TypeInfo.BigInt _ -> Ok(if options.TreatBigIntAsString then String else Bytes)
            | TypeInfo.Guid _ -> Ok(if options.TreatGuidAsString then String else Fixed {Name="guid"; Size=16; Aliases=[]})
            | TypeInfo.DateTime _ -> String |> Ok
            | TypeInfo.DateTimeOffset _ -> String |> Ok
            | TypeInfo.TimeSpan _ -> Int |> Ok
            | TypeInfo.Any type' ->
                match customRules |> List.tryFind (fun t -> type' = t.InstanceType) with
                | Some rule -> TypeRecord.create rule.SurrogateType |> (fun tr -> gen tr.TypeInfo)
                | None -> NotSupportedType type' |> Error
            | ti -> NotSupportedTypeInfo ti |> Error
        and genRecord recordName (fieldsInfo:(string*TypeInfo) list) =
            match cache.TryGetValue recordName with
            | true, schema -> schema |> Ok
            | _ ->
                let fields = ResizeArray<RecordFieldSchema>()
                let schema =
                    Record { Name = recordName
                             Aliases =
                                annotator.Record recordName
                                |> Option.map (fun r -> r.Aliases)
                                |> Option.defaultValue []
                             Fields = fields}
                cache.[recordName] <- schema  // create schema in advance for using it in recursive types

                fieldsInfo
                |> Avro.FSharp.Utils.traverse (fun (fieldName,typeInfo) ->
                    gen typeInfo
                    |> Result.map (fun fieldSchema ->
                        let defValue =
                            if options.StubDefaultValues then stub options customRules typeInfo |> Some else None

                        let fieldSchema =
                            match fieldSchema with
                            | Decimal schema ->
                                match annotator.Decimal recordName fieldName with
                                | Some a -> Decimal {schema with Scale = a.Scale}
                                | _ -> fieldSchema
                            | _ -> fieldSchema

                        match annotator.Field recordName fieldName with
                        | Some a -> { Name = fieldName; Aliases = a.Aliases; Type = fieldSchema; Default = a.Default |> Option.orElse defValue}
                        | None -> { Name = fieldName; Aliases = []; Type = fieldSchema; Default = defValue}))
                |> Result.map (fun recordFields -> fields.AddRange(recordFields); schema)
                |> Result.mapError AggregateError

        gen typeRecord.TypeInfo

    let private toString' (isCanonical:bool) (schema:Schema) =
        use stream = new MemoryStream()
        use writer = new Utf8JsonWriter(stream)
        let cache = Dictionary<string,Schema>()

        let writeType (typeName:string) = writer.WriteString("type", typeName)
        let writeName (name:string) = writer.WriteString("name", name)
        let writeArray (name:string) = function
            | [||] -> ()
            | (values:string array) ->
                writer.WritePropertyName(name)
                writer.WriteStartArray()
                values |> Array.iter writer.WriteStringValue
                writer.WriteEndArray()
        let writeAliases = if isCanonical then ignore else Array.ofList >> (writeArray "aliases")
        let writeDefault =
            if isCanonical then ignore
            else Option.iter (fun (el:JsonElement) ->
                writer.WritePropertyName "default"
                el.WriteTo writer)

        let rec write = function
            | Union cases ->
                writer.WriteStartArray()
                cases |> Array.iter write
                writer.WriteEndArray()
            | Null -> writer.WriteStringValue "null"
            | Boolean -> writer.WriteStringValue "boolean"
            | Int -> writer.WriteStringValue "int"
            | Long -> writer.WriteStringValue "long"
            | Float -> writer.WriteStringValue "float"
            | Double -> writer.WriteStringValue "double"
            | Bytes -> writer.WriteStringValue "bytes"
            | String -> writer.WriteStringValue "string"
            | Array schema ->
                writer.WriteStartObject()
                writeType "array"
                writer.WritePropertyName "items"
                write schema.Items
                writeDefault schema.Default
                writer.WriteEndObject()
            | Map schema ->
                writer.WriteStartObject()
                writeType "map"
                writer.WritePropertyName "values"
                write schema.Values
                writeDefault schema.Default
                writer.WriteEndObject()
            | Enum schema when cache.ContainsKey schema.Name -> writer.WriteStringValue schema.Name
            | Enum schema ->
                writer.WriteStartObject()
                writeType "enum"
                writeName schema.Name
                writeAliases schema.Aliases
                writeArray "symbols" schema.Symbols
                writeDefault schema.Default
                writer.WriteEndObject()
                cache.[schema.Name] <- Enum schema
            | Record schema when cache.ContainsKey schema.Name -> writer.WriteStringValue schema.Name
            | Record schema ->
                cache.[schema.Name] <- Record schema
                writer.WriteStartObject()
                writeType "record"
                writeName schema.Name
                writeAliases schema.Aliases
                writer.WritePropertyName "fields"
                writer.WriteStartArray()
                for field in schema.Fields do
                    writer.WriteStartObject()
                    writeName field.Name
                    writeAliases field.Aliases
                    writer.WritePropertyName "type"
                    write field.Type
                    writeDefault field.Default
                    writer.WriteEndObject()
                writer.WriteEndArray()
                writer.WriteEndObject()
                cache.[schema.Name] <- Record schema
            | Fixed schema when cache.ContainsKey schema.Name -> writer.WriteStringValue schema.Name
            | Fixed schema ->
                writer.WriteStartObject()
                writeType "fixed"
                writeName schema.Name
                writeAliases schema.Aliases
                writer.WriteNumber("size", schema.Size)
                writer.WriteEndObject()
                cache.[schema.Name] <- Fixed schema
            | Decimal schema ->
                if isCanonical then
                    writer.WriteStringValue "bytes"
                else
                    writer.WriteStartObject()
                    writeType "bytes"
                    writer.WriteString("logicalType", "decimal")
                    writer.WriteNumber("precision", schema.Precision)
                    writer.WriteNumber("scale", schema.Scale)
                    writer.WriteEndObject()
            | _ -> ()

        write schema
        writer.Flush()
        let bytes = stream.ToArray()
        let schemaJson = Encoding.UTF8.GetString(bytes, 0, bytes.Length)

        match schema with
        | Boolean | Int | Long | Float | Double | Bytes | String -> sprintf "{\"type\": \"%s\"}" schemaJson   // root primitive types special case
        | _ -> schemaJson

    let toString = toString' false

    let toCanonicalString = toString' true