namespace Avro.FSharp

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Text.Json
open System.Reflection
open FSharp.Reflection
open Avro.FSharp.Annotations

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
and RecordSchema = {Name: string; Aliases: string list; Fields: List<RecordField>} 
and RecordField = {Name: string; Aliases: string list; Type: Schema; Default: string option} 
and EnumSchema = {Name: string; Aliases: string list; Symbols: string array; Default: string option} 
and ArraySchema = {Items: Schema; Default: string option}
and MapSchema = {Values: Schema; Default: string option}
and FixedSchema = {Name: string; Aliases: string list; Size: int}
and DecimalSchema = {Precision: int; Scale: int}

type SchemaError = 
    | AggregateError of SchemaError list
    | NotSupportedType of Type

type SchemaCacheKey =
    | EnumCacheKey of Type
    | RecordCacheKey of Type
    | UnionCaseCacheKey of unionType:Type * caseName:string
    | ArrayCacheKey of Type
    | MapCacheKey of Type
    | CustomCacheKey of Type
    | NullableCacheKey of Type

type Cache<'TKey,'TValue when 'TKey : equality>() =
    inherit Dictionary<'TKey, 'TValue>()
    member this.AddSchema key schema = this.[key] <- schema; schema
    member this.TryFindSchema key = match this.TryGetValue key with true, schema -> Some schema | _ -> None

type SchemaCache = Cache<SchemaCacheKey,Schema>

module Schema =

    let private getOrCreate (cache:SchemaCache) key creator =
        match cache.TryFindSchema key with
        | Some schema -> schema |> Ok
        | None -> creator key cache |> Result.map (cache.AddSchema key)

    let private (|AsEnumerable|_|) (type':Type) =
        type'.GetInterfaces()
        |> Array.tryFind (fun it -> it.IsGenericType && it.GetGenericTypeDefinition() = typedefof<IEnumerable<_>>)

    let private (|AsOption|_|) (type':Type) =
        if type'.IsGenericType && type'.GetGenericTypeDefinition() = typedefof<Option<_>> 
        then type'.GetGenericArguments().[0] |> Some
        else None

    let rec private typeName (type':Type) : string =
        let name = match type' with AsOption _  -> "Nullable`" | _ -> type'.FullName.Replace('+','.')

        match name.IndexOf('`') with
        | -1 -> name
        | idx -> 
            type'.GetGenericArguments()
            |> Array.map (fun t -> (typeName t).Replace('.','_'))
            |> String.concat "_And_"
            |> (sprintf "%s_Of_%s" (name.Substring(0, idx)))

    let private canonicalName ns (name:string) =
        match name.LastIndexOf "." with
        | -1 -> ns, if ns <> "" then ns + "." + name else name
        | idx -> name.Substring(0, idx), name

    let private getAliases (type':Type) =
        match type'.GetCustomAttribute(typeof<AliasesAttribute>) with :? AliasesAttribute as attr -> attr.Aliases |> List.ofArray | _ -> []

    let private getDefaultValue (type':Type) =
        match type'.GetCustomAttribute(typeof<DefaultValueAttribute>) with :? DefaultValueAttribute as attr -> Some attr.Value | _ -> None

    let private createEnumSchema (type':Type) =
        Enum {
            Name = typeName type'
            Aliases = getAliases type'
            Symbols = Enum.GetNames(type')
            Default = getDefaultValue type'}

    let private createMapSchema (type':Type) itemsSchema =
        Map {Values = itemsSchema; Default = getDefaultValue type'}

    let private createArraySchema (type':Type) itemsSchema =
        Array {Items = itemsSchema; Default = getDefaultValue type'}

    let private createRecordSchema (key:SchemaCacheKey) fields =
        let type',name =
            match key with
            | RecordCacheKey type' -> type', typeName type'
            | UnionCaseCacheKey (type',caseName) -> type', typeName type' + "." + caseName
            | _ -> failwithf "Not supported key for record scheme: %A" key
        
        Record {Name = name; Aliases = getAliases type'; Fields = fields}

    let private splitResults<'Tag> (results:('Tag*Result<Schema,SchemaError>) array) =
        Array.foldBack (fun  (tag,result) (okeys, errors)-> 
            match result with
            | Ok result -> ((tag, result) :: okeys), errors
            | Error err -> okeys, (err :: errors)
            ) results ([],[])

    let private recordFieldInfo (pi:PropertyInfo) =
        let getAliasesOfPI (pi:PropertyInfo) = match pi.GetCustomAttribute(typeof<AliasesAttribute>) with :? AliasesAttribute as attr -> attr.Aliases | _ -> [||]
        let getDefaultOfPI (pi:PropertyInfo) = match pi.GetCustomAttribute(typeof<DefaultValueAttribute>) with :? DefaultValueAttribute as attr -> Some attr.Value | _ -> None
        let getScaleOfPI (pi:PropertyInfo) = match pi.GetCustomAttribute(typeof<ScaleAttribute>) with :? ScaleAttribute as attr -> Some attr.Scale | _ -> None
        
        {| Name = pi.Name; Aliases = getAliasesOfPI pi; Default = getDefaultOfPI pi; Scale = getScaleOfPI pi; Type = pi.PropertyType |}

    let private getRecordFieldsInfo (type':Type) = FSharpType.GetRecordFields type' |> Array.map recordFieldInfo

    let private getUnionCaseFieldsInfo (uci: UnionCaseInfo) = uci.GetFields() |> Array.map recordFieldInfo

    let private getTupleFieldsInfo (type': Type) =
        FSharpType.GetTupleElements type'
        |> Array.mapi (fun idx t -> {| Name = sprintf"Item%d" (idx+1); Aliases = [||]; Default = None; Scale = None; Type = t |})

    let rec private genSchema (type':Type) (cache:Cache<SchemaCacheKey,Schema>) : Result<Schema,SchemaError> =
        match type' with
        | t when t = typeof<string> -> String |> Ok
        | t when t = typeof<bool> -> Boolean |> Ok
        | t when t = typeof<int> -> Int |> Ok
        | t when t = typeof<int64> -> Long |> Ok
        | t when t = typeof<float32> -> Float |> Ok
        | t when t = typeof<float> -> Double |> Ok
        | t when t = typeof<byte array> -> Bytes |> Ok
        | t when t = typeof<decimal> -> Decimal {Precision=29; Scale=14} |> Ok
        | t when t.IsEnum -> getOrCreate cache (EnumCacheKey t) (fun _ _ -> createEnumSchema t |> Ok)
        | AsEnumerable it ->
            let itemType = it.GetGenericArguments().[0]
            if itemType.IsGenericType && itemType.GetGenericTypeDefinition() = typedefof<KeyValuePair<_,_>> 
            then
                let mapValueType = itemType.GetGenericArguments().[1]
                getOrCreate cache (MapCacheKey type') (fun _ -> genSchema mapValueType >> Result.map (createMapSchema type'))
            else
                let arrayItemType = it.GetGenericArguments().[0]
                getOrCreate cache (ArrayCacheKey type') (fun _ -> genSchema arrayItemType >> Result.map (createArraySchema type'))
        | t when FSharpType.IsRecord t -> getOrCreate cache (RecordCacheKey t) (getRecordFieldsInfo t |> genRecordSchema) 
        | t when FSharpType.IsTuple t -> getOrCreate cache (RecordCacheKey t) (getTupleFieldsInfo t |> genRecordSchema)
        | t when FSharpType.IsUnion t ->
            match t with
            | AsOption someType -> 
                genSchema someType cache 
                |> Result.bind (fun someSchema ->
                    (fun _ _ -> Union [|Null; someSchema|] |> Ok)
                    |> getOrCreate cache (NullableCacheKey t))                
            | _ ->
                let schemas, errors =
                    FSharpType.GetUnionCases t 
                    |> Array.map (fun uci -> 
                        let key = UnionCaseCacheKey (t,uci.Name)
                        uci, getOrCreate cache key (getUnionCaseFieldsInfo uci |> genRecordSchema))
                    |> splitResults
                match schemas, errors with
                | schemas,[] -> schemas |> List.map snd |> List.toArray |> Union |> Ok
                | _, errors -> AggregateError errors |> Error  
        | t ->         
            match cache.TryFindSchema (CustomCacheKey t) with
            | Some schema -> schema |> Ok
            | None -> NotSupportedType t |> Error

    and private genRecordSchema 
            (fieldsInfo:{|Name:string; Aliases:string array; Default:string option; Scale: int option; Type:Type|} array)
            (key:SchemaCacheKey)
            (cache:Cache<SchemaCacheKey,Schema>) =

        let fields = List<RecordField>()
        let schema = 
            createRecordSchema key fields
            |> cache.AddSchema key  // create schema in advance for using it in recursive types

        let fieldSchemas, errors =
            fieldsInfo 
            |> Array.map (fun fi -> 
                let schema = 
                    match fi.Type with
                    | t when t = typeof<decimal> && fi.Scale.IsSome -> Decimal {Precision = 29; Scale = fi.Scale.Value} |> Ok
                    | _ -> genSchema fi.Type cache
                fi, schema)
            |> splitResults

        match fieldSchemas, errors with
        | fieldSchemas,[] ->
            fieldSchemas
            |> List.map(fun (fi,schema) ->
                let defValue =  match fi.Type with AsOption _ -> Some "null" | _ -> fi.Default    
                {Name = fi.Name; Aliases = fi.Aliases |> List.ofArray; Type = schema; Default = defValue}:RecordField)
            |> fields.AddRange

            schema |> Ok

        | _, errors -> AggregateError errors |> Error

    let private primitives = 
        [ "null", Null; "string", String; "boolean", Boolean; "int", Int; "long", Long; "float", Float; "double", Double; "bytes", Bytes] 
        |> Map.ofList

    let private (|IsPrimitive|_|) = primitives.TryFind

    let private (|IsCached|_|) (cache:Dictionary<string,Schema>) ns (typeName:string) =
        match cache.TryGetValue(canonicalName ns typeName |> snd) with
        | true, schema -> Some schema
        | _ -> None

    let ofString (jsonString:string) = 
        let doc = JsonDocument.Parse jsonString
        let cache = Cache<string,Schema>()

        let tryGetProperty (name:string) (el:JsonElement) = match el.TryGetProperty name with true, el -> Some el | _ -> None
        let getProperty (name:string) (el:JsonElement) = 
            match el.TryGetProperty name with 
            | true, el -> el 
            | _ -> failwithf "property '%s' is absent in %A" name (el.GetRawText())        
        let getSeq (el:JsonElement) = seq { for i in 0 .. el.GetArrayLength() - 1 do el.[i].GetString() }
        let getName ns (el:JsonElement) = 
            let ns = match tryGetProperty "namespace" el with Some el -> el.GetString() | _ -> ns            
            canonicalName ns ((getProperty "name" el).GetString())
        let getAliases ns (el:JsonElement) = 
            match el.TryGetProperty "aliases" with 
            | true, el ->  getSeq el |> List.ofSeq |> List.map ((canonicalName ns) >> snd)
            | _ -> []
        let getFieldName (el:JsonElement) = (getProperty "name" el).GetString()
        let getSymbols (el:JsonElement) = match el.TryGetProperty "symbols" with true, el -> getSeq el |> Array.ofSeq | _ -> [||]
        let getSize (el:JsonElement) = (getProperty "size" el).GetInt32()
        let getPrecision (el:JsonElement) = (getProperty "precision" el).GetInt32()
        let getScale (el:JsonElement) = (getProperty "scale" el).GetInt32()
        let getDefault (el:JsonElement) = 
            tryGetProperty "default" el 
            |> Option.map (fun el -> 
                match el.ValueKind with
                | JsonValueKind.String -> el.GetString()
                | _ -> el.GetRawText())

        let rec parse ns (el:JsonElement) =
            match el.ValueKind with
            | JsonValueKind.Object ->
                let typeEl = getProperty "type" el
                match typeEl.ValueKind with
                | JsonValueKind.String ->
                    match typeEl.GetString() with
                    | IsPrimitive schema -> 
                        match tryGetProperty "logicalType" el with
                        | Some logicalEl when logicalEl.GetString() = "decimal" -> Decimal {Precision = getPrecision el; Scale = getScale el}
                        | _ -> schema
                    | IsCached cache ns schema -> schema
                    | "array" -> Array { Items = getProperty "items" el |> parse ns; Default = getDefault el}
                    | "map" -> Map { Values = getProperty "values" el |> parse ns; Default = getDefault el}
                    | "enum" -> 
                        let ns, name = getName ns el
                        Enum { Name = name; Aliases = getAliases ns el; Symbols = getSymbols el; Default = getDefault el}
                        |> cache.AddSchema name                    
                    | "record" ->    
                        let ns, name = getName ns el
                        let fields = List<RecordField>()
                        let schema = 
                            Record {Name = name; Aliases = getAliases ns el; Fields = fields}
                            |> cache.AddSchema name
                        let fieldsEl = getProperty "fields" el
                        for i in 0 .. fieldsEl.GetArrayLength() - 1 do
                            let el = fieldsEl.[i]
                            {Name = getFieldName el; Aliases = getAliases "" el; Type = getProperty "type" el |> parse ns; Default = getDefault el} 
                            |> fields.Add
                        schema
                    | "fixed" ->
                        let ns, name = getName ns el
                        Fixed {Name = name; Aliases = getAliases ns el; Size = getSize el}
                        |> cache.AddSchema name                    
                    | typeName -> failwithf "Unknown type: %s" typeName
                | JsonValueKind.Array -> parse ns typeEl
                | kind -> failwithf "not supported kind for 'type' property: %A" kind 
            | JsonValueKind.Array ->
                seq{for i in 0 .. el.GetArrayLength() - 1 do parse ns el.[i]} |> Array.ofSeq |> Union
            | JsonValueKind.String ->
                match el.GetString() with
                | IsPrimitive schema -> schema
                | IsCached cache ns schema -> schema
                | typeName -> failwithf "Unknown type: %s" typeName
            | kind -> failwithf "not supported kind%A" kind 
        
        parse "" doc.RootElement

    let private toString' (isCanonical:bool) (schema:Schema) =        
        use stream = new MemoryStream()
        use writer = new Utf8JsonWriter(stream)
        let cache = Cache<string,Schema>()

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
        let writeDefault (schema:Schema) = 
            let rec wr v = function
                | Null -> writer.WriteNullValue()
                | Boolean -> writer.WriteBooleanValue (bool.Parse(v))
                | Int | Long | Float | Double -> writer.WriteNumberValue(Decimal.Parse(v))
                | Record _ | Map _ | Array _ -> JsonDocument.Parse(v).WriteTo writer
                | Union arr -> wr v arr.[0]
                | _ -> writer.WriteStringValue v
            
            if isCanonical then ignore
            else Option.iter (fun v -> writer.WritePropertyName "default";  wr v schema) 

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
                writeDefault schema.Items (schema.Default)
                writer.WriteEndObject()
            | Map schema -> 
                writer.WriteStartObject()
                writeType "map"
                writer.WritePropertyName "values"
                write schema.Values
                writeDefault schema.Values schema.Default
                writer.WriteEndObject()
            | Enum schema when cache.ContainsKey schema.Name -> writer.WriteStringValue schema.Name
            | Enum schema ->
                writer.WriteStartObject()
                writeType "enum"
                writeName schema.Name
                writeAliases schema.Aliases
                writeArray "symbols" schema.Symbols
                writeDefault String schema.Default
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
                    writeDefault field.Type field.Default
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

        match primitives |> Map.toList |> List.tryFind (snd >> (=) schema) with
        | Some (typeName, _) -> sprintf "{\"type\": \"%s\"}" typeName   // root primitive types special case
        | None -> 
            write schema
            writer.Flush()
            let bytes = stream.ToArray() 
            Encoding.UTF8.GetString(bytes, 0, bytes.Length)

    let toString = toString' false

    let toCanonicalString = toString' true

    let private generate' (cache:SchemaCache) (customRules:CustomRule list) (type':Type): Result<Schema, SchemaError> =
        for rule in seq {yield! CustomRule.buidInRules; yield!customRules} do
            cache.Add(CustomCacheKey rule.TargetType, ofString rule.Schema)
        genSchema type' cache

    let generate(customRules:CustomRule list) (type':Type)  : Result<Schema, SchemaError> =
        generate' (SchemaCache()) customRules type'

    let generateWithReflector (rules:CustomRule list) (type':Type) : Result<Schema*SchemaReflector, SchemaError>  =
        let cache = SchemaCache()

        generate' cache rules type'
        |> Result.map (fun schema ->
            let reflector = SchemaReflector()

            // it is significant to add custom rules before arrays, maps, etc
            for rule in seq {yield! CustomRule.buidInRules; yield!rules} do
                reflector.AddWriteCast rule.TargetType rule.WriteCast
                reflector.AddReadCast rule.TargetType rule.ReadCast
            
            // adding of arrays and maps should be before adding records and unioncases
            for pair in cache do match pair.Key with ArrayCacheKey type' -> reflector.AddArray type' | _ -> ()
            for pair in cache do match pair.Key with MapCacheKey type' -> reflector.AddMap type' | _ -> ()
            for pair in cache do match pair.Key,pair.Value with (EnumCacheKey type'),(Enum schema) -> reflector.AddEnum schema.Name type' | _ -> ()
            for pair in cache do match pair.Key,pair.Value with (RecordCacheKey type'),(Record schema) -> reflector.AddRecord schema.Name type' | _ -> ()
            for pair in cache do match pair.Key,pair.Value with (UnionCaseCacheKey (type',name)),(Record schema) -> reflector.AddUnionCase schema.Name name type' | _ -> ()
            for pair in cache do match pair.Key with NullableCacheKey type' -> reflector.AddNullable type' | _ -> ()

            schema,reflector)
      
    let generateWithCache (cache:SchemaCache) (rules:CustomRule list) (type':Type) : Result<Schema, SchemaError>  =
        generate' cache rules type'