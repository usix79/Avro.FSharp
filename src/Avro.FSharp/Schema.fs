module Avro.FSharp.Schema

open System
open System.Globalization
open System.Reflection
open System.Collections.Generic
open Newtonsoft.Json.Linq
open FSharp.Reflection
open Avro

type SchemaError = 
    | AggregateError of SchemaError list
    | NotSupportedType of Type

type private SchemaCacheKey =
    | Enum of Type
    | Record of Type
    | UnionCase of unionType:Type * caseName:string
    | Array of Type
    | Map of Type
    | Custom of Type
    | Nullable of Type

type private SchemaCache() =
    inherit Dictionary<SchemaCacheKey, Schema>()
    member this.AddSchema key schema = this.[key] <- schema; schema
    member this.TryFindSchema key = 
        match this.TryGetValue key with 
        | true, schema -> Some schema 
        | _ -> None

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

    let name =
        match type' with
        | AsOption _  -> "Nullable`"
        | _ -> type'.FullName.Replace('+','.')

    let genericPartIndex = name.IndexOf('`')
    if genericPartIndex <> -1 
    then 
        type'.GetGenericArguments()
        |> Array.map (fun t -> (typeName t).Replace('.','_'))
        |> String.concat "_And_"
        |> (sprintf "%s_Of_%s" (name.Substring(0, genericPartIndex)))
    else name

let private getAliases (type':Type) =
    match type'.GetCustomAttribute(typeof<Annotations.AliasesAttribute>) with 
    | :? Annotations.AliasesAttribute as attr -> attr.Aliases
    | _ -> [||]

let private primitive type' = PrimitiveSchema.NewInstance(type') :> Schema

let private enumSchemaConstructor =

    let types = [|
        typeof<SchemaName>      // name
        typeof<IList<SchemaName>>   // aliases
        typeof<List<String>>     // symbols
        typeof<IDictionary<string, int>> // symbolMap
        typeof<PropertyMap>     // props
        typeof<SchemaNames>     // names
        typeof<string>          // doc      
    |]

    let ctr = typeof<EnumSchema>.GetConstructor(BindingFlags.Instance ||| BindingFlags.NonPublic, null, types, null)

    fun (name:string) (aliases:string array) (symbols:string array) ->
        let name = SchemaName(name, "", "")
        let aliases = if aliases.Length > 0 then aliases |> Array.map (fun name -> SchemaName(name, "", "")) |> List else null
        ctr.Invoke([|name; aliases; List(symbols); null; null; SchemaNames(); null |]) :?> Schema

let private createEnumSchema (type':Type) =
    type'.GetFields(BindingFlags.Public ||| BindingFlags.Static) |> Array.map(fun fi -> fi.Name)
    |> enumSchemaConstructor (typeName type') (getAliases type') 

let private fixedSchemaConstructor =

    let types = [|
        typeof<SchemaName>      // name
        typeof<IList<SchemaName>>   // aliases
        typeof<int>             // size
        typeof<PropertyMap>     // props
        typeof<SchemaNames>     // names
        typeof<string>          // doc      
    |]

    let ctr = typeof<FixedSchema>.GetConstructor(BindingFlags.Instance ||| BindingFlags.NonPublic, null, types, null)

    fun (name:string) (size:int) ->
        let name = SchemaName(name, "", "")
        ctr.Invoke([|name; null; size; null; SchemaNames(); null |]) :?> Schema

let private createFixedSchema name size =
    fixedSchemaConstructor name size

let private guidSchema = createFixedSchema "guid" 16

let private arraySchemaConstructor () =

    let types = [|
        typeof<Schema>          // items schema
        typeof<PropertyMap>     // props
    |]

    let ctr = typeof<ArraySchema>.GetConstructor(BindingFlags.Instance ||| BindingFlags.NonPublic, null, types, null)

    fun (itemsSchema:Schema) ->
        ctr.Invoke([|itemsSchema; null|]) :?> Schema

let private createArraySchema : Schema -> Schema = arraySchemaConstructor() 

let private mapSchemaConstructor () =

    let types = [|
        typeof<Schema>          // value schema
        typeof<PropertyMap>     // props
    |]

    let ctr = typeof<MapSchema>.GetConstructor(BindingFlags.Instance ||| BindingFlags.NonPublic, null, types, null)

    fun (itemsSchema:Schema) ->
        ctr.Invoke([|itemsSchema; null|]) :?> Schema

let private createMapSchema : Schema -> Schema = mapSchemaConstructor() 

let private fieldConstructor () =

    let types = [|
        typeof<Schema>          // schema
        typeof<string>          // name
        typeof<IList<string>>   // aliases
        typeof<int>             // pos
        typeof<string>          // doc      
        typeof<JToken>          // defaultValue
        typeof<Field.SortOrder> // sortorder
        typeof<PropertyMap>     // props
    |]

    let ctr = typeof<Field>.GetConstructor(BindingFlags.Instance ||| BindingFlags.NonPublic, null, types, null)

    fun (name:string) (aliases:string array) (default':string) (schema:Schema) ->
        let aliases = if aliases.Length > 0 then aliases else null
        let default' = if not (String.IsNullOrEmpty default') then JToken.Parse(default') else null
        ctr.Invoke([|schema; name; aliases; 0; null; default'; Field.SortOrder.ignore; null |]) :?> Field

let private createFieldSchema : string -> string array -> string -> Schema -> Field = fieldConstructor()

let private recordSchemaConstructor =

    let types = [|
        typeof<Schema.Type>     // type
        typeof<SchemaName>      // name
        typeof<IList<SchemaName>>   // aliases
        typeof<PropertyMap>     // props
        typeof<List<Field>>     // fields
        typeof<bool>            // request
        typeof<IDictionary<string, Field>> // fieldMap
        typeof<IDictionary<string, Field>> // fieldAliasMap
        typeof<SchemaNames>     // names
        typeof<string>          // doc      
    |]

    let ctr = typeof<RecordSchema>.GetConstructor(BindingFlags.Instance ||| BindingFlags.NonPublic, null, types, null)

    fun (name:string) (aliases:string array) (fields:List<Field>) ->
        let name = SchemaName(name, "", "")
        let aliases = if aliases.Length > 0 then aliases |> Array.map (fun name -> SchemaName(name, "", "")) |> List else null
        ctr.Invoke([|Schema.Type.Record; name; aliases; null; fields; false; null; null; SchemaNames(); null |]) :?> Schema

let private createRecordSchema (key:SchemaCacheKey) (fields:List<Field>) =
    let type',name =
        match key with
        | Record type' -> type', typeName type'
        | UnionCase (type',caseName) -> type', typeName type' + "." + caseName
        | _ -> failwithf "Not supported key for record scheme: %A" key
    recordSchemaConstructor name (getAliases type') fields
 
let private unionSchemaConstructor () =

    let types = [|
        typeof<List<Schema> >   // schemas
        typeof<PropertyMap>     // props
    |]

    let ctr = typeof<UnionSchema>.GetConstructor(BindingFlags.Instance ||| BindingFlags.NonPublic, null, types, null)

    fun (schemas:Schema list) ->
        ctr.Invoke([|List<Schema>(schemas); null|]) :?> Schema

let private createUnionSchema : Schema list -> Schema = unionSchemaConstructor() 

let private logicalSchemaConstructor () =

    let types = [|
        typeof<Schema>          // base schema
        typeof<string>          // logicalTypeName
        typeof<PropertyMap>     // props
    |]

    let ctr = typeof<LogicalSchema>.GetConstructor(BindingFlags.Instance ||| BindingFlags.NonPublic, null, types, null)

    fun (baseSchema:Schema) (logicalTypeName:string ) (props:PropertyMap) ->
        ctr.Invoke([|baseSchema; logicalTypeName; props|]) :?> Schema

let private nullSchema = primitive "null"
let private bytesSchema = primitive "bytes"
let private stringSchema = primitive "string"
let private booleanSchema = primitive "boolean"
let private intSchema = primitive "int"
let private longSchema = primitive "long"
let private floatSchema = primitive "float"
let private doubleSchema = primitive "double"

let private createLogicalSchema : Schema -> string -> PropertyMap -> Schema = logicalSchemaConstructor() 

let private uuidSchema : Schema = createLogicalSchema stringSchema "uuid" null

let private createDecimalSchema pecision scale : Schema = 
    let props = PropertyMap()
    props.Set("precision", pecision.ToString())
    props.Set("scale", scale.ToString())
    createLogicalSchema bytesSchema "decimal" props

let private decimalSchema : Schema = createDecimalSchema 29 14

let private splitResults<'Tag> (results:('Tag*Result<Schema,SchemaError>) array) =
    Array.foldBack (fun  (tag,result) (okeys, errors)-> 
        match result with
        | Ok result -> ((tag, result) :: okeys), errors
        | Error err -> okeys, (err :: errors)
        ) results ([],[])

let private recordFieldInfo (pi:PropertyInfo) =
    let getAliasesOfPI (pi:PropertyInfo) =
        match pi.GetCustomAttribute(typeof<Annotations.AliasesAttribute>) with 
        | :? Annotations.AliasesAttribute as attr -> attr.Aliases
        | _ -> [||]

    let getDefaultOfPI (pi:PropertyInfo) =
        match pi.GetCustomAttribute(typeof<Annotations.DefaultValueAttribute>) with 
        | :? Annotations.DefaultValueAttribute as attr -> attr.Value
        | _ -> null

    let getScaleOfPI (pi:PropertyInfo) =
        match pi.GetCustomAttribute(typeof<Annotations.ScaleAttribute>) with 
        | :? Annotations.ScaleAttribute as attr -> Some attr.Scale
        | _ -> None

    {|
        Name=pi.Name
        Aliases=getAliasesOfPI pi
        Default = getDefaultOfPI pi
        Scale = getScaleOfPI pi
        Type=pi.PropertyType
    |}

let private getRecordFieldsInfo (type':Type) = 
    FSharpType.GetRecordFields type' |> Array.map recordFieldInfo

let private getUnionCaseFieldsInfo (uci: UnionCaseInfo) =
    uci.GetFields() |> Array.map recordFieldInfo

let private getTupleFieldsInfo (type': Type) =
    FSharpType.GetTupleElements type'
    |> Array.mapi (fun idx t -> {|Name=sprintf"Item%d" (idx+1); Aliases = [||]; Default = ""; Scale = None; Type = t|})

let rec private genSchema (type':Type) (cache:SchemaCache) : Result<Schema,SchemaError> =
    match type' with
    | t when t = typeof<string> -> stringSchema |> Ok
    | t when t = typeof<bool> -> booleanSchema |> Ok
    | t when t = typeof<int> -> intSchema |> Ok
    | t when t = typeof<int64> -> longSchema |> Ok
    | t when t = typeof<float32> -> floatSchema |> Ok
    | t when t = typeof<float> -> doubleSchema |> Ok
    | t when t = typeof<byte array> -> bytesSchema |> Ok
    | t when t = typeof<decimal> -> decimalSchema |> Ok
    | t when t.IsEnum -> getOrCreate cache (Enum t) (fun _ _ -> createEnumSchema t |> Ok)
    | AsEnumerable it ->
        let itemType = it.GetGenericArguments().[0]
        if itemType.IsGenericType && itemType.GetGenericTypeDefinition() = typedefof<KeyValuePair<_,_>> 
        then
            let mapValueType = itemType.GetGenericArguments().[1]
            getOrCreate cache (Map type') (fun _ -> genSchema mapValueType >> Result.map createMapSchema)
        else
            let arrayItemType = it.GetGenericArguments().[0]
            getOrCreate cache (Array type') (fun _ -> genSchema arrayItemType >> Result.map createArraySchema)
    | t when FSharpType.IsRecord t -> getOrCreate cache (Record t) (getRecordFieldsInfo t |> genRecordSchema) 
    | t when FSharpType.IsTuple t -> getOrCreate cache (Record t) (getTupleFieldsInfo t |> genRecordSchema)
    | t when FSharpType.IsUnion t ->
        match t with
        | AsOption someType -> 
            genSchema someType cache 
            |> Result.bind (fun someSchema ->
                (fun _ _ -> [nullSchema; someSchema] |> createUnionSchema |> Ok)
                |> getOrCreate cache (Nullable t))                
        | _ ->
            let schemas, errors =
                FSharpType.GetUnionCases t 
                |> Array.map (fun uci -> 
                    let key = UnionCase (t,uci.Name)
                    uci, getOrCreate cache key (getUnionCaseFieldsInfo uci |> genRecordSchema))
                |> splitResults

            match schemas, errors with
            | schemas,[] -> schemas |> List.map snd |> createUnionSchema |> Ok
            | _, errors -> AggregateError errors |> Error  
    | t ->         
        match cache.TryFindSchema (Custom t) with
        | Some schema -> schema |> Ok
        | None -> NotSupportedType t |> Error

and private genRecordSchema 
        (fieldsInfo:{|Name:string; Aliases:string array; Default:string; Scale: int option; Type:Type|} array)
        (key:SchemaCacheKey)
        (cache:SchemaCache) =

    let fields = List<Field>()
    let schema = cache.AddSchema key <| createRecordSchema key fields // create schema in advance to prevent stack overflow

    let fieldSchemas, errors =
        fieldsInfo 
        |> Array.map (fun fi -> 
            let schema = 
                match fi.Type with
                | t when t = typeof<decimal> && fi.Scale.IsSome -> createDecimalSchema 29 fi.Scale.Value |> Ok
                | _ -> genSchema fi.Type cache
            fi, schema)
        |> splitResults

    match fieldSchemas, errors with
    | fieldSchemas,[] ->
        fieldSchemas
        |> List.map(fun (fi,schema) ->
            let defValue =  match fi.Type with AsOption _ -> "null" | _ -> fi.Default    
            createFieldSchema fi.Name fi.Aliases defValue schema)                                
        |> fields.AddRange
        schema |> Ok
    | _, errors -> AggregateError errors |> Error

let buidInRules = [|
    CustomRule(typeof<Guid>, 
        """{"type": "fixed", "name": "guid", "size": 16}""", 
        (fun v -> (v :?> Guid).ToByteArray() :> obj),
        (fun v -> (v :?> byte[]) |> Guid :> obj))
    CustomRule(typeof<Uri>, 
        """{"type": "string"}""", 
        (fun v -> v.ToString() :> obj),
        (fun v -> (v :?> string) |> Uri :> obj))
    CustomRule(typeof<DateTime>,
        """{"type": "string"}""", 
        (fun v -> (v :?> DateTime).ToString("O", CultureInfo.InvariantCulture) :> obj),
        (fun v -> DateTime.Parse((v :?> string), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) :> obj))
    CustomRule(typeof<DateTimeOffset>,
        """{"type": "string"}""", 
        (fun v -> (v :?> DateTimeOffset).ToString("O", CultureInfo.InvariantCulture) :> obj),
        (fun v -> DateTimeOffset.Parse((v :?> string), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) :> obj))
    CustomRule(typeof<TimeSpan>,
        """{"type": "string"}""", 
        (fun v -> (v :?> TimeSpan) |> Xml.XmlConvert.ToString :> obj),
        (fun v -> Xml.XmlConvert.ToTimeSpan(v :?> string) :> obj))
|]

let private generateSchema' (cache:SchemaCache) (customRules:CustomRule array) (type':Type): Result<Schema,SchemaError> =
    for rule in seq {yield! buidInRules; yield!customRules} do
        cache.Add(Custom rule.TargetType, Schema.Parse rule.Schema)
    genSchema type' cache

let generateSchema (customRules:CustomRule array) (type':Type)  : Result<Schema,SchemaError> =
    generateSchema' (SchemaCache()) customRules type'

let generateSchemaAndReflector (customRules:CustomRule array) (type':Type) : Result<Schema*SchemaReflector, SchemaError>  =
    let cache = SchemaCache()

    generateSchema' cache customRules type'
    |> Result.map (fun schema ->
        let reflector = SchemaReflector()

        // it is significant to add custom rules before arrays, maps, etc
        for rule in seq {yield! buidInRules; yield!customRules} do
            reflector.AddWriteCast rule.TargetType rule.WriteCast
            reflector.AddReadCast rule.TargetType rule.ReadCast
        
        // adding of arrays and maps should be before adding records and unioncases
        for pair in cache do match pair.Key with Array type' -> reflector.AddArray type' | _ -> ()
        for pair in cache do match pair.Key with Map type' -> reflector.AddMap type' | _ -> ()
        for pair in cache do match pair.Key with Enum type' -> reflector.AddEnum pair.Value.Fullname type' | _ -> ()
        for pair in cache do match pair.Key with Record type' -> reflector.AddRecord pair.Value.Fullname type' | _ -> ()
        for pair in cache do match pair.Key with UnionCase (type',name) -> reflector.AddUnionCase pair.Value.Fullname name type' | _ -> ()
        for pair in cache do match pair.Key with Nullable type' -> reflector.AddNullable type' | _ -> ()

        schema,reflector)