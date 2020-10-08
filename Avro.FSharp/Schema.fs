module Avro.FSharp.Schema

open System
open System.Reflection
open System.Collections.Generic
open Newtonsoft.Json.Linq
open FSharp.Reflection
open Avro

type SchemaError = 
    | AggregateError of SchemaError list
    | NotSupportedType of Type

let private primitive type' = PrimitiveSchema.NewInstance(type') :> Schema

let private enumSchemaConstructor () =

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

    fun (symbols:string array) (aliases:string array) (name:string)  ->
        let name = SchemaName(name, "", "")
        let aliases = if aliases.Length > 0 then aliases |> Array.map (fun name -> SchemaName(name, "", "")) |> List else null
        ctr.Invoke([|name; aliases; List(symbols); null; null; SchemaNames(); null |]) :?> Schema

let createEnumSchema : string array -> string array -> string -> Schema = enumSchemaConstructor() 

let private arraySchemaConstructor () =

    let types = [|
        typeof<Schema>          // items schema
        typeof<PropertyMap>     // props
    |]

    let ctr = typeof<ArraySchema>.GetConstructor(BindingFlags.Instance ||| BindingFlags.NonPublic, null, types, null)

    fun (itemsSchema:Schema) ->
        ctr.Invoke([|itemsSchema; null|]) :?> Schema

let createArraySchema : Schema -> Schema = arraySchemaConstructor() 

let private mapSchemaConstructor () =

    let types = [|
        typeof<Schema>          // value schema
        typeof<PropertyMap>     // props
    |]

    let ctr = typeof<MapSchema>.GetConstructor(BindingFlags.Instance ||| BindingFlags.NonPublic, null, types, null)

    fun (itemsSchema:Schema) ->
        ctr.Invoke([|itemsSchema; null|]) :?> Schema

let createMapSchema : Schema -> Schema = mapSchemaConstructor() 

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

let createFieldSchema : string -> string array -> string -> Schema -> Field = fieldConstructor()

let private recordSchemaConstructor () =

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

let createRecordSchema : string -> string array -> List<Field>  -> Schema = recordSchemaConstructor() 

let private unionSchemaConstructor () =

    let types = [|
        typeof<List<Schema> >   // schemas
        typeof<PropertyMap>     // props
    |]

    let ctr = typeof<UnionSchema>.GetConstructor(BindingFlags.Instance ||| BindingFlags.NonPublic, null, types, null)

    fun (schemas:Schema list) ->
        ctr.Invoke([|List<Schema>(schemas); null|]) :?> Schema

let createUnionSchema : Schema list -> Schema = unionSchemaConstructor() 

let private logicalSchemaConstructor () =

    let types = [|
        typeof<Schema>          // base schema
        typeof<string>          // logicalTypeName
        typeof<PropertyMap>     // props
    |]

    let ctr = typeof<LogicalSchema>.GetConstructor(BindingFlags.Instance ||| BindingFlags.NonPublic, null, types, null)

    fun (baseSchema:Schema) (logicalTypeName:string ) (props:PropertyMap) ->
        ctr.Invoke([|baseSchema; logicalTypeName; props|]) :?> Schema

let createLogicalSchema : Schema -> string -> PropertyMap -> Schema = logicalSchemaConstructor() 

let private bytesSchema = primitive "bytes"
let private stringSchema = primitive "string"
let private booleanSchema = primitive "boolean"
let private intSchema = primitive "int"
let private longSchema = primitive "long"
let private floatSchema = primitive "float"
let private doubleSchema = primitive "double"

let genDecimalSchema pecision scale : Schema = 
    let props = PropertyMap()
    props.Set("precision", pecision.ToString())
    props.Set("scale", scale.ToString())
    createLogicalSchema bytesSchema "decimal" props

let decimalSchema : Schema = genDecimalSchema 29 14

let uuidSchema : Schema = 
    createLogicalSchema stringSchema "uuid" null


let (|AsEnumerable|_|) (type':Type) =
    type'.GetInterfaces()
    |> Array.tryFind (fun it -> it.IsGenericType && it.GetGenericTypeDefinition() = typedefof<IEnumerable<_>>)

let rec private typeName (type':Type) : string =

    let name = type'.FullName.Replace('+','.')
    let genericPartIndex = name.IndexOf('`')
    if genericPartIndex <> -1 
    then 
        type'.GetGenericArguments()
        |> Array.map (fun t -> (typeName t).Replace('.','_'))
        |> String.concat "_And_"
        |> (sprintf "%s_Of_%s" (name.Substring(0, genericPartIndex)))
    else name

let private splitResults<'Tag> (results:('Tag*Result<Schema,SchemaError>) array) =
    Array.foldBack (fun  (tag,result) (okeys, errors)-> 
        match result with
        | Ok result -> ((tag, result) :: okeys), errors
        | Error err -> okeys, (err :: errors)
        ) results ([],[])

let getOrCreate (cache:Dictionary<string,Schema>) creator name =
    match cache.TryGetValue name with
    | true, schema -> Ok schema
    | false, _ -> 
        creator name |> Result.map (fun schema -> cache.[name] <- schema; schema)

let getAliases (type':Type) =
    match type'.GetCustomAttribute(typeof<Annotations.AliasesAttribute>) with 
    | :? Annotations.AliasesAttribute as attr -> attr.Aliases
    | _ -> [||]

let rec private genRecordSchemaOfPI 
        (cache:Dictionary<string,Schema>) 
        (aliases:string array)
        (pis:PropertyInfo array)
        (name:string) =

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

    let fieldsInfo = 
        pis |> Array.map (fun pi -> 
                {|
                    Name=pi.Name; 
                    Aliases=getAliasesOfPI pi
                    Default = getDefaultOfPI pi
                    Scale = getScaleOfPI pi
                    Type=pi.PropertyType
                |})

    genRecordSchema cache aliases fieldsInfo name

and private genRecordSchema 
        (cache:Dictionary<string,Schema>)
        (aliases:string array) 
        (fieldsInfo:{|Name:string; Aliases:string array; Default:string; Scale: int option; Type:Type|} array) 
        (name:string) =
    let fields = List<Field>()
    let schema = createRecordSchema name aliases fields
    cache.Add(name, schema)

    let fieldSchemas, errors =
        fieldsInfo 
        |> Array.map (fun fi -> 
            let schema = 
                match fi.Type with
                | t when t = typeof<decimal> && fi.Scale.IsSome -> genDecimalSchema 29 fi.Scale.Value |> Ok
                | _ -> genSchema cache fi.Type
            fi, schema)
        |> splitResults

    match fieldSchemas, errors with
    | fieldSchemas,[] ->
        fieldSchemas
        |> List.map(fun (fi,schema) -> createFieldSchema fi.Name fi.Aliases fi.Default schema)                                
        |> fields.AddRange
        schema |> Ok
    | _, errors -> AggregateError errors |> Error

and private genSchema (cache:Dictionary<string,Schema>) (type':Type) : Result<Schema,SchemaError> =
    match type' with
    | t when t = typeof<string> -> stringSchema |> Ok
    | t when t = typeof<bool> -> booleanSchema |> Ok
    | t when t = typeof<int> -> intSchema |> Ok
    | t when t = typeof<int64> -> longSchema |> Ok
    | t when t = typeof<float32> -> floatSchema |> Ok
    | t when t = typeof<float> -> doubleSchema |> Ok
    | t when t = typeof<byte array> -> bytesSchema |> Ok
    | t when t = typeof<decimal> -> decimalSchema |> Ok
    | t when t = typeof<Guid> -> uuidSchema |> Ok
    | t when t = typeof<DateTime> -> stringSchema |> Ok //  ISO 8601 string is supposed
    | t when t = typeof<TimeSpan> -> stringSchema |> Ok //  ISO 8601 string is supposed
    | t when t = typeof<DateTimeOffset> -> stringSchema |> Ok //  ISO 8601 string is supposed
    | t when t = typeof<Uri> -> stringSchema |> Ok 
    | t when t.IsEnum ->
        let creator = 
            t.GetFields(BindingFlags.Public ||| BindingFlags.Static) 
            |> Array.map(fun fi -> fi.Name)
            |> createEnumSchema
        getOrCreate cache (creator (getAliases t) >> Ok) (typeName t) 
    | AsEnumerable it ->
        let itemType = it.GetGenericArguments().[0]
        if itemType.IsGenericType && itemType.GetGenericTypeDefinition() = typedefof<KeyValuePair<_,_>> 
        then
            itemType.GetGenericArguments().[1]
            |> genSchema cache
            |> Result.map createMapSchema
        else
            it.GetGenericArguments().[0]
            |> genSchema cache
            |> Result.map createArraySchema
    | t when FSharpType.IsRecord t ->
        let aliases = getAliases t
        getOrCreate cache (FSharpType.GetRecordFields t |> genRecordSchemaOfPI cache aliases) (typeName t) 
    | t when FSharpType.IsTuple t ->
        let fieldsInfo = 
            FSharpType.GetTupleElements t 
            |> Array.mapi (fun idx t -> {|Name=sprintf"Item%d" (idx+1); Aliases = [||]; Default = ""; Scale = None; Type = t|})
        getOrCreate cache (genRecordSchema cache [||] fieldsInfo) (typeName t) 
    | t when FSharpType.IsUnion t ->
        let schemas, errors =
            FSharpType.GetUnionCases t 
            |> Array.map (fun uci -> 
                let name = typeName(t) + "." + uci.Name
                uci, getOrCreate cache (uci.GetFields() |> genRecordSchemaOfPI cache [||]) name)
            |> splitResults

        match schemas, errors with
        | schemas,[] -> schemas |> List.map snd |> createUnionSchema |> Ok
        | _, errors -> AggregateError errors |> Error  

    | t -> NotSupportedType t |> Error

let generateSchema (type':Type) : Result<Schema,SchemaError> =
    genSchema (Dictionary<string,Schema>()) type'