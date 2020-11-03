namespace Avro.FSharp

open System
open System.IO
open System.Linq
open System.Collections.Generic
open System.Text
open FSharp.Reflection
open Microsoft.FSharp.Quotations.Patterns

module internal ReflectionFactoryHelpers =

    type FieldInfo = {
        Idx: int
        Type: Type
        Schema: RecordField
        Cast: (obj -> obj) option
        Default: obj
    }

    let getCast (casts: Dictionary<Type, obj -> obj>) (type':Type) =
        match casts.TryGetValue type' with true, cast -> cast | _ -> id

    let getMethodInfo = function
        | Call (_, methodInfo, _) -> methodInfo
        | _ -> failwith "Expression is not a call."

    let arrayElementType (type':Type) =
        match type' with
        | t when t.IsArray -> t.GetElementType()
        | t when t.IsGenericType -> t.GetGenericArguments().[0]
        | _ -> typeof<obj>

    let arrayConstructor (type':Type) =
        match type' with
        | t when t.IsArray ->
            let castMi =
                (getMethodInfo <@ Enumerable.Cast null @>)
                    .GetGenericMethodDefinition()
                    .MakeGenericMethod([|t.GetElementType()|])
            let toArrayMi =
                (getMethodInfo <@ Enumerable.ToArray null @>)
                    .GetGenericMethodDefinition()
                    .MakeGenericMethod([|t.GetElementType()|])
            fun obj -> toArrayMi.Invoke(null, [|castMi.Invoke(null, [|obj|])|])
        | t when t.GetGenericTypeDefinition() = typedefof<Collections.List<_>> ->
            let castMi =
                (getMethodInfo <@ Enumerable.Cast null @>)
                    .GetGenericMethodDefinition()
                    .MakeGenericMethod(t.GetGenericArguments())
            let ofSeqMi =
                (getMethodInfo <@ List.ofSeq null @>)
                    .GetGenericMethodDefinition()
                    .MakeGenericMethod(t.GetGenericArguments())
            fun obj -> ofSeqMi.Invoke(null, [|castMi.Invoke(null, [|obj|])|])
        | t ->
            let targetCtrArgument = typedefof<IEnumerable<_>>.MakeGenericType(t.GetGenericArguments())
            match t.GetConstructor [|targetCtrArgument|] with
            | null -> id    // obj array
            | ci ->
                let castMi =
                    (getMethodInfo <@ Enumerable.Cast null @>)
                        .GetGenericMethodDefinition()
                        .MakeGenericMethod(t.GetGenericArguments())
                fun obj -> ci.Invoke([|castMi.Invoke(null, [|obj|])|])

    let arrayInstanceConstructor (casts: Dictionary<Type, obj -> obj>) (ctr: Collections.ArrayList -> obj) (itemType:Type) (schema: ArraySchema) =
        let cast = getCast casts itemType
        fun () ->
            let values = Collections.ArrayList()
            { new IInstanceConstructor with
                member _.ExpectedValueSchema = schema.Items
                member _.ExpectedValueType = itemType
                member _.SetKey(key: string) = failwithf "array constructor is not supposed to set a key. But SetKey is called with key='%s'" key
                member _.SetValue(v: 'TArrayValue) = values.Add(cast(v:>obj)) |> ignore
                member _.Construct() = ctr values}

    let arrayDeconstructor (type':Type) =
        let sizeFun =
            match type' with
            | t when t.IsArray ->
                fun (obj:obj) -> (obj :?> Array).Length
            | t when typeof<Collections.ICollection>.IsAssignableFrom(t) ->
                fun obj -> (obj :?> Collections.ICollection).Count
            | _ ->
                fun obj ->
                    let mutable count = 0
                    for _ in obj :?> Collections.IEnumerable do count <- count + 1
                    count

        { new IArrayDeconstructor with
             member _.Size (obj:obj) = sizeFun obj
             member _.Items (obj:obj) =  obj :?> Collections.IEnumerable}

    let castSeqToDictionary<'TValue> (array: IEnumerable<KeyValuePair<string,obj>>) =
        let dict = Dictionary<string, 'TValue>()
        for pair in array do
            dict.[pair.Key] <- pair.Value :?> 'TValue
        dict

    let castSeqToMap<'TValue> (array: IEnumerable<KeyValuePair<string,obj>>) =
        array
        |> Seq.map(fun pair -> pair.Key, pair.Value :?> 'TValue)
        |> Map.ofSeq

    let mapValueType (type': Type) = if type'.IsGenericType then type'.GetGenericArguments().[1] else typeof<obj>

    let mapConstructor (type':Type) =
        match type' with
        | t when t.GetGenericTypeDefinition() = typedefof<Map<_,_>> ->
            let mi =
                (getMethodInfo <@ castSeqToMap null @>)
                    .GetGenericMethodDefinition()
                    .MakeGenericMethod([|t.GetGenericArguments().[1]|])
            fun obj -> mi.Invoke(null, [|obj|])
        | t ->
            let mi =
                (getMethodInfo <@ castSeqToDictionary null @>)
                    .GetGenericMethodDefinition()
                    .MakeGenericMethod([|t.GetGenericArguments().[1]|])
            fun obj -> mi.Invoke(null, [|obj|])

    let mapInstanceConstructor (casts: Dictionary<Type, obj -> obj>) (ctr: obj -> obj) (valueType: Type) (schema: MapSchema) =
        let cast = getCast casts valueType
        fun () ->
            let values = Dictionary<string,obj>()
            let mutable currentKey = ""
            { new IInstanceConstructor with
                member _.SetKey(key: string) = currentKey <- key; true
                member _.ExpectedValueSchema = schema.Values
                member _.ExpectedValueType = valueType
                member _.SetValue(v: 'TMapValue) = values.Add(currentKey, v)
                member _.Construct() = ctr(values:>obj)}

    let enumerateDict<'TKey,'TValue> (dict:IDictionary<'TKey,'TValue>) =
        seq { for pair in dict do
                Collections.DictionaryEntry(pair.Key, pair.Value) }

    let mapDeconstructor (type': Type) =
        let sizeFun =
            match type' with
            | t when typeof<Collections.ICollection>.IsAssignableFrom(t) ->
                fun (obj:obj) -> (obj :?> Collections.ICollection).Count
            | t when t.GetGenericTypeDefinition() = typedefof<Map<_,_>> ->
                let mi =
                    typedefof<Map<_,_>>
                        .MakeGenericType(t.GetGenericArguments())
                        .GetProperty("Count")
                        .GetGetMethod()
                fun obj -> mi.Invoke(obj, [||]) |> unbox
            | _ ->
                fun (obj:obj) ->
                    let mutable count = 0
                    for _ in obj :?> Collections.IEnumerable do count <- count + 1
                    count
        let itemsFun =
            match type' with
            | t when typeof<Collections.IDictionary>.IsAssignableFrom(t) ->
                fun (obj:obj) ->
                    seq{
                        for item in (obj :?> Collections.IDictionary) do
                            item :?> Collections.DictionaryEntry
                    }
            | t when t.GetGenericTypeDefinition() = typedefof<Map<_,_>> ->
                let mi =
                    (getMethodInfo <@ enumerateDict null @>)
                        .GetGenericMethodDefinition()
                        .MakeGenericMethod(t.GetGenericArguments())
                fun obj -> mi.Invoke(null, [|obj|]) :?> Collections.DictionaryEntry seq

            | _ -> failwithf "Map type is not supported: %s" type'.Name

        { new IMapDeconstructor with
             member _.Size (obj:obj) = sizeFun obj
             member _.Items (obj:obj) =  itemsFun obj}

    let fieldsInfoDict (infos: FieldInfo[]) =
        let dict = Dictionary<string,FieldInfo>()
        for info in infos do
            dict.[info.Schema.Name] <- info
            for alias in info.Schema.Aliases do dict.[alias] <- info
        dict

    let constructor (fields: FieldInfo[]) (ctr: obj[] -> obj) =
        let casts = fields |> Array.choose (fun info -> info.Cast |> Option.map(fun cast -> info.Idx,cast))
        let defs = fields |> Array.choose (fun info -> if isNull info.Default then None else Some (info.Idx,info.Default))

        fun (fieldValues: obj[]) ->
            defs |> Array.iter (fun (idx,v) -> if isNull fieldValues.[idx] then fieldValues.[idx] <- v)
            casts |> Array.iter (fun (idx,cast) -> fieldValues.[idx] <- cast fieldValues.[idx])
            ctr fieldValues

    let recordInstanceConstructor (ctr: obj[] -> obj) (fields: Dictionary<string,FieldInfo>) (schema: RecordSchema) =
        fun () ->
            let values = Array.zeroCreate schema.Fields.Count
            let mutable currentField = Unchecked.defaultof<FieldInfo>
            { new IInstanceConstructor with
                member _.SetKey(key: string) =
                    match fields.TryGetValue key with
                    | true, field ->  currentField <- field; true
                    | _ -> false
                member _.ExpectedValueSchema = currentField.Schema.Type
                member _.ExpectedValueType = currentField.Type
                member _.SetValue(v: 'TFieldValue1) = values.[currentField.Idx] <- v :> obj
                member _.Construct() = ctr values}

    let recordDeconstructor (type':Type) =
        let fieldsFun =
            match type' with
            | t when FSharpType.IsRecord t -> FSharpValue.PreComputeRecordReader t
            | t when FSharpType.IsTuple t -> FSharpValue.PreComputeTupleReader t
            | _ -> failwithf "Type is not supported as record %s" type'.Name

        { new IRecordDeconstructor with
            member _.Fields (obj:obj) = fieldsFun obj}

    let someInstanceConstructor (casts: Dictionary<Type, obj -> obj>) (ctr: obj -> obj) (someType: Type) (schema: Schema) =
        let cast = getCast casts someType
        fun () ->
            let mutable instance = null
            { new IInstanceConstructor with
                member _.SetKey(_: string) = true
                member _.ExpectedValueSchema = schema
                member _.ExpectedValueType = someType
                member _.SetValue(v: 'TOptionValue) =  instance <- ctr(cast(v:>obj))
                member _.Construct() = instance }

    let valueInstanceCtr (casts: Dictionary<Type, obj -> obj>) (valueType: Type) (valueSchema: Schema) =
        let cast = getCast casts valueType
        fun () ->
            let mutable instance:obj = null
            { new IInstanceConstructor with
                member _.Construct() = instance
                member _.ExpectedValueSchema = valueSchema
                member _.ExpectedValueType = valueType
                member _.SetKey(key: string) = failwithf "value constructor is not supposed to set a key. But SetKey is called with key='%s'" key
                member _.SetValue(v: 'TValue) = instance <- cast(v:>obj) }

    let unionDeconstructor (type':Type) =
        let idxMap =
            FSharpType.GetUnionCases(type')
            |> Array.mapi (fun idx uci -> uci.Tag,idx)
            |> Map.ofArray
        let getTag = FSharpValue.PreComputeUnionTagReader (type')
        let fieldFuns =
            FSharpType.GetUnionCases(type')
            |> Array.map FSharpValue.PreComputeUnionReader

        { new IUnionDeconstructor with
            member _.CaseIdx (obj:obj) = idxMap.[getTag obj]
            member _.CaseFields (idx:int) (obj:obj) = fieldFuns.[idx](obj)}

open ReflectionFactoryHelpers

type InstanceFactory(targetType:Type, rules:CustomRule list) =

    let serializationCasts = Dictionary<Type, obj -> obj>()
    let deserializationCasts = Dictionary<Type, obj -> obj>()
    do for rule in seq {yield! CustomRule.buidInRules; yield! rules} do
        serializationCasts.[rule.TargetType] <- rule.WriteCast
        deserializationCasts.[rule.TargetType] <- rule.ReadCast

    let enumConstructors = Dictionary<Type,IEnumConstructor>()
    let unionConstructors = Dictionary<Type, Dictionary<string, unit -> IInstanceConstructor>>()
    let nullableConstructors = Dictionary<Type, unit -> IInstanceConstructor>()
    let constructors = Dictionary<Type, unit -> IInstanceConstructor>()
    let enumDeconstructors = Dictionary<Type, IEnumDeconstructor>()
    let arrayDeconstructors = Dictionary<Type, IArrayDeconstructor>()
    let mapDeconstructors = Dictionary<Type, IMapDeconstructor>()
    let recordDeconstructors = Dictionary<Type, IRecordDeconstructor>()
    let unionDeconstructors = Dictionary<string, IUnionDeconstructor>()

    let cache = SchemaCache()
    let rootSchema =
        match Schema.generateWithCache cache rules targetType with
        | Ok schema -> schema
        | Error err -> failwithf "Schema error %A" err

    let fieldsInfo (casts: Dictionary<Type, obj -> obj>) (schemas: List<RecordField>)  =
        Array.mapi (fun idx type' ->
            let schema = schemas.[idx]
            {
                Idx = idx
                Type = type'
                Schema = schema
                Cast = match casts.TryGetValue type' with | true, cast -> Some cast | _ -> None
                Default =
                    match schema.Default with
                    | Some str ->
                        printfn "TYPE: %A" type'
                        let factory = new InstanceFactory(type', rules)
                        let builder = InstanceBuilder(factory)
                        let director = JsonDirector()
                        use stream = new MemoryStream(Encoding.UTF8.GetBytes(str))
                        director.Construct(stream, builder)
                        builder.Instance
                    | None -> null
            })

    do for pair in cache do
        match pair.Key, pair.Value with
        | (EnumCacheKey type'), (Enum schema) ->
            let values = (Enum.GetValues type').Cast<obj>().ToArray()
            let symbols =
                values
                |> Array.map (fun v -> (v.ToString()), v)
                |> Map.ofArray
                |> Dictionary

            let indexes = Dictionary<obj,int>()
            values |> Array.iteri(fun idx v -> indexes.[v] <- idx)

            enumConstructors.[type'] <-
                match schema.Default with
                | Some str ->
                    let defValue = Enum.Parse(type', str)
                    { new IEnumConstructor with
                        member _.Construct symbol = match symbols.TryGetValue symbol with true, v -> v | _ -> defValue}
                | None -> { new IEnumConstructor with member _.Construct symbol = symbols.[symbol]}

            enumDeconstructors.[type'] <- {new IEnumDeconstructor with member _.Idx obj = indexes.[obj]}

        | (ArrayCacheKey type'), (Array schema) ->
            let itemType = arrayElementType type'
            let ctr = arrayConstructor type'
            constructors.[type'] <- arrayInstanceConstructor deserializationCasts ctr itemType schema
            arrayDeconstructors.[type'] <- arrayDeconstructor type'
        | (MapCacheKey type'), (Map schema) ->
            let valueType = mapValueType type'
            let ctr = mapConstructor type'
            constructors.[type'] <- mapInstanceConstructor deserializationCasts ctr valueType schema
            mapDeconstructors.[type'] <- mapDeconstructor type'
        | (RecordCacheKey type'),(Record schema) ->
            if FSharpType.IsRecord type' then
                let fieldsInfo =
                    FSharpType.GetRecordFields type'
                    |> Array.map (fun pi -> pi.PropertyType)
                    |> fieldsInfo deserializationCasts schema.Fields
                let ctr = constructor fieldsInfo (FSharpValue.PreComputeRecordConstructor type')
                let fields = fieldsInfoDict fieldsInfo
                constructors.[type'] <- recordInstanceConstructor ctr fields schema
            else if FSharpType.IsTuple type' then
                let fieldsInfo =
                    FSharpType.GetTupleElements type'
                    |> fieldsInfo deserializationCasts schema.Fields
                let ctr = constructor fieldsInfo (FSharpValue.PreComputeTupleConstructor type')
                let fields =
                    fieldsInfo
                    |> Array.mapi (fun idx info -> (sprintf "Item%d" (idx+1)), info)
                    |> Map.ofArray
                    |> Dictionary
                constructors.[type'] <- recordInstanceConstructor ctr fields schema
            else failwithf "Type is not supported as record %A Schema: %A" type' schema
            recordDeconstructors.[type'] <- recordDeconstructor type'
        | (UnionCaseCacheKey (type', caseName)),(Record schema) ->
            let uci =
                FSharpType.GetUnionCases(type')
                |> Array.find (fun uci -> uci.Name = caseName)
            let fieldsInfo =
                uci.GetFields()
                |> Array.map (fun pi -> pi.PropertyType)
                |> fieldsInfo deserializationCasts schema.Fields
            let ctr = constructor fieldsInfo (FSharpValue.PreComputeUnionConstructor uci)
            let fields = fieldsInfoDict fieldsInfo
            let instanceCtr = recordInstanceConstructor ctr fields schema

            let dict =
                match unionConstructors.TryGetValue type' with
                | true, dict -> dict
                | _ -> Dictionary<string, unit -> IInstanceConstructor>()
            dict.[schema.Name] <- instanceCtr
            for alias in schema.Aliases do dict.[alias] <- instanceCtr
            unionConstructors.[type'] <- dict

            unionDeconstructors.[schema.Name] <- unionDeconstructor type'

        | (NullableCacheKey type'), schema ->
            let someUci = (FSharpType.GetUnionCases type').[1] // Option.Some
            let someCtr = FSharpValue.PreComputeUnionConstructor someUci
            let ctr = fun v -> someCtr [|v|]
            let someType = (someUci.GetFields().[0]).PropertyType
            nullableConstructors.[type'] <- someInstanceConstructor deserializationCasts ctr someType schema

            let someReader = FSharpValue.PreComputeUnionReader someUci
            serializationCasts.[type'] <- fun (obj:obj) -> (someReader obj).[0]

        | (CustomCacheKey _), _ -> ()
        | type' -> failwithf "Cann't process type: %A" type'

    interface IInstanceFactory with
        member _.TargetSchema = rootSchema
        member _.TargetType = targetType
        member _.Constructor(type':Type) = constructors.[type']()
        member _.NullableConstructor(type':Type) = nullableConstructors.[type']()
        member _.ValueConstructor(type': Type, schema: Schema) = (valueInstanceCtr deserializationCasts type' schema)()
        member _.EnumConstructor(type': Type, schema: EnumSchema) = enumConstructors.[type']
        member _.IsKnownUnionCase(type':Type, recordName:string) = unionConstructors.[type'].ContainsKey recordName
        member _.UnionConstructor(type':Type, recordName:string) =
            match unionConstructors.[type'].TryGetValue recordName with
            | true, ctr -> ctr()
            | _ -> failwithf "union case '%s' for %A not found" recordName type'
        member _.EnumDeconstructor(targetObj: obj) = enumDeconstructors.[targetObj.GetType()]
        member _.ArrayDeconstructor(targetObj: obj) = arrayDeconstructors.[targetObj.GetType()]
        member _.MapDeconstructor(targetObj: obj) = mapDeconstructors.[targetObj.GetType()]
        member _.RecordDeconstructor(targetObj: obj) = recordDeconstructors.[targetObj.GetType()]
        member _.UnionDeconstructor(recordName: string) = unionDeconstructors.[recordName]
        member _.SerializationCast(targetObj: obj): obj -> obj =
            match targetObj with
            | null -> id
            | _ ->
                match serializationCasts.TryGetValue (targetObj.GetType()) with
                | true, cast -> cast
                | _ -> id