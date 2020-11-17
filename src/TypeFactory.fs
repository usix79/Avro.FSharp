namespace Avro.FSharp

open System
open System.IO
open System.Linq
open System.Collections
open System.Collections.Generic
open System.Text
open FSharp.Reflection
open Microsoft.FSharp.Quotations.Patterns

type IObjectConstructor =
    abstract SetKey: key:string -> bool
    abstract ExpectedValueSchema: Schema
    abstract ExpectedValueType: TypeRecord
    abstract SetValue: v:'TValue -> unit
    abstract Construct: unit -> 'TInstance

type internal IEnumDeconstructor =
    abstract Idx: obj -> int

type internal IEnumConstructor =
    abstract Construct: string -> obj

type internal IArrayDeconstructor =
    abstract Size: obj -> int
    abstract Items: obj -> IEnumerable

type IMapDeconstructor =
    abstract Size: obj -> int
    abstract Items: obj -> DictionaryEntry seq

type IRecordDeconstructor =
    abstract Fields: obj -> obj array

type IUnionDeconstructor =
    abstract CaseIdx: obj -> int
    abstract CaseFields: int -> obj -> obj array

type FieldInfo = {
    Idx: int
    Type: TypeRecord
    Schema: RecordFieldSchema
    Default: obj
}

module internal TypeHelpers =

    let getMethodInfo = function
        | Call (_, methodInfo, _) -> methodInfo
        | _ -> failwith "Expression is not a call."

    let valueInstanceCtr (valueType:TypeRecord) (valueSchema: Schema) =
        fun () ->
            let mutable instance:obj = null
            { new IObjectConstructor with
                member _.Construct<'TInstance>() = instance :?> 'TInstance
                member _.ExpectedValueSchema = valueSchema
                member _.ExpectedValueType = valueType
                member _.SetKey(key: string) = failwithf "value constructor is not supposed to set a key. But SetKey is called with key='%s'" key
                member _.SetValue(v: 'TValue) = instance <- v }

    let enumCtr (schema:EnumSchema) (type':Type) =
        let symbols =
            schema.Symbols
            |> Array.map(fun symbol -> symbol, Enum.Parse(type', symbol))
            |> Map.ofArray

        let defValue = schema.Default |> Option.map (fun el -> Enum.Parse(type', el.GetString()))
        { new IEnumConstructor with
            member _.Construct symbol =
                match symbols.TryGetValue symbol with
                | true, v -> v
                | _ -> defValue |> Option.defaultWith (fun () -> failwithf "Enum symbol is not resolved %s" symbol) }

    let enumDctr (schema:EnumSchema) (type':Type) =
        let indexes = Dictionary<obj,int>()
        schema.Symbols |> Array.iteri(fun idx symbol -> indexes.[Enum.Parse(type', symbol)] <- idx)

        {new IEnumDeconstructor with member _.Idx obj = indexes.[obj]}

    let arrayDctr (arrayType:TypeRecord) =
        let sizeFun =
            match arrayType.TargetType with
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


    let arrayCtr (arrayType:TypeRecord) (itemType:TypeRecord) (schema: ArraySchema) =
        let ctr =
            match arrayType.TargetType with
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
            | t when t.GetGenericTypeDefinition() = typedefof<Generic.IEnumerable<_>> ->    // sequence
                let castMi =
                    (getMethodInfo <@ Enumerable.Cast null @>)
                        .GetGenericMethodDefinition()
                        .MakeGenericMethod(t.GetGenericArguments())
                fun obj -> castMi.Invoke(null, [|obj|])
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

        fun () ->
            let values = ArrayList()
            { new IObjectConstructor with
                member _.ExpectedValueSchema = schema.Items
                member _.ExpectedValueType = itemType
                member _.SetKey(key: string) = failwithf "array constructor is not supposed to set a key. But SetKey is called with key='%s'" key
                member _.SetValue(v: 'TArrayItem) = values.Add(v) |> ignore
                member _.Construct<'TInstance>() = ctr values :?> 'TInstance}

    let enumerateDict<'TKey,'TValue> (dict:IDictionary<'TKey,'TValue>) =
        seq { for pair in dict do DictionaryEntry(pair.Key, pair.Value) }

    let mapDeconstructor (type': Type) =
        let sizeFun =
            match type' with
            | t when t.GetGenericTypeDefinition() = typedefof<Map<_,_>> ->
                let mi =
                    typedefof<Map<_,_>>
                        .MakeGenericType(t.GetGenericArguments())
                        .GetProperty("Count")
                        .GetGetMethod()
                fun obj -> mi.Invoke(obj, [||]) |> unbox
            | _ ->
                fun (obj:obj) -> (obj :?> Collections.ICollection).Count

        let itemsFun =
            match type' with
            | t when t.GetGenericTypeDefinition() = typedefof<Map<_,_>> ->
                let mi =
                    (getMethodInfo <@ enumerateDict null @>)
                        .GetGenericMethodDefinition()
                        .MakeGenericMethod(t.GetGenericArguments())
                fun obj -> mi.Invoke(null, [|obj|]) :?> Collections.DictionaryEntry seq
            | _ ->
                fun (obj:obj) ->
                    seq { for item in (obj :?> Collections.IDictionary) do item :?> DictionaryEntry}

        { new IMapDeconstructor with
             member _.Size (obj:obj) = sizeFun obj
             member _.Items (obj:obj) =  itemsFun obj}

    let castSeqToDictionary<'TValue> (array: IEnumerable<KeyValuePair<string,obj>>) =
        let dict = Dictionary<string, 'TValue>()
        for pair in array do
            dict.[pair.Key] <- pair.Value :?> 'TValue
        dict

    let castSeqToMap<'TValue> (array: IEnumerable<KeyValuePair<string,obj>>) =
        array
        |> Seq.map(fun pair -> pair.Key, pair.Value :?> 'TValue)
        |> Map.ofSeq

    let mapConstructor (type':Type) (valueType: TypeRecord) (schema: MapSchema) =
        let ctr =
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

        fun () ->
            let values = Dictionary<string,obj>()
            let mutable currentKey = ""
            { new IObjectConstructor with
                member _.SetKey(key: string) = currentKey <- key; true
                member _.ExpectedValueSchema = schema.Values
                member _.ExpectedValueType = valueType
                member _.SetValue(v: 'TMapValue) = values.Add(currentKey, v)
                member _.Construct<'TInstance>() = ctr(values:>obj) :?> 'TInstance}

    let recordDeconstructor (type':Type) =
        let fieldsFun =
            if FSharpType.IsRecord type' then FSharpValue.PreComputeRecordReader type'
            else if FSharpType.IsTuple type' then FSharpValue.PreComputeTupleReader type'
            else failwithf "Not supported record type %A" type'

        { new IRecordDeconstructor with
            member _.Fields (obj:obj) = fieldsFun obj}

    let recordConstructor (ctr: obj array -> obj) (fields: FieldInfo array) (schema: RecordSchema) =
        let fieldsMap =
            fields
            |> Array.collect (fun info -> [|
                    info.Schema.Name,info
                    for alias in info.Schema.Aliases do alias, info|])
            |> Map.ofArray

        let defs = fields |> Array.choose (fun info -> if isNull info.Default then None else Some (info.Idx,info.Default))

        let ctr =
            fun (fieldValues: obj[]) ->
                defs |> Array.iter (fun (idx,v) -> if isNull fieldValues.[idx] then fieldValues.[idx] <- v)
                ctr fieldValues

        fun () ->
            let values = Array.zeroCreate schema.Fields.Count
            let mutable currentField = Unchecked.defaultof<FieldInfo>
            { new IObjectConstructor with
                member _.SetKey(key: string) =
                    match fieldsMap.TryGetValue key with
                    | true, field ->  currentField <- field; true
                    | _ -> false
                member _.ExpectedValueSchema = currentField.Schema.Type
                member _.ExpectedValueType = currentField.Type
                member _.SetValue(v: 'TFieldValue1) = values.[currentField.Idx] <- v :> obj
                member _.Construct<'TInstance>() = ctr(values) :?> 'TInstance}

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

    let optionConstructor (ctr: obj -> obj) (someType: TypeRecord) (schema: Schema) =
        fun () ->
            let mutable instance = null
            { new IObjectConstructor with
                member _.SetKey(_: string) = true
                member _.ExpectedValueSchema = schema
                member _.ExpectedValueType = someType
                member _.SetValue(v: 'TOptionValue) =  instance <- ctr v
                member _.Construct<'TInstance>() = instance :?> 'TInstance}

open TypeHelpers

type internal DefaultValueCreator = TypeRecord -> Schema -> Json.JsonElement -> obj

type internal TypeFactory(targetType:TypeRecord, targetSchema:Schema, defValueCreator:DefaultValueCreator) =

    let enumConstructors = Dictionary<Type,IEnumConstructor>()
    let enumDeconstructors = Dictionary<Type, IEnumDeconstructor>()
    let arrayDeconstructors = Dictionary<Type, IArrayDeconstructor>()
    let mapDeconstructors = Dictionary<Type, IMapDeconstructor>()
    let recordDeconstructors = Dictionary<Type, IRecordDeconstructor>()
    let constructors = Dictionary<Type, unit -> IObjectConstructor>()
    let unionConstructors = Dictionary<Type, Dictionary<string, unit -> IObjectConstructor>>()
    let unionDeconstructors = Dictionary<Type, IUnionDeconstructor>()
    let optionConstructors = Dictionary<Type, unit -> IObjectConstructor>()
    let optionDeconstructors = Dictionary<Type, obj -> obj>()


    let fieldsInfo (fieldsTypes:TypeRecord array) (schemas:ResizeArray<RecordFieldSchema>) =
        [|for idx in 0 .. fieldsTypes.Length - 1 do
            let fieldType = fieldsTypes.[idx]
            let schema = schemas.[idx]
            {
                Idx = idx
                Type = fieldType
                Schema = schema
                Default =
                    match schema.Default with
                    | Some el -> defValueCreator fieldType schema.Type el
                    | None -> null
            } |]

    let rec prepare (schema:Schema) (tr:TypeRecord)  =
        if not (constructors.ContainsKey tr.TargetType) then
            match schema, tr.TypeInfo with
            | Enum schema, TypeInfo.Enum (type', _) ->
                enumConstructors.[type'] <- enumCtr schema type'
                enumDeconstructors.[type'] <- enumDctr schema type'
            | Array schema, TypeInfo.Array itemType
            | Array schema, TypeInfo.List itemType
            | Array schema, TypeInfo.Set itemType
            | Array schema, TypeInfo.ResizeArray itemType
            | Array schema, TypeInfo.HashSet itemType
            | Array schema, TypeInfo.Seq itemType ->
                constructors.[tr.TargetType] <- arrayCtr tr itemType schema
                arrayDeconstructors.[tr.TargetType] <- arrayDctr tr
                prepare schema.Items itemType
            | Map schema, TypeInfo.Map (_, valueType)
            | Map schema, TypeInfo.Dictionary (_, valueType) ->
                constructors.[tr.TargetType] <- mapConstructor tr.TargetType valueType schema
                mapDeconstructors.[tr.TargetType] <- mapDeconstructor tr.TargetType
                prepare schema.Values valueType
            | Record schema, TypeInfo.Record (_, fieldsRecords) ->
                let fieldsTypes =
                    fieldsRecords
                    |> Seq.map (fun r -> r.FieldType)
                    |> Array.ofSeq
                let fields = fieldsInfo fieldsTypes schema.Fields
                let ctr = FSharpValue.PreComputeRecordConstructor tr.TargetType
                constructors.[tr.TargetType] <- recordConstructor ctr fields schema
                recordDeconstructors.[tr.TargetType] <- recordDeconstructor tr.TargetType
                fields |> Array.iter(fun f -> prepare f.Schema.Type f.Type)
            | Record schema, TypeInfo.Tuple itemsTypes ->
                let fields = fieldsInfo itemsTypes schema.Fields
                let ctr = FSharpValue.PreComputeTupleConstructor tr.TargetType
                constructors.[tr.TargetType] <- recordConstructor ctr fields schema
                recordDeconstructors.[tr.TargetType] <- recordDeconstructor tr.TargetType
                fields |> Array.iter(fun f -> prepare f.Schema.Type f.Type)
            | Union schemas, TypeInfo.Union (_,cases) ->
                if not (unionConstructors.ContainsKey tr.TargetType) then
                    let dict = Dictionary<string, unit -> IObjectConstructor>()
                    schemas
                    |> Array.iteri (fun idx -> function
                        | (Record schema) ->
                            let caseRecord = cases.[idx]
                            let ctr = FSharpValue.PreComputeUnionConstructor caseRecord.Info
                            let fields = fieldsInfo caseRecord.CaseTypes schema.Fields
                            let instanceCtr = recordConstructor ctr fields schema

                            dict.[schema.Name] <- instanceCtr
                            for alias in schema.Aliases do dict.[alias] <- instanceCtr
                        | _ -> ())
                    unionConstructors.[tr.TargetType] <- dict
                    unionDeconstructors.[tr.TargetType] <- unionDeconstructor tr.TargetType
                    schemas |> Array.iteri (fun idx -> function
                        | Record schema ->
                            let caseRecord = cases.[idx]
                            schema.Fields |> Seq.iteri(fun idx fieldSchema -> prepare fieldSchema.Type caseRecord.CaseTypes.[idx])
                        | _ -> ())
            | Union _, TypeInfo.Option someType ->
                let someUci = (FSharpType.GetUnionCases tr.TargetType).[1] // Option.Some
                let someCtr = FSharpValue.PreComputeUnionConstructor someUci
                let ctr = fun v -> someCtr [|v|]
                optionConstructors.[tr.TargetType] <- optionConstructor ctr someType schema
                let someReader = FSharpValue.PreComputeUnionReader someUci
                optionDeconstructors.[tr.TargetType] <- fun (obj:obj) -> (someReader obj).[0]
            | _ -> ()

    do prepare targetSchema targetType

    member _.TargetType = targetType
    member _.TargetSchema = targetSchema

    member _.EnumConstructor(type':Type) = enumConstructors.[type']
    member _.EnumDeconstructor(type':Type) = enumDeconstructors.[type']

    member _.ArrayDeconstructor(type':Type) = arrayDeconstructors.[type']
    member _.MapDeconstructor(type':Type) = mapDeconstructors.[type']
    member _.RecordDeconstructor(type':Type) = recordDeconstructors.[type']

    member _.Constructor(type':Type) = constructors.[type']()

    member _.IsKnownUnionCase(type':Type, recordName:string) =  unionConstructors.[type'].ContainsKey recordName
    member _.UnionConstructor(type':Type, recordName:string) = unionConstructors.[type'].[recordName]()
    member _.UnionDeconstructor(type': Type) = unionDeconstructors.[type']
    member _.UnionStub(customRules:CustomRule list, tr:TypeRecord, schema:Schema) =
        let el = Schema.stub Schema.defaultOptions customRules tr.TypeInfo
        defValueCreator tr schema el

    member _.OptionConstructor(type':Type) = optionConstructors.[type']()
    member _.OptionDeconstructor(type':Type) = optionDeconstructors.[type']

    member _.ValueConstructor(tr: TypeRecord, schema: Schema) = (valueInstanceCtr tr schema)()