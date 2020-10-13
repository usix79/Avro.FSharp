namespace Avro.FSharp

open System
open System.Linq
open System.Collections.Generic
open Avro
open FSharp.Reflection
open Microsoft.FSharp.Quotations.Patterns

module internal Reflector =

    type RecordInfo = {
        Name: string
        Creator: obj array -> obj
        FieldValues: obj -> obj []
        FieldTypes : Dictionary<string, Type>
    }

    type UnionCaseInfo = {
        SchemaName: string
        GetCaseIdx: obj -> int
    }

    type EnumInfo = {
        Name: string
        Symbols: Dictionary<string, obj>
    }

    type ArrayInfo = {
        GetSize: obj -> int
        Items: obj -> Collections.IEnumerable
    }

    type MapInfo = {
        GetSize: obj -> int
        Items: obj -> Collections.DictionaryEntry seq
    }


    let tryGetValue<'TKey,'TValue> (dict:IDictionary<'TKey,'TValue>) key (desctiption:string->string)  =
        match dict.TryGetValue key with
        | true, value' -> value'
        | _ -> failwith <| desctiption (sprintf "%A" key)

    let getMethodInfo = function
        | Call (_, methodInfo, _) -> methodInfo
        | _ -> failwith "Expression is not a call."

    let enumerateDict<'TKey,'TValue> (dict:IDictionary<'TKey,'TValue>) =
        seq{
            for pair in dict do
                Collections.DictionaryEntry(pair.Key, pair.Value)
        }

    let castSeqToDictionary<'TValue> (array:IEnumerable<KeyValuePair<string,obj>>) =
        let dict = Dictionary<string, 'TValue>()
        for pair in array do
            dict.[pair.Key] <- pair.Value :?> 'TValue
        dict

    let castSeqToMap<'TValue> (array:IEnumerable<KeyValuePair<string,obj>>) =
        array
        |> Seq.map(fun pair -> pair.Key, pair.Value :?> 'TValue)
        |> Map.ofSeq        

open Reflector

type SchemaReflector() =
    let records = Dictionary<string, RecordInfo>()
    let enums = Dictionary<string, EnumInfo>()
    let arrays = Dictionary<Type, ArrayInfo>()
    let maps = Dictionary<Type, MapInfo>()
    let unionCases = Dictionary<string, UnionCaseInfo>()
    let writeCasts = Dictionary<Type, obj -> obj>()
    let readCasts = Dictionary<Type, obj -> obj>()

    member _.AddWriteCast originalType cast =
        writeCasts.[originalType] <- cast

    member _.WriteCast obj =
        match writeCasts.TryGetValue (obj.GetType()) with
        | true, cast -> cast obj
        | _ -> obj

    member _.AddReadCast targetType cast =
        readCasts.[targetType] <- cast

    member _.ReadCast targetType =
        match readCasts.TryGetValue targetType with
        | true, cast -> cast
        | _ -> id

    member private _.CreateConstructor (fieldsType:Type []) (ctr:obj[]->obj) =
        let casts =
            fieldsType
            |> Array.mapi (fun idx t -> idx,t)
            |> Array.choose (fun (idx, t) -> 
                match readCasts.TryGetValue t with
                | true, cast -> Some (idx,cast)
                | _ -> 
                    None)

        fun (fieldValues:obj[]) -> 
            casts |> Array.iter (fun (idx,cast) -> fieldValues.[idx] <- cast fieldValues.[idx])
            ctr fieldValues

    member this.AddRecord name type' =
        if FSharpType.IsRecord type' then
            let fieldsPI =  FSharpType.GetRecordFields type' 
            let info = {
                Name = name
                Creator =
                    let fieldsType = fieldsPI |> Array.map (fun pi -> pi.PropertyType)
                    this.CreateConstructor fieldsType (FSharpValue.PreComputeRecordConstructor type')
                FieldValues = FSharpValue.PreComputeRecordReader type'
                FieldTypes =
                    fieldsPI 
                    |> Array.map (fun pi -> pi.Name, pi.PropertyType)
                    |> Map.ofArray
                    |> Dictionary
            }
            records.Add(name, info)
        else if FSharpType.IsTuple type' then
            let fieldsType=  FSharpType.GetTupleElements type' 
            let info = {
                Name = name
                Creator = this.CreateConstructor fieldsType (FSharpValue.PreComputeTupleConstructor type')
                FieldValues = FSharpValue.PreComputeTupleReader type'
                FieldTypes =
                    fieldsType 
                    |> Array.mapi (fun idx t -> sprintf "Item%d" (idx+1), t)
                    |> Map.ofArray
                    |> Dictionary
            }
            records.Add(name, info)
        else failwithf "Type is not supported as record %s" type'.Name


    member private _.GetRecordInfo name =
        sprintf "looking for record's reflection: %s"
        |> tryGetValue records name

    member this.CreateRecord recordName args =
        (this.GetRecordInfo recordName).Creator args

    member this.GetFieldType recordName fieldName =
        sprintf "looking for type of %s.%s" recordName
        |> tryGetValue (this.GetRecordInfo recordName).FieldTypes fieldName

    member this.GetFieldValues recordName =
        (this.GetRecordInfo recordName).FieldValues

    member this.AddUnionCase schemaName caseName type' = 
        let idxMap =
            FSharpType.GetUnionCases(type')
            |> Array.mapi (fun idx uci -> uci.Tag,idx)
            |> Map.ofArray

        let caseInfo = {
            SchemaName = schemaName
            GetCaseIdx = 
                let getTag = FSharpValue.PreComputeUnionTagReader (type')
                fun obj -> idxMap.[getTag obj]
        }
        unionCases.Add(schemaName, caseInfo)

        let uci =             
            FSharpType.GetUnionCases(type')
            |> Array.find (fun uci -> uci.Name = caseName)

        let fieldsPI = uci.GetFields()
        let recordInfo = {
            Name = schemaName
            Creator =  
                let fieldsType = fieldsPI |> Array.map (fun pi -> pi.PropertyType)                
                this.CreateConstructor fieldsType (FSharpValue.PreComputeUnionConstructor uci)
            FieldValues = FSharpValue.PreComputeUnionReader uci
            FieldTypes =
                fieldsPI
                |> Array.map (fun pi -> pi.Name, pi.PropertyType)
                |> Map.ofArray
                |> Dictionary
        }
        records.Add(schemaName, recordInfo)

    member private _.GetUnionCaseInfo name =
        sprintf "looking for union case's reflection: %s"
        |> tryGetValue unionCases name

    member this.GetCaseIdx schemaName obj =
        (this.GetUnionCaseInfo schemaName).GetCaseIdx obj

    member _.AddEnum name type' =         
        let info = {
            Name = name
            Symbols = 
                (Enum.GetValues type').Cast<obj>().ToArray()
                |> Array.map (fun v -> (v.ToString()), v)
                |> Map.ofArray
                |> Dictionary
        }
        enums.Add(name, info)

    member private _.GetEnumInfo name =     
        sprintf "looking for enum: %s"
        |> tryGetValue enums name

    member this.GetEnumValue enumName symbol =
        sprintf "looking for enum value %s.%s" enumName
        |> tryGetValue (this.GetEnumInfo enumName).Symbols symbol

    member _.AddArray (type':Type) =
        let info : ArrayInfo =
            {
                GetSize = 
                    match type' with
                    | t when t.IsArray -> 
                        fun obj -> (obj :?> Array).Length
                    | t when typeof<Collections.ICollection>.IsAssignableFrom(t) -> 
                        fun obj -> (obj :?> Collections.ICollection).Count
                    | _ ->
                        fun obj -> 
                            let mutable count = 0
                            for _ in obj :?> Collections.IEnumerable do count <- count + 1
                            count                
                Items = fun obj -> obj :?> Collections.IEnumerable
            }
        arrays.Add(type', info)

        let readCast =
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

        readCasts.Add(type', readCast)


    member private _.GetArrayInfo arrayType =     
        sprintf "looking for array: %s"
        |> tryGetValue arrays arrayType

    member this.GetArraySize obj =
        let arrayType = obj.GetType()
        (this.GetArrayInfo arrayType).GetSize obj

    member this.GetArrayItems obj =
        let arrayType = obj.GetType()
        (this.GetArrayInfo arrayType).Items obj

    member _.AddMap (type':Type) =
        let info : MapInfo =
            {
                GetSize = 
                    match type' with
                    | t when typeof<Collections.ICollection>.IsAssignableFrom(t) -> 
                        fun obj -> (obj :?> Collections.ICollection).Count
                    | t when t.GetGenericTypeDefinition() = typedefof<Map<_,_>> ->
                        let mi = 
                            typedefof<Map<_,_>>
                                .MakeGenericType(t.GetGenericArguments())
                                .GetProperty("Count")
                                .GetGetMethod()
                        fun obj -> mi.Invoke(obj, [||]) |> unbox
                    | _ ->
                        fun obj -> 
                            let mutable count = 0
                            for _ in obj :?> Collections.IEnumerable do count <- count + 1
                            count                
                Items = 
                    match type' with
                    | t when typeof<Collections.IDictionary>.IsAssignableFrom(t) -> 
                        fun obj -> 
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
            }
        maps.Add(type', info)
    
        let readCast = 
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
        readCasts.Add(type', readCast)

    member private _.GetMapInfo mapType =     
        sprintf "looking for map: %s"
        |> tryGetValue maps mapType

    member this.GetMapSize obj =
        let mapType = obj.GetType()
        (this.GetMapInfo mapType).GetSize obj

    member this.GetMapItems obj =
        let mapType = obj.GetType()
        (this.GetMapInfo mapType).Items obj

    member _.AddNullable (type':Type) =
        let casesInfo = FSharpType.GetUnionCases type'
        let noneUci = casesInfo.[0]
        let someUci = casesInfo.[1]

        let someReader = FSharpValue.PreComputeUnionReader someUci
        let writeCast obj = (someReader obj).[0]

        writeCasts.Add(type', writeCast)

        let someCtr = FSharpValue.PreComputeUnionConstructor someUci
        let noneCtr = FSharpValue.PreComputeUnionConstructor noneUci

        let readCast obj =
            match obj with
            | null -> noneCtr [||]
            | _ -> someCtr [|obj|]

        readCasts.Add(type', readCast)
