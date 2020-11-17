namespace Avro.FSharp

open System
open FSharp.Reflection
open System.Reflection
open Avro.FSharp.Utils

[<RequireQualifiedAccess>]
type TypeInfo =
    | Unit
    | Bool
    | String
    | Byte
    | Short
    | Int32
    | Long
    | UInt16
    | UInt32
    | UInt64
    | Float32
    | Float
    | Decimal
    | BigInt
    | DateTime
    | DateTimeOffset
    | TimeSpan
    | Guid
    | Array of TypeRecord
    | List of TypeRecord
    | Set of TypeRecord
    | ResizeArray of TypeRecord
    | HashSet of TypeRecord
    | Seq of TypeRecord
    | Map of TypeRecord * TypeRecord
    | Dictionary of TypeRecord * TypeRecord
    | Enum of (Type*TypeRecord)
    | Record of (Type*ResizeArray<FieldRecord>)
    | Tuple of TypeRecord [ ]
    | Option of TypeRecord
    | Union of (Type*ResizeArray<UnionCaseRecord>)
    | Any of Type
and FieldRecord =
  { FieldName: string
    FieldType: TypeRecord
    PropertyInfo : PropertyInfo }
and UnionCaseRecord =
  { CaseName: string
    CaseTypes: TypeRecord [ ]
    Info: UnionCaseInfo }
and TypeRecord =
  { TargetType: Type
    TypeInfo: TypeInfo}

[<RequireQualifiedAccess>]
module TypeRecord =

    let typeRecord type' ti = {TargetType = type'; TypeInfo = ti}

    let (|PrimitiveType|_|) (type': Type) =
        match type' with
        | t when t = typeof<unit> -> TypeInfo.Unit |> Some
        | t when t = typeof<bool> -> TypeInfo.Bool |> Some
        | t when t = typeof<string> -> TypeInfo.String |> Some
        | t when t = typeof<byte> -> TypeInfo.Byte |> Some
        | t when t = typeof<int16> -> TypeInfo.Short |> Some
        | t when t = typeof<int> -> TypeInfo.Int32 |> Some
        | t when t = typeof<int64> -> TypeInfo.Long |> Some
        | t when t = typeof<uint16> -> TypeInfo.UInt16 |> Some
        | t when t = typeof<uint32> -> TypeInfo.UInt32 |> Some
        | t when t = typeof<uint64> -> TypeInfo.UInt64 |> Some
        | t when t = typeof<float32> -> TypeInfo.Float32 |> Some
        | t when t = typeof<float> -> TypeInfo.Float |> Some
        | t when t = typeof<decimal> -> TypeInfo.Decimal |> Some
        | t when t = typeof<Numerics.BigInteger> -> TypeInfo.BigInt |> Some
        | t when t = typeof<DateTime> -> TypeInfo.DateTime |> Some
        | t when t = typeof<DateTimeOffset> -> TypeInfo.DateTimeOffset |> Some
        | t when t = typeof<TimeSpan> -> TypeInfo.TimeSpan |> Some
        | t when t = typeof<Guid>-> TypeInfo.Guid |> Some
        | _ -> None

    let (|ArrayType|_|) (t:Type) =
        t.IsArray
        |> Option.ofBool (fun () -> t.GetElementType())

    let (|ListType|_|) (t: Type) =
        (t.IsGenericType && (t.GetGenericTypeDefinition() = typedefof<List<_>>))
        |> Option.ofBool (fun () -> t.GetGenericArguments().[0] )

    let (|SetType|_|) (t: Type) =
        (t.IsGenericType && (t.GetGenericTypeDefinition() = typedefof<Set<_>>))
        |> Option.ofBool (fun () -> t.GetGenericArguments().[0])

    let (|ResizeArrayType|_|) (t: Type) =
        (t.IsGenericType && (t.GetGenericTypeDefinition() = typedefof<ResizeArray<_>>))
        |> Option.ofBool (fun () -> t.GetGenericArguments().[0])

    let (|HashSetType|_|) (t: Type) =
        (t.IsGenericType && (t.GetGenericTypeDefinition() = typedefof<Collections.Generic.HashSet<_>>))
        |> Option.ofBool (fun () -> t.GetGenericArguments().[0])

    let (|SeqType|_|) (t: Type) =
        (t.IsGenericType && (t.GetGenericTypeDefinition() = typedefof<Collections.Generic.IEnumerable<_>>))
        |> Option.ofBool(fun () -> t.GetGenericArguments().[0])
        |> Option.orElseWith (fun () ->
            t.GetInterfaces()
            |> Array.tryFind (fun it -> it.IsGenericType && it.GetGenericTypeDefinition() = typedefof<Collections.Generic.IEnumerable<_>>)
            |> Option.map (fun it -> it.GetGenericArguments().[0]))

    let (|MapType|_|) (t: Type) =
        (t.IsGenericType && (t.GetGenericTypeDefinition() = typedefof<Map<_,_>>))
        |> Option.ofBool (fun () -> let genArgs = t.GetGenericArguments() in genArgs.[0], genArgs.[1])

    let (|DictionaryType|_|) (t: Type) =
        (t.IsGenericType && (t.GetGenericTypeDefinition() = typedefof<Collections.Generic.Dictionary<_,_>>))
        |> Option.ofBool (fun () -> let genArgs = t.GetGenericArguments() in genArgs.[0], genArgs.[1])

    let (|RecordType|_|) (t: Type) =
        FSharpType.IsRecord t
        |> Option.ofBool (fun () -> FSharpType.GetRecordFields t)

    let (|TupleType|_|) (t: Type) =
        FSharpType.IsTuple t
        |> Option.ofBool (fun () -> FSharpType.GetTupleElements(t))

    let (|OptionType|_|) (t:Type) =
        (t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<Option<_>>)
        |> Option.ofBool (fun () -> t.GetGenericArguments().[0])

    let (|UnionType|_|) (t: Type) =
        FSharpType.IsUnion t
        |> Option.ofBool (fun () -> FSharpType.GetUnionCases t)

    let (|EnumType|_|) (t: Type) =
        t.IsEnum
        |> Option.ofBool (fun () -> t.GetEnumUnderlyingType())

    let create (resolvedType: Type) : TypeRecord =
        let cache = Collections.Generic.Dictionary<Type, TypeRecord>()

        let rec parse resolvedType =
            match cache.TryGetValue resolvedType with
            | true, r -> r
            | _ ->
                let r =
                    match resolvedType with
                    | PrimitiveType ti -> typeRecord resolvedType  ti
                    | ArrayType elemType ->  TypeInfo.Array (parse elemType) |> typeRecord resolvedType
                    | ListType elemType -> TypeInfo.List (parse elemType) |> typeRecord resolvedType
                    | SetType elemType -> TypeInfo.Set (parse elemType) |> typeRecord resolvedType
                    | ResizeArrayType elemType -> TypeInfo.ResizeArray (parse elemType) |> typeRecord resolvedType
                    | HashSetType elemType -> TypeInfo.HashSet (parse elemType) |> typeRecord resolvedType
                    | MapType (keyType, valueType) -> TypeInfo.Map (parse keyType, parse valueType) |> typeRecord resolvedType
                    | DictionaryType (keyType, valueType) -> TypeInfo.Dictionary (parse keyType, parse valueType) |> typeRecord resolvedType
                    | SeqType elemType -> TypeInfo.Seq (parse elemType) |> typeRecord resolvedType     // should be afer all other arrays and maps
                    | EnumType underlyingType -> TypeInfo.Enum (resolvedType, parse underlyingType) |> typeRecord resolvedType
                    | RecordType fields ->
                        match cache.TryGetValue resolvedType with
                        | true, ti -> ti
                        | _ ->
                            let fieldsInfo = ResizeArray<FieldRecord>()
                            let recordTypeInfo = TypeInfo.Record (resolvedType, fieldsInfo) |> typeRecord resolvedType
                            cache.[resolvedType] <- recordTypeInfo
                            fields
                            |> Array.iter(fun pi ->
                                {   FieldName = pi.Name
                                    FieldType = (parse pi.PropertyType)
                                    PropertyInfo = pi}
                                |> fieldsInfo.Add    )
                            recordTypeInfo
                    | TupleType types -> TypeInfo.Tuple (Array.map parse types) |> typeRecord resolvedType
                    | OptionType elemType -> TypeInfo.Option (parse elemType)|> typeRecord resolvedType
                    | UnionType cases ->
                        match cache.TryGetValue resolvedType with
                        | true, ti -> ti
                        | _ ->
                            let casesInfo = ResizeArray<UnionCaseRecord>()
                            let unionTypeInfo = TypeInfo.Union (resolvedType, casesInfo) |> typeRecord resolvedType
                            cache.[resolvedType] <- unionTypeInfo
                            cases
                            |> Array.iter(fun uci ->
                                {   CaseName = uci.Name
                                    CaseTypes = uci.GetFields() |> Array.map(fun pi -> parse pi.PropertyType)
                                    Info = uci}
                                |> casesInfo.Add)
                            unionTypeInfo
                    | _ -> TypeInfo.Any resolvedType |> typeRecord resolvedType
                cache.[resolvedType] <- r
                r

        parse resolvedType