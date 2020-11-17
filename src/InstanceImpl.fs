namespace Avro.FSharp

open System
open System.Collections.Generic
open System.Globalization

type SerializationOptions = {
    CustomRules: CustomRule list
}

module internal InstanceDirector =

    let create (options:SerializationOptions) (typeRecord:TypeRecord) (schema:Schema) (factory:TypeFactory) =
        let rulesDict = Dictionary<Type, {|Cast:obj->obj;Type:TypeRecord|}>()
        options.CustomRules |> List.iter(fun rule ->
            rulesDict.[rule.InstanceType] <- {|Cast=rule.CastToSurrogate; Type= TypeRecord.create rule.SurrogateType|})

        fun (instance:obj) (builder:IAvroBuilder) ->
            let rec write (tr:TypeRecord) (schema:Schema) (obj:obj) =
                match schema, tr.TypeInfo with
                | Boolean, TypeInfo.Bool _ -> builder.Boolean(unbox obj)
                | String, TypeInfo.String _ -> if isNull obj then builder.Null() else builder.String(unbox obj)
                | Int, TypeInfo.Int32 _ -> builder.Int (unbox obj)
                | Long, TypeInfo.Long _ -> builder.Long (unbox obj)
                | Float, TypeInfo.Float32 _ -> builder.Float (unbox obj)
                | Double, TypeInfo.Float _ -> builder.Double (unbox obj)
                | Bytes, TypeInfo.Array {TypeInfo = TypeInfo.Byte} -> builder.Bytes (unbox obj)
                | Int, TypeInfo.Byte _ -> builder.Int (unbox<byte> obj |> int)
                | Int, TypeInfo.Short _ -> builder.Int (unbox<int16> obj |> int)
                | Int, TypeInfo.UInt16 _ -> builder.Int (unbox<uint16> obj |> int)
                | Int, TypeInfo.UInt32 _ -> builder.Int (unbox<uint32> obj |> int)
                | Long, TypeInfo.UInt64 _ -> builder.Long (unbox<uint64> obj |> int64)
                | Enum schema, TypeInfo.Enum (type', _) ->
                    let idx = factory.EnumDeconstructor(type').Idx obj
                    builder.Enum(idx, schema.Symbols.[idx])
                | Array schema, TypeInfo.Array itemType
                | Array schema, TypeInfo.ResizeArray itemType
                | Array schema, TypeInfo.HashSet itemType
                | Array schema, TypeInfo.List itemType
                | Array schema, TypeInfo.Set itemType
                | Array schema, TypeInfo.Seq itemType ->
                    let dctr = factory.ArrayDeconstructor tr.TargetType
                    builder.StartArray()
                    builder.StartArrayBlock(int64(dctr.Size(obj)))
                    for item in dctr.Items(obj) do write itemType schema.Items item
                    builder.EndArray()
                | Map schema, TypeInfo.Map (_,valueType)
                | Map schema, TypeInfo.Dictionary (_,valueType) ->
                    let dctr = factory.MapDeconstructor tr.TargetType
                    builder.StartMap()
                    builder.StartMapBlock(int64(dctr.Size obj))
                    for pair in dctr.Items obj do
                        builder.Key(pair.Key.ToString())
                        write valueType schema.Values pair.Value
                    builder.EndMap()
                | Record schema, TypeInfo.Record (_,fields) ->
                    let dctr = factory.RecordDeconstructor tr.TargetType
                    builder.StartRecord()
                    dctr.Fields obj
                    |> Array.iteri(fun idx obj ->
                        let fieldSchema = schema.Fields.[idx]
                        let fieldRecord = fields.[idx]
                        if builder.Field fieldSchema.Name then
                            write fieldRecord.FieldType fieldSchema.Type obj)
                    builder.EndRecord()
                | Record schema, TypeInfo.Tuple itemTypes ->
                    let dctr = factory.RecordDeconstructor tr.TargetType
                    builder.StartRecord()
                    dctr.Fields obj
                    |> Array.iteri(fun idx obj ->
                        let fieldSchema = schema.Fields.[idx]
                        if builder.Field fieldSchema.Name then
                            write itemTypes.[idx] fieldSchema.Type obj)
                    builder.EndRecord()
                | Union schemas, TypeInfo.Union (_,casesRecords) ->
                    let dctr = factory.UnionDeconstructor tr.TargetType
                    let idx = dctr.CaseIdx obj
                    match schemas.[idx] with
                    | Record recordSchema ->
                        if builder.StartUnionCase(idx,recordSchema.Name) then
                            let caseRecord = casesRecords.[idx]
                            builder.StartRecord()
                            dctr.CaseFields idx obj
                            |> Array.iteri(fun idx obj ->
                                let field = recordSchema.Fields.[idx]
                                if builder.Field field.Name then
                                    write caseRecord.CaseTypes.[idx] field.Type obj)
                            builder.EndRecord()
                            builder.EndUnionCase()
                    | wrongSchema -> failwithf "Union should consist of the record's schemas, but wrong schema found: %A" wrongSchema
                | Union [|Null;someSchema|], TypeInfo.Option someType ->
                    match obj with
                    | null -> builder.NoneCase()
                    | _ ->
                        builder.StartSomeCase someSchema
                        factory.OptionDeconstructor tr.TargetType obj
                        |> write someType someSchema
                        builder.EndSomeCase()

                | Decimal schema, TypeInfo.Decimal _ -> builder.Decimal(unbox<decimal> obj, schema)
                | Double, TypeInfo.Decimal _ -> builder.Double (unbox<decimal> obj |> float)    // if treat decimal as double
                | Fixed schema, TypeInfo.Guid _ ->
                    builder.Fixed ((unbox<Guid> obj).ToByteArray())
                | String, TypeInfo.Guid _ -> builder.String ((unbox<Guid> obj).ToString())    // if treat guid as string
                | Bytes, TypeInfo.BigInt _ -> builder.Bytes ((unbox<bigint> obj).ToByteArray())    // if treat guid as string
                | String, TypeInfo.BigInt _ -> builder.String ((unbox<bigint> obj).ToString())    // if treat guid as string
                | String, TypeInfo.DateTime _ -> (unbox<DateTime> obj).ToString("O", CultureInfo.InvariantCulture) |> builder.String
                | String, TypeInfo.DateTimeOffset _ -> (unbox<DateTimeOffset> obj).ToString("O", CultureInfo.InvariantCulture) |> builder.String
                | Int, TypeInfo.TimeSpan _ -> (int)(unbox<TimeSpan> obj).TotalMilliseconds |> builder.Int

                | Fixed schema, _ -> builder.Fixed (unbox obj)
                | _, TypeInfo.Any type' ->
                    match rulesDict.TryGetValue type' with
                    | true, rule ->
                        rule.Cast obj
                        |> write rule.Type schema
                    | _ -> failwithf "type is not supported, add build in rule: %A" type'
                | wrong -> failwithf "wrong combination %A" wrong

            builder.Start()
            write  typeRecord schema instance
            builder.End()

type DeserializationOptions = {
    CustomRules: CustomRule list
    EvolutionTolerantMode: bool
}

type internal InstanceBuilder(options:DeserializationOptions, factory:TypeFactory) =
    let rulesDict = Dictionary<Type, {|Cast:obj->obj|}>()
    do options.CustomRules |> List.iter(fun rule ->
        rulesDict.[rule.InstanceType] <- {|Cast=rule.CastFromSurrogate|})

    let stack = Stack<IObjectConstructor>()
    let mutable instance = Unchecked.defaultof<obj>

    member _.Instance = instance

    interface IAvroBuilder with
        member _.Start() =
            stack.Clear()
            stack.Push(factory.ValueConstructor(factory.TargetType, factory.TargetSchema))

        member this.Null() = this.SetValue(null)
        member this.Boolean(v: bool) = this.SetValue(v)

        member this.String(v: string) =
            match this.ExpectedType.TypeInfo with
            | TypeInfo.String -> this.SetValue(v)
            | TypeInfo.Guid -> Guid.Parse v |> this.SetValue
            | TypeInfo.DateTime -> DateTime.Parse(v, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) |> this.SetValue
            | TypeInfo.DateTimeOffset -> DateTimeOffset.Parse(v, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) |> this.SetValue
            | TypeInfo.BigInt -> bigint.Parse v |> this.SetValue
            | _ -> this.SetValue(v)

        member this.Int(v: int) =
            match this.ExpectedType.TypeInfo with
            | TypeInfo.Int32 -> this.SetValue(v)
            | TypeInfo.Long -> this.SetValue(int64 v)
            | TypeInfo.TimeSpan -> TimeSpan.FromMilliseconds((float)v) |> this.SetValue
            | TypeInfo.Byte -> this.SetValue(byte v)
            | TypeInfo.Short -> this.SetValue(int16 v)
            | TypeInfo.UInt16 -> this.SetValue(uint16 v)
            | TypeInfo.UInt32 -> this.SetValue(uint32 v)
            | TypeInfo.UInt64 -> this.SetValue(uint64 v)
            | _ -> this.SetValue v

        member this.Long(v: int64) =
            match this.ExpectedType.TypeInfo with
            | TypeInfo.Int32 -> this.SetValue(int v)
            | TypeInfo.Long -> this.SetValue(v)
            | TypeInfo.Byte -> this.SetValue(byte v)
            | TypeInfo.Short -> this.SetValue(int16 v)
            | TypeInfo.UInt16 -> this.SetValue(uint16 v)
            | TypeInfo.UInt32 -> this.SetValue(uint32 v)
            | TypeInfo.UInt64 -> this.SetValue(uint64 v)
            | _ -> this.SetValue v

        member this.Double(v: float) =
            match this.ExpectedType.TypeInfo with
            | TypeInfo.Float -> this.SetValue(v)
            | TypeInfo.Float32 -> this.SetValue(float32 v)
            | TypeInfo.Decimal -> this.SetValue(decimal v)
            | _ -> this.SetValue v

        member this.Decimal(v: decimal, _:DecimalSchema) =
            this.SetValue v

        member this.Float(v: float32) =
            match this.ExpectedType.TypeInfo with
            | TypeInfo.Float -> this.SetValue(float v)
            | TypeInfo.Float32 -> this.SetValue(v)
            | TypeInfo.Decimal -> this.SetValue(decimal v)
            | _ -> this.SetValue v

        member this.Bytes(v: byte array) =
            match this.ExpectedType.TypeInfo with
            | TypeInfo.BigInt -> Numerics.BigInteger(v) |> this.SetValue
            | _ -> this.SetValue(v)

        member this.Enum(_:int, symbol: string) =
            factory.EnumConstructor(this.ExpectedType.TargetType).Construct symbol |> this.SetValue

        member this.StartArray() =
            factory.Constructor this.ExpectedType.TargetType |> stack.Push
        member _.StartArrayBlock(size: int64) = ()
        member this.EndArray() = this.ConstructValue()

        member this.StartMap() = factory.Constructor this.ExpectedType.TargetType |> stack.Push
        member _.StartMapBlock(size:int64) = ()
        member this.Key(key: string) =  this.SetKey key |> ignore
        member this.EndMap() = this.ConstructValue()

        member this.StartRecord() = factory.Constructor this.ExpectedType.TargetType |> stack.Push
        member this.Field(fieldName: string) = this.SetKey fieldName
        member this.EndRecord() = this.ConstructValue()

        member this.StartUnionCase (idx:int,name:string) =
            if factory.IsKnownUnionCase(this.ExpectedType.TargetType, name) then
                factory.UnionConstructor(this.ExpectedType.TargetType, name) |> stack.Push
                true
            else
                if options.EvolutionTolerantMode then   // create stub case
                    factory.UnionStub(options.CustomRules, this.ExpectedType, this.ExpectedSchema) |> this.SetValue
                false
        member this.EndUnionCase() = this.ConstructValue()

        member this.NoneCase() = this.SetValue null
        member this.StartSomeCase(schema:Schema) = factory.OptionConstructor this.ExpectedType.TargetType |> stack.Push
        member this.EndSomeCase() = this.ConstructValue()

        member this.Fixed(v: byte array) =
            match this.ExpectedType.TypeInfo with
            | TypeInfo.Guid -> Guid(v) |> this.SetValue
            | _ -> this.SetValue(v)

        member _.End() = instance <- stack.Pop().Construct()

    interface IAvroBuilderWithSchema with
        member this.ExpectedValueSchema = this.ExpectedSchema

    member private _.ExpectedSchema = stack.Peek().ExpectedValueSchema
    member private _.ExpectedType = stack.Peek().ExpectedValueType
    member private _.SetKey (key:string) = stack.Peek().SetKey(key)
    member private this.SetValue (v:'TValue) =
        match this.ExpectedType.TypeInfo with
        | TypeInfo.Any type' ->
            match rulesDict.TryGetValue type' with
            | true, rule -> rule.Cast (v :> obj) |> stack.Peek().SetValue
            | _ -> v |> stack.Peek().SetValue
        | _ ->
            stack.Peek().SetValue v

    member private this.ConstructValue () = stack.Pop().Construct() |> this.SetValue