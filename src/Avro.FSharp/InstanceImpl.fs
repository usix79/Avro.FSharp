namespace Avro.FSharp

open System.Collections.Generic

type InstanceDirector(factory: IInstanceFactory) =

    member _.Construct(instance:obj, builder:IAvroBuilder) = 
        
        let rec write (schema:Schema) (obj:obj) =
            let obj = factory.SerializationCast obj obj
            match schema with
            | Null -> builder.Null()
            | Boolean -> builder.Boolean (unbox obj)
            | Int -> builder.Int (unbox obj)
            | Long -> builder.Long (unbox obj)
            | Float -> builder.Float (unbox obj)
            | Double -> builder.Double (unbox obj)
            | String -> builder.String (unbox obj)
            | Bytes -> builder.Bytes (unbox obj)
            | Enum _ -> 
                let dctr = factory.EnumDeconstructor obj
                builder.Enum(dctr.Idx(obj), obj.ToString())
            | Decimal schema -> builder.Decimal(unbox obj, schema)
            | Array schema ->
                let dctr = factory.ArrayDeconstructor obj
                builder.StartArray()
                builder.StartArrayBlock(int64(dctr.Size obj))
                for obj in dctr.Items obj do write schema.Items obj
                builder.EndArray()
            | Map schema ->
                let dctr = factory.MapDeconstructor obj
                builder.StartMap()
                builder.StartMapBlock(int64(dctr.Size obj))
                for pair in dctr.Items obj do
                    builder.Key(pair.Key.ToString())
                    write schema.Values pair.Value
                builder.EndMap()
            | Record schema ->
                let dctr = factory.RecordDeconstructor obj
                builder.StartRecord()
                dctr.Fields obj
                |> Array.iteri(fun idx obj -> 
                    let field = schema.Fields.[idx]
                    if builder.Field field.Name then
                        write field.Type obj)
                builder.EndRecord()
            | Union schemas ->
                match schemas with
                | [|Null;someSchema|] -> 
                    match obj with
                    | null -> builder.NoneCase()
                    | _ -> 
                        builder.StartSomeCase someSchema
                        factory.SerializationCast obj obj
                        |> write someSchema 
                        builder.EndSomeCase()
                | _ -> 
                    let schemaName =
                        match schemas.[0] with
                        | Record schema -> schema.Name
                        | wrongSchema -> failwithf "Union should consist of the record's schemas, but wrong schema found: %A" wrongSchema

                    let dctr = factory.UnionDeconstructor schemaName
                    let idx = dctr.CaseIdx obj                    
                    match schemas.[idx] with
                    | Record recordSchema -> 
                        builder.StartUnionCase(idx,recordSchema.Name)
                        builder.StartRecord()
                        dctr.CaseFields idx obj
                        |> Array.iteri(fun idx obj -> 
                            let field = recordSchema.Fields.[idx]
                            if builder.Field field.Name then
                                write field.Type obj)
                        builder.EndRecord()
                        builder.EndUnionCase()
                    | wrongSchema -> failwithf "Union should consist of the record's schemas, but wrong schema found: %A" wrongSchema
            | Fixed schema -> builder.Fixed (unbox obj)

        builder.Start()
        let o = factory.SerializationCast instance instance
        write factory.TargetSchema (o)
        builder.End()

type InstanceBuilder(factory: IInstanceFactory) =

    let stack = Stack<IInstanceConstructor>()
    let mutable instance = Unchecked.defaultof<obj>

    member _.Instance = instance

    interface IAvroBuilder with

        member _.Start() = 
            stack.Clear()
            stack.Push(factory.ValueConstructor(factory.TargetType, factory.TargetSchema))

        member this.Null() = this.SetValue(null) 
        member this.Boolean(v: bool) = this.SetValue(v)
        member this.Int(v: int) = this.SetValue(v)
        member this.Long(v: int64) = this.SetValue(v)
        member this.Double(v: float) = this.SetValue(v)
        member this.Float(v: float32) = this.SetValue(v)
        member this.String(v: string) = this.SetValue(v)
        member this.Bytes(v: byte array) = this.SetValue(v)
        member this.Decimal(v: decimal, _: DecimalSchema) = this.SetValue(v) 

        member this.Enum(idx:int, symbol: string) = 
            match this.ExpectedSchema with
            | Enum schema -> 
                let ctr = factory.EnumConstructor(this.ExpectedType, schema)
                ctr.Construct symbol |> this.SetValue
            | schema -> failwithf "Expected schema should be Enum Schema, but it is %A" schema

        member this.StartRecord() = factory.Constructor this.ExpectedType |> stack.Push
        member this.Field(fieldName: string) = this.SetKey fieldName            
        member this.EndRecord() = this.ConstructValue()

        member this.StartArray() = factory.Constructor this.ExpectedType |> stack.Push
        member this.StartArrayBlock(size:int64) = ()
        member this.EndArray() = this.ConstructValue()

        member this.StartMap() = factory.Constructor this.ExpectedType |> stack.Push
        member this.StartMapBlock(size:int64) = ()
        member this.Key(key: string) =  this.SetKey key |> ignore
        member this.EndMap() = this.ConstructValue()

        member this.StartUnionCase (idx:int,name:string) = factory.UnionConstructor(this.ExpectedType, name) |> stack.Push
        member this.EndUnionCase() = this.ConstructValue()

        member this.NoneCase() = this.SetValue null
        member this.StartSomeCase(schema:Schema) = factory.NullableConstructor this.ExpectedType |> stack.Push
        member this.EndSomeCase() = this.ConstructValue()

        member this.Fixed(v: byte array) = this.SetValue(v)

        member _.End() = instance <- stack.Pop().Construct()

    interface IAvroBuilderWithSchema with
        member this.ExpectedValueSchema = this.ExpectedSchema

    member private _.ExpectedSchema = stack.Peek().ExpectedValueSchema
    member private _.ExpectedType = stack.Peek().ExpectedValueType
    member private _.SetKey (key:string) = stack.Peek().SetKey(key)
    member private _.SetValue (v:'TValue) = stack.Peek().SetValue(v)
    member private this.ConstructValue () = stack.Pop().Construct() |> this.SetValue