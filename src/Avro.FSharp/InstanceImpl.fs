namespace Avro.FSharp

open System.Collections.Generic

type InstanceDirector<'T>() =
    let rootSchema, reflector = 
        match Schema.generateWithReflector [] typeof<'T> with
        | Ok (schema, reflector) -> schema,reflector
        | Error err -> failwithf "Schema error %A" err

    member _.Schema = rootSchema

    member _.Construct(instance:'T, builder:IAvroBuilder) = 
        
        let rec write (schema:Schema) (obj:obj) =
            let obj = reflector.WriteCast obj
            match schema with
            | Null -> builder.Null()
            | Boolean -> builder.Boolean (unbox obj)
            | Int -> builder.Int (unbox obj)
            | Long -> builder.Long (unbox obj)
            | Float -> builder.Float (unbox obj)
            | Double -> builder.Double (unbox obj)
            | String -> builder.String (unbox obj)
            | Bytes -> builder.Bytes (unbox obj)
            | Enum _ -> builder.String(obj.ToString())
            | Decimal schema -> builder.Decimal(unbox obj, schema)
            | Array schema ->
                builder.StartArray(reflector.GetArraySize obj)
                for obj in reflector.GetArrayItems obj do write schema.Items obj
                builder.EndArray()
            | Map schema ->
                builder.StartMap(reflector.GetMapSize obj)
                for pair in reflector.GetMapItems obj do
                    builder.Key(pair.Key.ToString())
                    write schema.Values pair.Value
                builder.EndMap()
            | Record schema ->
                builder.StartRecord()
                reflector.GetFieldValues schema.Name obj
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
                        reflector.WriteCast obj
                        |> write someSchema 
                        builder.EndSomeCase()
                | _ -> 
                    let idx = 
                        match schemas.[0] with
                        | (Record rs) -> reflector.GetCaseIdx rs.Name obj
                        | wrongSchema -> failwithf "Union should consist of the record's schemas, but wrong schema found: %A" wrongSchema
                    
                    match schemas.[idx] with
                    | Record recordSchema -> 
                        builder.StartUnionCase(idx,recordSchema.Name)
                        write (Record recordSchema) obj
                        builder.EndUnionCase()
                    | wrongSchema -> failwithf "Union should consist of the record's schemas, but wrong schema found: %A" wrongSchema
            | Fixed schema -> builder.Bytes (unbox obj)

        builder.Start()
        let o = reflector.WriteCast instance
        write rootSchema (o)
        builder.End()

type InstanceBuilder(factory: IInstanceFactory) =

    let stack = Stack<IInstanceConstructor>()
    let mutable instance = Unchecked.defaultof<obj>

    member _.Instance = instance

    interface IAvroBuilder with

        member _.Start() = 
            stack.Clear()
            stack.Push(factory.CreateValueConstructor(factory.TargetType, factory.TargetSchema))

        member this.Null() = this.SetValue(null) 
        member this.Boolean(v: bool) = this.SetValue(v)
        member this.Int(v: int) = this.SetValue(v)
        member this.Long(v: int64) = this.SetValue(v)
        member this.Double(v: float) = this.SetValue(v)
        member this.Float(v: float32) = this.SetValue(v)
        member this.String(v: string) = this.SetValue(v)
        member this.Bytes(v: byte array) = this.SetValue(v)
        member this.Decimal(v: decimal, _: DecimalSchema) = this.SetValue(v) 

        member this.Enum(symbol: string) = 
            match this.ExpectedSchema with
            | Enum schema -> 
                let ctr = factory.CreateEnumConstructor(this.ExpectedType, schema)
                ctr.Construct symbol |> this.SetValue
            | schema -> failwithf "Expected schema should be Enum Schema, but it is %A" schema

        member this.StartRecord() = factory.CreateConstructor this.ExpectedType |> stack.Push
        member this.Field(fieldName: string) = this.SetKey fieldName            
        member this.EndRecord() = stack.Pop().Construct() |> this.SetValue

        member this.StartArray(size:int) = factory.CreateConstructor this.ExpectedType |> stack.Push

        member this.EndArray() = stack.Pop().Construct() |> this.SetValue

        member this.StartMap(size:int) = factory.CreateConstructor this.ExpectedType |> stack.Push
        member this.Key(key: string) =  this.SetKey key |> ignore
        member this.EndMap() = stack.Pop().Construct() |> this.SetValue

        member this.StartUnionCase (idx:int,name:string) = factory.CreateUnionConstructor(this.ExpectedType, name) |> stack.Push
        member this.EndUnionCase() = stack.Pop().Construct() |> this.SetValue

        member this.NoneCase() = this.SetValue null
        member this.StartSomeCase(schema:Schema) = factory.CreateNullableConstructor this.ExpectedType |> stack.Push
        member this.EndSomeCase() = stack.Pop().Construct() |> this.SetValue

        member _.End() = instance <- stack.Pop().Construct()

    interface IAvroBuilderWithSchema with
        member _.ExpectedValueSchema =  stack.Peek().ExpectedValueSchema

    member private _.ExpectedSchema = stack.Peek().ExpectedValueSchema
    member private _.ExpectedType = stack.Peek().ExpectedValueType
    member private _.SetKey (key:string) = stack.Peek().SetKey(key)
    member private _.SetValue (v:'TValue) = stack.Peek().SetValue(v)