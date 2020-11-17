namespace Avro.FSharp

open System
open System.IO
open System.Numerics

type BinaryBuilder(writer:BinaryWriter) =

    interface IAvroBuilder with

        member _.Start() = ()
        member _.Null() = ()
        member _.Boolean(v: bool) = writer.Write(if v then 1uy else 0uy)
        member _.Int(v: int) = writer.Write v
        member _.Long(v: int64) = writer.Write v
        member _.Float(v: float32) = writer.Write v
        member _.Double(v: float) = writer.Write v
        member this.String(v: string) = (this :> IAvroBuilder).Bytes(System.Text.Encoding.UTF8.GetBytes(v))

        member _.Bytes(v: byte array) =
            writer.Write v.LongLength
            writer.Write v

        member this.Decimal(v: decimal, schema: DecimalSchema) =
            let intValue =  (v * decimal (Math.Pow(10., float schema.Scale))) |> Decimal.Truncate |> BigInteger
            let byteValue = intValue.ToByteArray()
            Array.Reverse(byteValue)
            (this :> IAvroBuilder).Bytes(byteValue)

        member _.Enum(idx:int, symbol: string) =  writer.Write idx
        member _.StartArray() =  ()
        member _.StartArrayBlock(size: int64) =  writer.Write(size)
        member _.EndArray() = writer.Write(0L)
        member _.StartMap() = ()
        member _.StartMapBlock(size: int64) = writer.Write(size)
        member this.Key(key: string) = (this :> IAvroBuilder).String(key)
        member _.EndMap() = writer.Write(0L)
        member _.StartRecord() = ()
        member _.Field(name: string) = true
        member _.EndRecord() = ()
        member _.StartUnionCase(caseIdx: int, caseName: string) =
            writer.Write(caseIdx)
            true
        member _.EndUnionCase() = ()
        member _.NoneCase() = writer.Write(0)
        member _.StartSomeCase(schema: Schema) = writer.Write(1)
        member _.EndSomeCase() = ()

        member _.Fixed(v: byte array) = writer.Write v

        member _.End() = writer.Flush()

module BinaryDirector =

    let dummyBuilder =
        {new IAvroBuilder with
            member _.Boolean(v: bool): unit = ()
            member _.Bytes(v: byte array): unit = ()
            member _.Double(v: float): unit = ()
            member _.Decimal(v: decimal, schema: DecimalSchema): unit = ()
            member _.End(): unit = ()
            member _.EndArray(): unit = ()
            member _.EndMap(): unit = ()
            member _.EndRecord(): unit = ()
            member _.EndSomeCase(): unit = ()
            member _.EndUnionCase(): unit = ()
            member _.Enum(idx: int, symbol: string): unit = ()
            member _.Field(name: string): bool = false
            member _.Fixed(v: byte array): unit = ()
            member _.Float(v: float32): unit = ()
            member _.Int(v: int): unit = ()
            member _.Key(key: string): unit = ()
            member _.Long(v: int64): unit = ()
            member _.NoneCase(): unit = ()
            member _.Null(): unit = ()
            member _.Start(): unit = ()
            member _.StartArray(): unit = ()
            member _.StartArrayBlock(size: int64): unit = ()
            member _.StartMap(): unit = ()
            member _.StartMapBlock(size: int64): unit = ()
            member _.StartRecord(): unit = ()
            member _.StartSomeCase(arg1: Schema): unit = ()
            member _.StartUnionCase(caseIdx: int, caseName: string): bool = false
            member _.String(v: string): unit = ()}

    let create () =
        fun (reader:BinaryReader) (writerSchema: Schema) (builder:IAvroBuilder) ->
            let rec write (builder:IAvroBuilder) = function
                | Null -> builder.Null()
                | Boolean -> (reader.ReadByte() <> 0uy) |> builder.Boolean
                | Int -> reader.ReadInt32() |> builder.Int
                | Long -> reader.ReadInt64() |> builder.Long
                | Float -> reader.ReadSingle() |> builder.Float
                | Double -> reader.ReadDouble() |> builder.Double
                | Bytes ->
                    let size = reader.ReadInt64()
                    reader.ReadBytes(int size) |> builder.Bytes
                | String ->
                    let size = reader.ReadInt64()
                    let bytes = reader.ReadBytes(int size)
                    builder.String(System.Text.Encoding.UTF8.GetString(bytes))
                | Decimal schema ->
                    let size = reader.ReadInt64()
                    let bytesValue = reader.ReadBytes(int size)
                    Array.Reverse(bytesValue)
                    let intValue = BigInteger(bytesValue)
                    let quotient, remainder = BigInteger.DivRem (intValue, BigInteger.Pow(10|>BigInteger, schema.Scale))
                    let decimalValue = (decimal quotient) + ((decimal remainder) / (decimal (Math.Pow(10., float schema.Scale))))
                    builder.Decimal(decimalValue, schema)
                | Fixed schema -> reader.ReadBytes(schema.Size) |> builder.Fixed
                | Enum schema ->
                    let idx = reader.ReadInt32()
                    builder.Enum(idx, schema.Symbols.[idx])
                | Array schema ->
                    let rec arrayFun = function
                        | 0L -> ()
                        | size ->
                            builder.StartArrayBlock size
                            for _ in 1 .. int size do
                                write builder schema.Items
                            reader.ReadInt64() |> arrayFun

                    builder.StartArray()
                    reader.ReadInt64() |> arrayFun
                    builder.EndArray()
                | Map schema ->
                    let rec mapFun = function
                        | 0L -> ()
                        | size ->
                            builder.StartMapBlock size
                            for _ in 1 .. int size do
                                let size = reader.ReadInt64()
                                let bytes = reader.ReadBytes(int size)
                                builder.Key(System.Text.Encoding.UTF8.GetString(bytes))
                                write builder schema.Values
                            reader.ReadInt64() |> mapFun

                    builder.StartMap()
                    reader.ReadInt64() |> mapFun
                    builder.EndMap()
                | Record schema ->
                    builder.StartRecord()
                    for field in schema.Fields do
                        write
                            (if builder.Field field.Name then builder else dummyBuilder)
                            field.Type
                    builder.EndRecord()
                | Union [|Null;someSchema|] ->
                    match reader.ReadInt32() with
                    | 0 -> builder.NoneCase()
                    | 1 ->
                        builder.StartSomeCase(someSchema)
                        write builder someSchema
                        builder.EndSomeCase()
                    | x -> failwithf "Not possible case index: %d for nullable schema: %A" x someSchema
                | Union schemas ->
                    let idx = reader.ReadInt32()
                    match schemas.[idx] with
                    | Record schema ->
                        let builder = if builder.StartUnionCase(idx, schema.Name) then builder else dummyBuilder
                        for field in schema.Fields do
                            write
                                (if builder.Field field.Name then builder else dummyBuilder)
                                field.Type
                        builder.EndUnionCase()
                    | wrongSchema -> failwithf "An union should consist of records, but not: %A" wrongSchema

            builder.Start()
            write builder writerSchema
            builder.End()