namespace Avro.FSharp

open System

type IAvroBuilder =
    abstract Start: unit -> unit

    abstract Null: unit -> unit
    abstract Boolean: v:bool -> unit 
    abstract Int: v:int -> unit 
    abstract Long: v:int64 -> unit 
    abstract Float: v:float32 -> unit 
    abstract Double: v:float -> unit 
    abstract String: v:string -> unit 
    abstract Bytes: v:byte array -> unit 
    abstract Decimal: v:decimal*schema:DecimalSchema -> unit

    abstract StartRecord: unit -> unit
    abstract Field: name:string -> bool
    abstract EndRecord: unit -> unit

    abstract StartArray: size:int -> unit
    abstract EndArray: unit -> unit

    abstract StartMap: size:int -> unit
    abstract Key: key:string -> unit
    abstract EndMap: unit -> unit

    abstract Enum: symbol:string -> unit

    abstract StartUnionCase: caseIdx:int*caseName:string -> unit
    abstract EndUnionCase: unit -> unit

    abstract NoneCase: unit -> unit
    abstract StartSomeCase: Schema -> unit
    abstract EndSomeCase: unit -> unit

    abstract End: unit -> unit

type IAvroBuilderWithSchema =
    inherit IAvroBuilder
    
    abstract ExpectedValueSchema : Schema

type IInstanceConstructor =
    abstract SetKey: key:string -> bool
    abstract ExpectedValueSchema: Schema
    abstract ExpectedValueType: System.Type
    abstract SetValue: v:'TValue -> unit
    abstract Construct: unit -> obj

type IEnumConstructor =
    abstract Construct: string -> obj

type IInstanceFactory =
    abstract TargetSchema: Schema
    abstract TargetType: Type
    abstract CreateConstructor: Type -> IInstanceConstructor
    abstract CreateNullableConstructor: Type -> IInstanceConstructor
    abstract CreateValueConstructor: Type*Schema -> IInstanceConstructor
    abstract CreateUnionConstructor: Type*string -> IInstanceConstructor    
    abstract CreateEnumConstructor: Type*EnumSchema -> IEnumConstructor