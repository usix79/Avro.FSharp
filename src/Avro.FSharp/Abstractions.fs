namespace Avro.FSharp

open System
open System.Collections

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

    abstract StartArray: unit -> unit
    abstract StartArrayBlock: size:int64 -> unit
    abstract EndArray: unit -> unit

    abstract StartMap: unit -> unit
    abstract StartMapBlock: size:int64 -> unit
    abstract Key: key:string -> unit
    abstract EndMap: unit -> unit

    abstract Enum: idx:int*symbol:string -> unit

    abstract StartUnionCase: caseIdx:int*caseName:string -> unit
    abstract EndUnionCase: unit -> unit

    abstract NoneCase: unit -> unit
    abstract StartSomeCase: Schema -> unit
    abstract EndSomeCase: unit -> unit

    abstract Fixed: v:byte array -> unit 

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

type IEnumDeconstructor =
    abstract Idx: obj -> int

type IArrayDeconstructor =
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

type IInstanceFactory =
    abstract TargetSchema: Schema
    abstract TargetType: Type
    
    abstract Constructor: Type -> IInstanceConstructor
    abstract NullableConstructor: Type -> IInstanceConstructor
    abstract ValueConstructor: Type*Schema -> IInstanceConstructor
    abstract UnionConstructor: Type*string -> IInstanceConstructor    
    abstract EnumConstructor: Type*EnumSchema -> IEnumConstructor
    
    abstract SerializationCast: obj -> (obj -> obj)
    abstract EnumDeconstructor: obj -> IEnumDeconstructor
    abstract ArrayDeconstructor: obj -> IArrayDeconstructor
    abstract MapDeconstructor: obj -> IMapDeconstructor
    abstract RecordDeconstructor: obj -> IRecordDeconstructor
    abstract UnionDeconstructor: recordName:string -> IUnionDeconstructor        