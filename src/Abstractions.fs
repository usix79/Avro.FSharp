namespace Avro.FSharp

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

    abstract StartUnionCase: caseIdx:int*caseName:string -> bool
    abstract EndUnionCase: unit -> unit

    abstract NoneCase: unit -> unit
    abstract StartSomeCase: Schema -> unit
    abstract EndSomeCase: unit -> unit

    abstract Fixed: v:byte array -> unit

    abstract End: unit -> unit

type IAvroBuilderWithSchema =
    inherit IAvroBuilder
    abstract ExpectedValueSchema : Schema