# Avro.FSharp

FSharp implementation of Apache Avro

## Overview

The library generates an Avro schema by an F# type as well as serialize and deserialise an F# data in binary and json encodings.
It works seamlessly with [Fable.Avro](https://github.com/usix79/Fable.Avro).

## Schema

To generate a schema, use Schema.generate:

```fsharp
record SchemaOptions
  val Annotations: String           // information about aliases and default values in json string
  val CustomRules: list<CustomRule> // custom rules for non supported types
  val StubDefaultValues: bool       // add stub default values if true
  val TreatDecimalAsDouble: bool    // generate double schema for decimals (otherwise bytes schema is generated)
  val TreatBigIntAsString: bool     // generate string schema for bigints (otherwise bytes schema is generated)
  val TreatGuidAsString: bool       // generate string schema for guids (otherwise fixed schema is generated)

val generate:
   options: SchemaOptions ->
   type'      : Type                    // type to build schema of
             -> Result<Schema,SchemaError>
```

To read a schema from json string, use Schema.ofString:
```fsharp
val ofString:
   jsonString: string
            -> Schema
```
To write schema, use Schema.toString:
```fsharp
val toString:
   schema: Schema
        -> string
```

### Primitive types

| F# type | Avro type |
|---------|-----------|
| `string` | `string` |
| `bool` | `boolean` |
| `byte` | `int` |
| `short` | `int` |
| `int` | `int` |
| `uint` | `int` |
| `int16` | `int` |
| `int64` | `long` |
| `uint64` | `long` |
| `float32` | `float` |
| `float` | `double` |
| `byte array` | `bytes` |

Examples:
* `generate Schema.defaultOptions typeof<string>` generates: `{"type": "string"}`
* `generate Schema.defaultOptions typeof<bool>` generates: `{"type": "boolean"}`
* `generate Schema.defaultOptions typeof<int>` generates: `{"type": "int"}`
* `generate Schema.defaultOptions typeof<int64>` generates: `{"type": "long"}`
* `generate Schema.defaultOptions typeof<float32>` generates: `{"type": "float"}`
* `generate Schema.defaultOptions typeof<float>` generates: `{"type": "double"}`
* `generate Schema.defaultOptions typeof<byte array>` generates: `{"type": "bytes"}`

### Arrays

Following F# types are mapped to avro's array:
* `'T list`
* `'T array` (if `'T` is not `byte`)
* `ResizeArray<'T>` (`System.Collection.Generic.List<'T>`)
* `Set<'T>`
* `HashSet<'T>`
* `'T Seq` (`IEnumerable<'T>`)

Examples:
* `generate Schema.defaultOptions typeof<string list>` generates: `{"type": "array", "values": "string"}`
* `generate Schema.defaultOptions typeof<int array>` generates: `{"type": "array", "values": "int"}`
* `generate Schema.defaultOptions typeof<List<bool>>` generates: `{"type": "array", "values": "boolean"}`

### Maps

Following F# types are mapped to avro's map:
* `Map<string,'TValue>`
* `Dictionary<string,'TValue>`

Examples:
* `generate Schema.defaultOptions typeof<Map<string,string>>` generates: `{"type": "map", "values": "string"}`
* `generate Schema.defaultOptions typeof<Dictionary<string, int>>` generates: `{"type": "map", "values": "int"}`


### Enums

F# Enum is mapped to Avro's `enum`

Example:

```fsharp
type TestState =
    | Red = 3
    | Yellow = 2
    | Green = 1

generate Schema.defaultOptions typeof<TestState>
```
generated schema:
```json
{"type": "enum", "name": "TestState", "symbols": ["Green", "Yellow", "Red"]}
```

Symbols are ordered by its values.

### Records

F# records and tuples are mapped to Avro's `record`.

Example:

```fsharp
type SimpleRecord = {
    Id : int
    Name : string
    Version : int64}

generate Schema.defaultOptions typeof<SimpleRecord>
```
generated schema:
```json
{
    "type": "record",
    "name": "SimpleRecord",
    "fields" : [
        {"name": "Id", "type": "int"},
        {"name": "Name", "type": "string"},
        {"name": "Version", "type": "long"}
    ]
}
```

A tuple is mapped to Avro's `record` with fields `Item1`, `Item2` and so on.

Example:

`generate Schema.defaultOptions typeof<int*string>>` generates
```json
{
    "type": "record",
    "name": "Tuple_Of_Int32_And_String",
    "fields" : [
        {"name": "Item1", "type": "int"},
        {"name": "Item2", "type": "string"},
    ]
}
```

Generic records are also allowed:
```fsharp
type GenericRecord<'T> = {
    Value : 'T
}

generate Schema.defaultOptions typeof<GenericRecord<string>>
```
generated schema:
```json
{
    "type":"record",
    "name":"GenericRecord_Of_String",
    "fields":[{"name":"Value","type":"string"}]
}
```
### Unions

F# Discriminated Union is mapped to Avro's `union` of records, generated from the union's cases

Example:

```fsharp
type BinaryTree =
    | Leaf of value:string
    | Node of left: BinaryTree * right: BinaryTree

generate Schema.defaultOptions typeof<BinaryTree>
```
generated schema:
```json
{
    "type":[
        {
            "type":"record",
            "name":"Leaf",
            "fields":[
                {"name":"value","type":"string"}
            ]
        },
        {
            "type":"record",
            "name":"Node",
            "fields":[
                {"name":"left","type":["Leaf","Node"]},
                {"name":"right","type":["Leaf","Node"]}
            ]
        }
    ]
}
```

Option is mapped as `union` of `null` and the option's generic argument's type

Example:

`generate Schema.defaultOptions typeof<Option<float>>` generates `["null","double"]`

### Known types

Following types are handled in special way

| F# type | Avro type | Description |
|---------|-----------|-------------|
| `Guid` |  `string` of `fixed` | |
| `Decimal` | `double` or `bytes` |  |
| `BigInt` | `string` of `bytes` | |
| `DateTime` | `string` | ISO 8601 |
| `DateTimeOffset` | `string` | ISO 8601 |
| `TimeSpan` | `int` | milliseconds |
| `Uri` | `string` | |

### Logical types

NOT YET SUPPORTED

## Annotations
Some schema's attributes can not be evolved from F# type (default values and aliases).
Additional annotation is used for the purpose. Here is example of the annotation json.

```json
{
    "records": [{
        "name": "Foo.Bar.NewRecord",        // Full name of the record
        "aliases": ["Foo.Bar.OldRecord"],   // Aliases attributes in the record's schema
        "fields": [{
                "name": "Caption",          // Name of the record's field
                "aliases": ["Title", "Cap"] // Aliases atttibute in the field's schema
            },
            {
                "name": "Description",
                "aliases": [],
                "default": "Not Yet Described"  // Default value in the fied's schema
            }
        ]
    }],
    "enums": [{
        "name": "Foo.Bar.NewTestState",     // Full name of the enum
        "aliases": ["Foo.Bar.TestState"],   // Aliases attribute is the enum's schema
        "default": "Blue"                   // Default value in the enum's schema
    }],
    "decimals": [{
        "record": "Foo.Bar.NewTestState", // Full name of the record
        "field": "Price",                 // Name of a decimal field
        "scale": 3                        // Scale for decimal field
    }]
}
```

You don't need to annotate all enums, records and fields. Annotate only those schemas which should be enriched with additional attributes. Since tuples and DU cases are mapped to a record, you may define attibutes for them as well. Remember, that tupel's field name is like `Item1, Item2, Item3 ...`. DU case name is composed from name of the DU and name of the case.

## Names

According to avro specification, only `[A-Za-z0-9_]` symbols are allowed in the name attributes.
Name of an enum is constructed from namespace and type's name.
Name of a record also contains description of the generic type arguments.

Rule of the generation of the records name is describer is the following table:

| Precicate | Rule |
|------|---------|
| Is kind of array | `Array_Of_{ItemTypeName}` |
| Is kind of map | `Map_Of_{ValueTypeName}` |
| IsGenericType | `{TypeName}_Of_{GenericType1Name}_And_{GenericType2Name}_...` |
| Is Result<OkType,ErrType> | `Result_Of_{OkTypeName}_{ErrTypeName}` |
| Is Option<TSome>| `Nullable_{TSomeName}` |
| Is Tuple| `Tuple_Of_{Item1TypeName}_{Item2TypeName}_...` |
| Is DU case | `{DU Type Name}.{CaseName}`
| `System.RestName` | `RestName` |

Examples of record names:
* `Result_Of_Int64_And_String.Ok`
* `Tuple_Of_Int32_And_String`
* `Foo.Bar.GenericRecord_Of_Nullable_String`

# Serde

To create serializer use:
```fsharp
record SerializationOptions
  val CustomRules: list<CustomRule>

val Serde.binarySerializer:
   options: SerializationOptions ->
   type'  : Type                 ->
   schema : Schema
         -> obj -> Stream -> unit

val Serde.jsonSerializer:
   options: SerializationOptions ->
   type'  : Type                 ->
   schema : Schema
         -> obj -> Utf8JsonWriter -> unit```

To create deserialized use:
```fsharp

record DeserializationOptions
  val CustomRules: list<CustomRule>
  val EvolutionTolerantMode: bool

val Serde.binaryDeserializer:
   options     : DeserializationOptions ->
   type'       : Type   ->
   readerSchema: Schema
              -> Schema -> Stream -> obj

val Serde.jsonDeserializer:
   options: DeserializationOptions ->
   type'  : Type   ->
   schema : Schema
         -> Stream -> obj
```
Here is basic example:

```fsharp
    let orig:MyType = createInstance()

    let schema =
        match Schema.generate Schema.defaultOptions typeof<MyType> with
        | Ok schema -> schema
        | Error err -> failwithf "Schema error: %A" err

    // binary
    let serializer = Serde.binarySerializer Serde.defaultSerializerOptions (case.InstanceType) schema
    let deserializer = Serde.binaryDeserializer Serde.defaultDeserializerOptions (case.InstanceType) schema

    use stream = new MemoryStream()
    serializer case.Instance stream

    let data = stream.ToArray()

    use stream = new MemoryStream(data)
    let copy = deserializer schema stream
    Expect.equal "copy shoud be equal to original" orig copy

    // json
    let serializer = Serde.jsonSerializer Serde.defaultSerializerOptions typeof<MyType> schema
    let deserializer = Serde.jsonDeserializer Serde.defaultDeserializerOptions typeof<MyType> schema

    use stream = new MemoryStream()
    use writer = new Utf8JsonWriter(stream)
    serializer case.Instance writer

    let data = stream.ToArray()

    use stream = new MemoryStream(data)
    let copy = deserializer stream
    Expect.equal "copy shoud be equal to original" orig copy
```


# Evolution issues

It is very important in microservices architecture, that changes in the schema do not break work of a service. The library aimed to make schemas evolution compatibility as simple as possible.

## Stub default values

Setting option `SchemaOptions.StubDefaultValues` enable adding default value to each field's schema and enum's schema.

Following rules are used:

| F# Type | Default Value |
| ---------- | ----- |
| `string` | `""` |
| `bool` | `false` |
| `byte`, `short`, `uint16`, `uint32`, `uint64`, `int`,  `long`, `float32`, `float` | `0` |
| `byte array` | `""` |
| `array`, `list`, `set`, `seq`, `ResizeArray`, `HashSet` | `[]` |
| `Map`, `Dictionary<_,_>` | `{}` |
| `Enum` | `"{NameOfFirsSymbol}"` Symbols are ordered by values |
| `Record` | `{"{Field1Name}": {DefaultValueForField1}, ...}` |
| `Tuple` | `{"{Item1}": {DefaultValueForItem1}, ...}` |
| `Option` | `null` |
| `DU` | `{"Case1Field1Name": {DefaultValueForCase1Field1}, ...} stub for first case in DU` |
| `decimal` | `0` |
| `BigInt` | `"0"` |
| `Guid` | `"00000000-0000-0000-0000-000000000000"` |
| `DateTime` | `"1970-01-01T00:00:00.000Z"` |
| `DateTimeOffset` | `"1970-01-01T00:00:00.000+00:00"` |
| `TimeSpan` | 0 |

If deserializer can not find field's value it looks default value in the annotations. If annotations do not have defalut value for the field, stub value is created. Set `DeserializationOptions.EvolutionTolerantMode=false` if you don't want the behaviour.

Deserializing of the Enums is performed by the same algorithm.

## Forward compatibility for DU

According to Avro standard, adding a new case at a union is a non forward compatible change ([see](https://avro.apache.org/docs/current/spec.html#Schema+Resolution)).

Let's pretend that first version of our domain looks like:
```fsharp
type DomainUnion =
    | Case1
    | Case2
```

Eventually, version 2 is evolved:
```fsharp
type DomainUnion =
    | Case1
    | Case2
    | Case3
```

According to Avro standard, microservices that use old schema (version 1) should get an error trying deserialize `Case3`. This is big obstacle for evolution of the algebraic types. Therefore the library substitutes unknown case with default value of DU (stub for the first case) if `DeserializationOptions.EvolutionTolerantMode=true`.

For example if deserializer's domain:

```fsharp
type DomainUnion =
    | UnknownCase
    | Case1
```

And serializer's domain:
```fsharp
type DomainUnion =
    | UnknownCase
    | Case1
    | Case2
    | Case3
```

`Case3` will be deserialized to `UnknownCase` (first case of the `DomainUnion`). This is true for any occasion of DU in deserialized type (whenever it is a record's field, or item in an array, or value in a map). For example, array `[Case1, Case3, Case2, Case1]` will be deserialized to `[Case1, UnknownCase, UnknownCase, Case1]`. Set `DeserializationOptions.EvolutionTolerantMode=false` if you don't want the behaviour.

# Custom Rules

It is possible to customize processing of a particular type. In that case `CustomRule` should be created.
```fsharp
record CustomRule
  val InstanceType: Type                // particular type
  val SurrogateType: Type               // surrogate type, shoud be supported by serializer
  val CastFromSurrogate: obj -> obj
  val CastToSurrogate: obj -> obj
  val StubValue: Json                   // default value, shoud be compatible with json format of the surrogate
```

Example of the implementation of the `CustomRule`:

```fsharp
{
    InstanceType = typeof<Uri>
    SurrogateType = typeof<string>
    CastToSurrogate = fun v -> v.ToString() |> box
    CastFromSurrogate = fun v -> Uri(unbox(v)) |> box
    StubValue =  """ "" """ // JSON with empty string
}
```

List with custom rules is passed to schema generator, serializer and deserializer as part of its options

# Examples
More examples of complex types and corresponging schemas is available in the [SchemaTests.fs](https://github.com/usix79/Avro.FSharp/blob/main/test/SchemaTests.fs).

See [CustomRule.fs](https://github.com/usix79/Avro.FSharp/blob/main/src/CustomRule.fs) for details of the implementation of the custom rules.

An implementation of Kafka producer and consumer, working with SchemaRegistry could be found [here](https://github.com/usix79/Avro.FSharp/tree/main/examples/KafkaSerde). (*The example is supposed you have an account in confluent.cloud. Change configuration code if you have on-premise installation.*)