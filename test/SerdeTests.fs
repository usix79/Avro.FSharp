module Avro.FSharp.SerdeTests

open System.Linq
open System.Collections.Generic
open System.IO
open System.Text.Json
open Expecto
open Expecto.Flip
open Foo.Bar

type Comparer = string -> obj -> obj -> unit
type SimpleCase = {Name: string; Instance: obj; InstanceType: System.Type; Comparer: Comparer }
type SimpleCaseList = {Name: string; Cases: SimpleCase list}
type EvolutionCase = {Name: string; Instance: obj; InstanceType: System.Type; Annotations:string; ExpectedInstance: obj; Comparer: Comparer }

let simpleCase'<'T> comparer name (instance:'T) =
    {Name = name; Comparer = comparer; Instance = instance; InstanceType = typeof<'T>}

let simpleCase<'T when 'T:equality> name (instance:'T) =
    {Name = name; Comparer = (fun msg v1 v2 -> Expect.equal msg (v1:?>'T) (v2:?>'T)); Instance = instance; InstanceType = typeof<'T>}

let simpleCaseList name cases = {Name = name; Cases = cases}

let compareSequences<'T> msg (expected:obj) (actual:obj) =
    Expect.isTrue msg <| System.Linq.Enumerable.SequenceEqual((expected :?> IEnumerable<'T>), (actual :?> IEnumerable<'T>))

let compareDictionaries<'TKey,'TValue> msg (expected:obj) (actual:obj) =
    compareSequences<KeyValuePair<'TKey,'TValue>> msg ((expected :?> Dictionary<'TKey,'TValue>).OrderBy(fun kv -> kv.Key).ToList()) ((actual :?> Dictionary<'TKey,'TValue>).OrderBy(fun kv -> kv.Key).ToList())

let evolutionCase<'TSource, 'TDest when 'TDest:equality> name (instance:'TSource) (expectedInstance:'TDest) (annotations:string)=
    {Name = name; Comparer = (fun msg v1 v2 -> Expect.equal msg (v1:?>'TDest) (v2:?>'TDest));
        Instance = instance; InstanceType = typeof<'TSource>; ExpectedInstance = expectedInstance; Annotations = annotations}

let simpleCases = [
    simpleCaseList "Primitive" [
        simpleCase "string" "Hello World!!!"
        simpleCase "empty string" ""
        simpleCase "bool true" true
        simpleCase "bool false" false
        simpleCase "int" 125
        simpleCase "float" 543.
        simpleCase "long" 789L
        simpleCase "float32" 101.2f
        simpleCase "byte" 42uy
        simpleCase "short" 42s
        simpleCase "uint16" 42us
        simpleCase "uint32" 42u
        simpleCase "uint64" 42UL
    ]
    simpleCaseList "Enum" [
        simpleCase "SimpleEnum" TestState.Green
    ]
    simpleCaseList "Array" [
        simpleCase "bytes" [|00uy; 255uy; 34uy; 72uy; 69uy; 76uy; 76uy; 79uy; 12uy; 16uy; 00uy|]
        simpleCase "List" ["One"; "Two"; "Three"]
        simpleCase "Array" [|"One"; "Two"; "Three"|]
        simpleCase "Set" (["One"; "Two"; "Three"] |> Set.ofList)
        simpleCase' compareSequences "ResizeArray" (ResizeArray(["One"; "Two"; "Three"]))
        simpleCase' compareSequences "HashSet" (HashSet(["One"; "Two"; "Three"]))
        simpleCase' compareSequences "Seq" (seq{"One"; "Two"; "Three"})
    ]
    simpleCaseList "Map" [
        let pairs = ["One", 1; "Two", 2; "Three", 3]

        simpleCase "Map" (pairs |> Map.ofList)

        let dict = Dictionary<string,int>()
        pairs |> Seq.iter dict.Add
        simpleCase' compareDictionaries<string, int32> "Dictionary" dict

    ]
    simpleCaseList "Record" [
        simpleCase "SimpleRecord" {Id = 1; Name = "Hello World!!!"; Version = 2L}
        simpleCase "ParentRecord" {
                Chield1 =  {Id = 1; Name = "Hello World!!!"; Version = 2L}
                Chield2 =  {Id = 3; Name = "World, Hello!!!"; Version = 1L}}
        simpleCase "RecordWithArray" {Value = ["Name1"; "Name2"; "Name3"]}
        simpleCase "RecordWithMap" {Value = ["One", 1; "Two", 2; "Three", 3] |> Map.ofList}
    ]
    simpleCaseList "Tuple" [
        simpleCase "Tuple" (123, "Hello")
        simpleCase "Tuple Record" {Value = (123, "Hello")}
    ]
    simpleCaseList "Union" [
        simpleCase "Result Ok" ((Ok "Hello"):Result<string,string>)
        simpleCase "Result Error" ((Error "Hello"):Result<string,string>)
        simpleCase "BinaryTree" (Node (Leaf "XXX", Node (Leaf "YYY", Leaf "ZZZ")))
    ]
    simpleCaseList "Nullable" [
        simpleCase "Option-Some" (Some "Hello World!!!")
        simpleCase "Option-None" (None:Option<string>)
    ]
    simpleCaseList "LogicalTypes" [
        simpleCase "Decimal" 3.1415926m
        simpleCase "Scaled Decimal" {Id = 124; Caption = ""; Price = 199.99m}
        simpleCase "ItemRecord" {Id = 123; Name ="Item"; Price = Price 9.99m}
        simpleCase "Single Guid" (System.Guid.NewGuid())
        simpleCase "Guid" {Value = System.Guid.NewGuid()}
        simpleCase "DateTime" {Value = System.DateTime.UtcNow}
        simpleCase "DateTimeOffset" {Value = System.DateTimeOffset.UtcNow}
        simpleCase "TimeSpan" {Value = System.TimeSpan.FromSeconds(321.5)}
        simpleCase "BigInt" 3141592653589737I
    ]
    simpleCaseList "ComplexTests" [
        let basket:Basket = [
            SaleItem (Product("XXX-1", "Product 1", 10m<GBP/Q>), 1m<Q>)
            SaleItem (Product("XXX-2", "Product 2", 1m<GBP/Q>), 10m<Q>)
            SaleItem (Product("XXX-3", "Product 3", 3.14m<GBP/Q>), 5m<Q>)
            TenderItem (Cash, 100m<GBP>)
            TenderItem ((Card "1111-1111-1111-1111"), 15m<GBP>)
            CancelItem 2
        ]
        simpleCase "Basket" basket
    ]
    simpleCaseList "CustomTypes" [
        simpleCase "Uri" {Value = System.Uri("http://www.example.com")}
    ]
]

let evolutionCases = [
    """{
    "records": [
        {"name": "Foo.Bar.RecordWithNewField",
         "aliases": ["Foo.Bar.RecordWithId"],
         "fields": [
            {"name": "NewField", "aliases": [], "default": "Hello"}
        ]}
    ]}"""
    |> evolutionCase "Added string field" ({Id=456}:RecordWithId) ({Id=456; NewField="Hello" }:RecordWithNewField)

    """{
    "records": [
        {"name": "Foo.Bar.NewRecord",
         "aliases": ["Foo.Bar.OldRecord"],
         "fields": [
            {"name": "Caption", "aliases": ["Title", "Cap"]},
            {"name": "Description", "aliases": [], "default": "Not Yet Described"}
        ]}
    ]}"""
    |> evolutionCase "New Record" ({Id=456; Title="Hello World!!!"}:OldRecord) ({Id=456; Caption="Hello World!!!"; Description="Not Yet Described"}:NewRecord)

    """{
    "enums": [
        {
         "name": "Foo.Bar.NewTestState",
         "aliases": ["Foo.Bar.TestState"],
         "default": "Blue"
        }
    ]}"""
    |> evolutionCase "New Enum" (TestState.Green) (NewTestState.Blue)

    """{
        "records": [
            {
                "name": "Foo.Bar.RecordV1",
                "fields": [
                    {"name": "Union", "aliases": [], "default": {"Item":"World!"}}
                ]
            }
        ]
    }"""
    |> evolutionCase "UnknownUnion in Record" ({Union = Case3 "Hello!"}:RecordV2) ({Union = UnionV1.UnknownCase ""}:RecordV1)

    """{
        "records": [
            {
                "name": "Foo.Bar.RecordV3",
                "fields": [
                    {"name": "Union2", "aliases": [], "default": {"Item":"World!"}}
                ]
            }
        ]
    }"""
    |> evolutionCase "Unknown Union field in Record" ({Union = Case3 "Hello!"}:RecordV2) ({Union = Case3 "Hello!"; Union2 = UnknownCase "World!"}:RecordV3)

    """{
        "records": [
            { "name": "Foo.Bar.UnionV1.Case1", "aliases": ["Foo.Bar.UnionV2.Case1"]}
        ]
    }"""
    |> evolutionCase "Array with unknown case"
        [Case1 "Start"; Case3 "Hello!"; Case2; Case1 "End"]
        [UnionV1.Case1 "Start"; UnionV1.UnknownCase ""; UnionV1.UnknownCase ""; UnionV1.Case1 "End"]

    """{
        "records": [
            { "name": "Foo.Bar.UnionV1.Case1", "aliases": ["Foo.Bar.UnionV2.Case1"]}
        ]
    }"""
    |> evolutionCase "Map with unknown case"
        (["I1", Case1 "Start"; "I2", Case3 "Hello!"; "I3", Case2; "I4",Case1 "End"] |> Map.ofList)
        (["I1", UnionV1.Case1 "Start"; "I2", UnionV1.UnknownCase ""; "I3", UnionV1.UnknownCase ""; "I4", UnionV1.Case1 "End"] |> Map.ofList)

    """{
    "records": [
        {"name": "Foo.Bar.GenericRecord2_Of_Uri",
         "aliases": ["Foo.Bar.GenericRecord_Of_Uri"],
         "fields": [
            {"name": "NewValue", "aliases": [], "default": "about:blank"}
        ]}
    ]}"""
    |> evolutionCase "Default value for custom rule type" {Value = System.Uri("http://www.example.com")} {Value = System.Uri("http://www.example.com"); NewValue = System.Uri("about:blank")}

]


let json2SimpleTest (case:SimpleCase) =
    test case.Name {
        let options = { Schema.defaultOptions with
                            TreatDecimalAsDouble = true
                            TreatBigIntAsString = true
                            TreatGuidAsString = true}
        let schema =
            match Schema.generate options (case.InstanceType) with
            | Ok schema -> schema
            | Error err -> failwithf "Schema error: %A" err

        let serializer = Serde.jsonSerializer Serde.defaultSerializerOptions (case.InstanceType) schema
        let deserializer = Serde.jsonDeserializer Serde.defaultDeserializerOptions (case.InstanceType) schema

        use stream = new MemoryStream()
        use writer = new Utf8JsonWriter(stream)
        serializer case.Instance writer

        let data = stream.ToArray()
        //printfn "Serialization result: %s" (System.Text.Encoding.UTF8.GetString(data, 0, data.Length))

        use stream = new MemoryStream(data)
        let copy = deserializer stream

        case.Comparer "Copy should be equal to original" case.Instance copy
    }

let json2EvolutionTest (case:EvolutionCase) =
    test case.Name {
        let options = { Schema.defaultOptions with
                            TreatDecimalAsDouble = true
                            TreatBigIntAsString = true
                            TreatGuidAsString = true}
        let writerSchema =
            match Schema.generate options (case.InstanceType) with
            | Ok schema -> schema
            | Error err -> failwithf "Schema error: %A" err

        let serializer = Serde.jsonSerializer Serde.defaultSerializerOptions (case.InstanceType) writerSchema

        let readerSchema =
            match Schema.generate {options with Annotations = case.Annotations} (case.ExpectedInstance.GetType()) with
            | Ok schema -> schema
            | Error err -> failwithf "Schema error: %A" err
        let deserializer = Serde.jsonDeserializer Serde.defaultDeserializerOptions (case.ExpectedInstance.GetType()) readerSchema

        use stream = new MemoryStream()
        use writer = new Utf8JsonWriter(stream)
        serializer case.Instance writer

        let data = stream.ToArray()
        //printfn "Serialization result: %s" (System.Text.Encoding.UTF8.GetString(data, 0, data.Length))

        use stream = new MemoryStream(data)
        let copy = deserializer stream

        case.Comparer "Deserialized data should be equal to original" case.ExpectedInstance copy
    }

let binary2SimpleTest (case:SimpleCase) =
    test case.Name {
        let options = { Schema.defaultOptions with
                            TreatDecimalAsDouble = false
                            TreatBigIntAsString = false
                            TreatGuidAsString = false}
        let schema =
            match Schema.generate options (case.InstanceType) with
            | Ok schema -> schema
            | Error err -> failwithf "Schema error: %A" err

        let serializer = Serde.binarySerializer Serde.defaultSerializerOptions (case.InstanceType) schema
        let deserializer = Serde.binaryDeserializer Serde.defaultDeserializerOptions (case.InstanceType) schema

        use stream = new MemoryStream()
        serializer case.Instance stream

        let data = stream.ToArray()

        use stream = new MemoryStream(data)
        let copy = deserializer schema stream

        case.Comparer "Copy should be equal to original" case.Instance copy
    }

let binary2EvolutionTest (case:EvolutionCase) =
    test case.Name {
        let options = { Schema.defaultOptions with
                            TreatDecimalAsDouble = false
                            TreatBigIntAsString = false
                            TreatGuidAsString = false}
        let writerSchema =
            match Schema.generate options (case.InstanceType) with
            | Ok schema -> schema
            | Error err -> failwithf "Schema error: %A" err

        let serializer = Serde.binarySerializer Serde.defaultSerializerOptions (case.InstanceType) writerSchema

        let readerSchema =
            match Schema.generate {options with Annotations = case.Annotations} (case.ExpectedInstance.GetType()) with
            | Ok schema -> schema
            | Error err -> failwithf "Schema error: %A" err
        let deserializer = Serde.binaryDeserializer Serde.defaultDeserializerOptions (case.ExpectedInstance.GetType()) readerSchema

        use stream = new MemoryStream()
        serializer case.Instance stream

        let data = stream.ToArray()

        use stream = new MemoryStream(data)
        let copy = deserializer writerSchema stream

        case.Comparer "Deserialized data should be equal to original" case.ExpectedInstance copy
    }


[<Tests>]
let json2SimpleCasesTests =
    simpleCases
    |> List.map (fun list -> testList list.Name (list.Cases |> List.map json2SimpleTest))
    |> testList "Json2"

[<Tests>]
let json2EvolutionTests =
    evolutionCases
    |> List.map json2EvolutionTest
    |> testList "Json2Evolution"

[<Tests>]
let binary2SimpleCasesTests =
    simpleCases
    |> List.map (fun list -> testList list.Name (list.Cases |> List.map binary2SimpleTest))
    |> testList "Binary2"

[<Tests>]
let binary2EvolutionTests =
    evolutionCases
    |> List.map binary2EvolutionTest
    |> testList "Binary2Evolution"