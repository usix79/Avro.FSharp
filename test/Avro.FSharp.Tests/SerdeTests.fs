module Avro.FSharp.SerdeTests

open System.Linq
open System.Collections.Generic
open System.IO
open System.Text.Json
open Expecto
open Expecto.Flip
open Avro.FSharp
open Foo.Bar

type Comparer = string -> obj -> obj -> unit
type SimpleCase = {Name: string; Instance: obj; InstanceType: System.Type; Comparer: Comparer }
type SimpleCaseList = {Name: string; Cases: SimpleCase list}
type EvolutionCase = {Name: string; Instance: obj; InstanceType: System.Type; ExpectedInstance: obj; Comparer: Comparer }

let simpleCase'<'T> comparer name (instance:'T) =
    {Name = name; Comparer = comparer; Instance = instance; InstanceType = typeof<'T>}

let simpleCase<'T when 'T:equality> name (instance:'T) =
    {Name = name; Comparer = (fun msg v1 v2 -> Expect.equal msg (v1:?>'T) (v2:?>'T)); Instance = instance; InstanceType = typeof<'T>}

let simpleCaseList name cases = {Name = name; Cases = cases}

let compareSequences<'T> msg (expected:obj) (actual:obj) =
    Expect.isTrue msg <| System.Linq.Enumerable.SequenceEqual((expected :?> IEnumerable<'T>), (actual :?> IEnumerable<'T>))

let compareDictionaries<'TKey,'TValue> msg (expected:obj) (actual:obj) =
    compareSequences<KeyValuePair<'TKey,'TValue>> msg ((expected :?> Dictionary<'TKey,'TValue>).OrderBy(fun kv -> kv.Key).ToList()) ((actual :?> Dictionary<'TKey,'TValue>).OrderBy(fun kv -> kv.Key).ToList())

let evolutionCase<'TSource, 'TDest when 'TDest:equality> name (instance:'TSource) (expectedInstance:'TDest) =
    {Name = name; Comparer = (fun msg v1 v2 -> Expect.equal msg (v1:?>'TDest) (v2:?>'TDest)); Instance = instance; InstanceType = typeof<'TSource>; ExpectedInstance = expectedInstance}

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
    ]
    simpleCaseList "Enum" [
        simpleCase "SimpleEnum" TestState.Green
    ]
    simpleCaseList "Array" [
        simpleCase "List" ["One"; "Two"; "Three"]
        simpleCase "Array" [|"One"; "Two"; "Three"|]
        simpleCase' compareSequences "Collection" (List(["One"; "Two"; "Three"]))
        simpleCase "RecordWithArray" {Value = ["Name1"; "Name2"; "Name3"]}
    ]
    simpleCaseList "Map" [
        let pairs = ["One", 1; "Two", 2; "Three", 3]

        simpleCase "Map" (pairs |> Map.ofList)

        let dict = Dictionary<string,int>()
        pairs |> Seq.iter dict.Add
        simpleCase' compareDictionaries<string, int32> "Dictionary" dict

        simpleCase "RecordWithMap" {Value = pairs |> Map.ofList}
    ]
    simpleCaseList "Record" [
        let record = {Id = 1; Name = "Hello World!!!"; Version = 2L}
        simpleCase "SimpleRecord" record
        simpleCase "HierarcyRecord" {Title = "Top"; Details = record; IsProcessed = true}
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
        simpleCase "Single Guid" (System.Guid.NewGuid())
        simpleCase "Guid" {Value = System.Guid.NewGuid()}
        simpleCase "Uri" {Value = System.Uri("http://www.example.com")}
        simpleCase "DateTime" {Value = System.DateTime.UtcNow}
        simpleCase "DateTimeOffset" {Value = System.DateTimeOffset.UtcNow}
        simpleCase "TimeSpan" {Value = System.TimeSpan.FromSeconds(321.5)}

    ]
]

let evolutionCases = [
    evolutionCase "Added string field" ({Id=456}:RecordWithId) ({Id=456; NewField="Hello" }:RecordWithNewField)
    evolutionCase "New Record" ({Id=456; Title="Hello World!!!"}:OldRecord) ({Id=456; Caption="Hello World!!!"; Description="Not Yet Described"}:NewRecord)
    evolutionCase "New Enum" (TestState.Green) (NewTestState.Blue)
]

let jsonSimpleTest (case:SimpleCase) =
    test case.Name {
        let factory = InstanceFactory(case.InstanceType, [])

        let instanceDirector = InstanceDirector(factory)
        use stream = new MemoryStream()
        use writer = new Utf8JsonWriter(stream)
        let jsonBuilder = JsonBuilder(writer)
        instanceDirector.Construct(case.Instance, jsonBuilder)

        let data = stream.ToArray()

        //printfn "Serialization result: %s" (System.Text.Encoding.UTF8.GetString(data, 0, data.Length))

        let instanceBuilder = InstanceBuilder(factory)
        let jsonDirector = JsonDirector()
        use stream = new MemoryStream(data)
        jsonDirector.Construct(stream, instanceBuilder)

        let copy = instanceBuilder.Instance

        case.Comparer "Copy should be equal to original" case.Instance copy
    }

let jsonEvolutionTest (case:EvolutionCase) =
    test case.Name {
        let factory = InstanceFactory(case.InstanceType, [])

        let instanceDirector = InstanceDirector(factory)
        use stream = new MemoryStream()
        use writer = new Utf8JsonWriter(stream)
        let jsonBuilder = JsonBuilder(writer)
        instanceDirector.Construct(case.Instance, jsonBuilder)

        let data = stream.ToArray()

        let destFactory = InstanceFactory(case.ExpectedInstance.GetType(), [])
        let instanceBuilder = InstanceBuilder(destFactory)
        let jsonDirector = JsonDirector()
        use stream = new MemoryStream(data)
        jsonDirector.Construct(stream, instanceBuilder)

        case.Comparer "Deserialized data should be equal to original" case.ExpectedInstance instanceBuilder.Instance
    }

[<Tests>]
let jsonSimpleCasesTests =
    simpleCases
    |> List.map (fun list -> testList list.Name (list.Cases |> List.map jsonSimpleTest))
    |> testList "Json"

[<Tests>]
let jsonEvolutionCasesTests =
    evolutionCases
    |> List.map jsonEvolutionTest
    |> testList "Json"


let binarySimpleTest (case:SimpleCase) =
    test case.Name {
        let factory = InstanceFactory(case.InstanceType, [])

        let director = InstanceDirector(factory)
        use stream = new MemoryStream()
        use writer = new BinaryWriter(stream, System.Text.Encoding.UTF8)
        let builder = BinaryBuilder(writer)
        director.Construct(case.Instance, builder)

        let data = stream.ToArray()

        let instanceBuilder = InstanceBuilder(factory)
        let binaryDirector = BinaryDirector()
        use stream = new MemoryStream(data)
        use reader = new BinaryReader(stream)
        binaryDirector.Construct(reader, (factory :> IInstanceFactory).TargetSchema, instanceBuilder)

        let copy = instanceBuilder.Instance

        case.Comparer "Copy should be equal to original" case.Instance copy
    }

let binaryEvolutionTest (case:EvolutionCase) =
    test case.Name {
        let factory = InstanceFactory(case.InstanceType, [])

        let instanceDirector = InstanceDirector(factory)
        use stream = new MemoryStream()
        use writer = new BinaryWriter(stream, System.Text.Encoding.UTF8)
        let binaryBuilder = BinaryBuilder(writer)
        instanceDirector.Construct(case.Instance, binaryBuilder)

        let data = stream.ToArray()

        let destFactory = InstanceFactory(case.ExpectedInstance.GetType(), [])
        let instanceBuilder = InstanceBuilder(destFactory)
        let binaryDirector = BinaryDirector()
        use stream = new MemoryStream(data)
        use reader = new BinaryReader(stream)
        binaryDirector.Construct(reader, (factory :> IInstanceFactory).TargetSchema, instanceBuilder)

        case.Comparer "Deserialized data should be equal to original" case.ExpectedInstance instanceBuilder.Instance
    }

[<Tests>]
let binarySimpleCasesTests =
    simpleCases
    |> List.map (fun list -> testList list.Name (list.Cases |> List.map binarySimpleTest))
    |> testList "Binary"

[<Tests>]
let binaryEvolutionCasesTests =
    evolutionCases
    |> List.map binaryEvolutionTest
    |> testList "Binary"
