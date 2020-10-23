module Avro.FSharp.JsonTests

open System.Linq
open System.Collections.Generic
open System.IO
open System.Text.Json
open Expecto
open Expecto.Flip
open Avro.FSharp
open Foo.Bar
open System.Text

let genTest'<'T> comparer name (orig:'T)  =
    test name {
        let factory = InstanceFactory(typeof<'T>, [])
        
        let instanceDirector = InstanceDirector<'T>()
        use stream = new MemoryStream()
        use writer = new Utf8JsonWriter(stream)
        let jsonBuilder = JsonBuilder(writer)
        instanceDirector.Construct(orig, jsonBuilder)

        let data = stream.ToArray()

        printfn "Serialization result: %s" (Encoding.UTF8.GetString(data, 0, data.Length))

        let instanceBuilder = InstanceBuilder(factory)
        let jsonDirector = JsonDirector()
        use stream = new MemoryStream(data)
        jsonDirector.Construct(stream, instanceBuilder)

        let copy = instanceBuilder.Instance :?> 'T
        
        comparer "Copy should be equal to original" orig copy
    }

let genTest<'T when 'T : equality>  = genTest'<'T> Expect.equal 
let compareSequences msg expected actual = Expect.isTrue msg <| System.Linq.Enumerable.SequenceEqual(expected, actual)
let compareDictionaries<'TKey,'TValue> msg (expected:Dictionary<'TKey,'TValue>) (actual:Dictionary<'TKey,'TValue>) = 
    compareSequences msg (expected.OrderBy(fun kv -> kv.Key).ToList()) (actual.OrderBy(fun kv -> kv.Key).ToList())

let genEvolutionTest<'TSource, 'TDest when 'TDest:equality>  name (orig:'TSource) (expectedData:'TDest) =
    test name {
        let factory = InstanceFactory(typeof<'TSource>, [])
        
        let instanceDirector = InstanceDirector<'TSource>()
        use stream = new MemoryStream()
        use writer = new Utf8JsonWriter(stream)
        let jsonBuilder = JsonBuilder(writer)
        instanceDirector.Construct(orig, jsonBuilder)

        let data = stream.ToArray()

        let destFactory = InstanceFactory(typeof<'TDest>, [])
        let instanceBuilder = InstanceBuilder(destFactory)
        let jsonDirector = JsonDirector()
        use stream = new MemoryStream(data)
        jsonDirector.Construct(stream, instanceBuilder)

        Expect.equal "Deserialized data should be equal to original" expectedData (instanceBuilder.Instance :?> 'TDest)
    }

[<Tests>]
let primitiveTests =
    testList "Primitive" [
        genTest "string" "Hello World!!!"
        genTest "empty string" ""
        genTest "bool true" true
        genTest "bool false" false
        genTest "int" 125
        genTest "float" 543.
        genTest "long" 789L
        genTest "float32" 101.2f
    ]|> testLabel "Json"    

[<Tests>]
let enumTests =
    testList "Enum" [
        genTest "SimpleEnum" TestState.Green
    ]|> testLabel "Json"

[<Tests>]
let arrayTests =
    testList "Array" [
        genTest "List" ["One"; "Two"; "Three"]
        genTest "Array" [|"One"; "Two"; "Three"|]
        genTest' compareSequences "Collection" (List(["One"; "Two"; "Three"])) 
        genTest "RecordWithArray" {Value = ["Name1"; "Name2"; "Name3"]}
            
    ]|> testLabel "Json"

[<Tests>]
let mapTests =
    testList "Map" [
        let pairs = ["One", 1; "Two", 2; "Three", 3]
        
        genTest "Map" (pairs |> Map.ofList)
        
        let dict = Dictionary<string,int>()
        pairs |> Seq.iter dict.Add        
        genTest' compareDictionaries "Dictionary" dict 
        
        genTest "RecordWithMap" {Value = pairs |> Map.ofList}
            
    ]|> testLabel "Json"

[<Tests>]
let recordTests =
    testList "Record" [
        let record = {Id = 1; Name = "Hello World!!!"; Version = 2L}
        genTest "SimpleRecord" record
        genTest "HierarcyRecord" {Title = "Top"; Details = record; IsProcessed = true}
    ]|> testLabel "Json"

[<Tests>]
let tupleTests =
    testList "Tuple" [
        genTest "Tuple" (123, "Hello")        
        genTest "Tuple Record" {Value = (123, "Hello")}
    ]|> testLabel "Json"

[<Tests>]
let unionTests =
    testList "Union" [
        genTest "Result Ok" ((Ok "Hello"):Result<string,string>)
        genTest "Result Error" ((Error "Hello"):Result<string,string>)
        genTest "BinaryTree" (Node (Leaf "XXX", Node (Leaf "YYY", Leaf "ZZZ")))
        
    ]|> testLabel "Json"

[<Tests>]
let nullableTests =
    testList "Nullable" [
        genTest "Option-Some" (Some "Hello World!!!")
        genTest "Option-None" (None:Option<string>)        
    ]|> testLabel "Json"

[<Tests>]
let logicalTypesTests =
    testList "LogicalTypes" [
        genTest "Decimal" 3.1415926m
        genTest "Scaled Decimal" {Id = 124; Caption = ""; Price = 199.99m}        
        genTest "ItemRecord" {Id = 123; Name ="Item"; Price = Price 9.99m}
    ]|> testLabel "Json"    

[<Tests>]
let complexTests =
    testList "ComplexTests" [
        let basket:Basket = [
            SaleItem (Product("XXX-1", "Product 1", 10m<GBP/Q>), 1m<Q>)
            SaleItem (Product("XXX-2", "Product 2", 1m<GBP/Q>), 10m<Q>)
            SaleItem (Product("XXX-3", "Product 3", 3.14m<GBP/Q>), 5m<Q>)
            TenderItem (Cash, 100m<GBP>)
            TenderItem ((Card "1111-1111-1111-1111"), 15m<GBP>)
            CancelItem 2
        ]
        genTest "Basket" basket

    ]|> testLabel "Json"   

[<Tests>]
let customTypesTests =
    testList "CustomTypes" [
        genTest "Single Guid" (System.Guid.NewGuid())
        genTest "Guid" {Value = System.Guid.NewGuid()}
        genTest "Uri" {Value = System.Uri("http://www.example.com")}
        genTest "DateTime" {Value = System.DateTime.UtcNow}
        genTest "DateTimeOffset" {Value = System.DateTimeOffset.UtcNow}
        genTest "TimeSpan" {Value = System.TimeSpan.FromSeconds(321.5)}
        
    ]|> testLabel "Json"    

[<Tests>]
let evolutionTests =
    testList "EvolutionTests" [
        genEvolutionTest "Added string field" ({Id=456}:RecordWithId) ({Id=456; NewField="Hello" }:RecordWithNewField)
        genEvolutionTest "New Record" ({Id=456; Title="Hello World!!!"}:OldRecord) ({Id=456; Caption="Hello World!!!"; Description="Not Yet Described"}:NewRecord)
        genEvolutionTest "New Enum" (TestState.Green) (NewTestState.Blue)

    ]|> testLabel "Json"
