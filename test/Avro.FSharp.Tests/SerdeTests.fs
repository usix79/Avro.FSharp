module SerdeTests

open System.IO
open System.Collections.Generic
open System.Linq
open Avro
open Avro.IO
open Avro.FSharp
open Expecto
open Expecto.Flip
open Foo.Bar


let genTest'<'T> comparer name (data:'T)  =
    test name {
        match Schema.generateWithReflector [] typeof<'T> with
        | Ok (schema, reflector) -> 
            let schema = Schema.Parse(schema |> Schema.toString)
            let writer = FSharpWriter<'T>(schema, reflector)                
            use writerStream = new MemoryStream(256)
            writer.Write(data, BinaryEncoder(writerStream))

            use readerStream = new MemoryStream(writerStream.ToArray())
            let reader = FSharpReader<'T>(schema, schema, reflector)
            let deserializedData = reader.Read(Unchecked.defaultof<'T>, BinaryDecoder(readerStream))
            comparer "Deserialized data should be equal to original" data deserializedData
        | Error err -> failwithf "Schema error %A" err 
    }

let genTest<'T when 'T : equality>  = genTest'<'T> Expect.equal 
let compareSequences msg expected actual = Expect.isTrue msg <| System.Linq.Enumerable.SequenceEqual(expected, actual)
let compareDictionaries<'TKey,'TValue> msg (expected:Dictionary<'TKey,'TValue>) (actual:Dictionary<'TKey,'TValue>) = 
    compareSequences msg (expected.OrderBy(fun kv -> kv.Key).ToList()) (actual.OrderBy(fun kv -> kv.Key).ToList())

let genEvolutionTest<'TSource, 'TDest when 'TDest:equality>  name (data:'TSource) (expectedData:'TDest) =
    test name {
        match Schema.generateWithReflector [] typeof<'TSource> with
        | Ok (writerSchema, reflector) -> 
            let writerSchema = Schema.Parse(writerSchema |> Schema.toString)
            let writer = FSharpWriter<'TSource>(writerSchema, reflector)                
            use writerStream = new MemoryStream(256)
            writer.Write(data, BinaryEncoder(writerStream))

            match Schema.generateWithReflector [] typeof<'TDest> with
            | Ok (readerSchema, reflector) -> 
                let readerSchema = Schema.Parse(readerSchema |> Schema.toString)
                use readerStream = new MemoryStream(writerStream.ToArray())
                let reader = FSharpReader<'TDest>(writerSchema, readerSchema, reflector)
                let deserializedData = reader.Read(Unchecked.defaultof<'TDest>, BinaryDecoder(readerStream))
                Expect.equal "Deserialized data should be equal to original" expectedData deserializedData
            | Error err -> failwithf "Reader Schema error %A" err 
        | Error err -> failwithf "Writer Schema error %A" err 
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
    ]|> testLabel "Serde"    

[<Tests>]
let enumTests =
    testList "Enum" [
        genTest "SimpleEnum" TestState.Green
    ]|> testLabel "Serde"

[<Tests>]
let arrayTests =
    testList "Array" [
        genTest "List" ["One"; "Two"; "Three"]
        genTest "Array" [|"One"; "Two"; "Three"|]
        genTest' compareSequences "Collection" (List(["One"; "Two"; "Three"])) 
        genTest "RecordWithArray" {Value = ["Name1"; "Name2"; "Name3"]}
            
    ]|> testLabel "Serde"

[<Tests>]
let mapTests =
    testList "Map" [
        let pairs = ["One", 1; "Two", 2; "Three", 3]
        
        genTest "Map" (pairs |> Map.ofList)
        
        let dict = Dictionary<string,int>()
        pairs |> Seq.iter dict.Add        
        genTest' compareDictionaries "Dictionary" dict 
        
        genTest "RecordWithMap" {Value = pairs |> Map.ofList}
            
    ]|> testLabel "Serde"

[<Tests>]
let recordTests =
    testList "Record" [
        let record = {Id = 1; Name = "Hello World!!!"; Version = 2L}
        genTest "SimpleRecord" record
        genTest "HierarcyRecord" {Title = "Top"; Details = record; IsProcessed = true}
    ]|> testLabel "Serde"

[<Tests>]
let unionTests =
    testList "Union" [
        genTest "Option-Some" (Some "Hello World!!!")
        genTest "Option-None" (None:Option<string>)
        genTest "Result Ok" ((Ok "Hello"):Result<string,string>)
        genTest "Result Error" ((Error "Hello"):Result<string,string>)
        let tree = Node (Leaf "XXX", Node (Leaf "YYY", Leaf "ZZZ"))
        genTest "BinaryTree" tree
        genTest "ItemRecord" {Id = 123; Name ="Item"; Price = Price 9.99m}
        
    ]|> testLabel "Serde"

[<Tests>]
let tupleTests =
    testList "Tuple" [
        genTest "Tuple" (123, "Hello")        
        genTest "Tuple Record" {Value = (123, "Hello")}
    ]|> testLabel "Serde"

[<Tests>]
let logicalTypesTests =
    testList "LogicalTypes" [
        genTest "Decimal" 3.1415926m
        genTest "Scaled Decimal" {Id = 124; Caption = ""; Price = 199.99m}        
    ]|> testLabel "Serde"    

[<Tests>]
let customTypesTests =
    testList "CustomTypes" [
        genTest "Single Guid" (System.Guid.NewGuid())
        genTest "Guid" {Value = System.Guid.NewGuid()}
        genTest "Uri" {Value = System.Uri("http://www.example.com")}
        genTest "DateTime" {Value = System.DateTime.UtcNow}
        genTest "DateTimeOffset" {Value = System.DateTimeOffset.UtcNow}
        genTest "TimeSpan" {Value = System.TimeSpan.FromSeconds(321.5)}
        
    ]|> testLabel "Serde"    

[<Tests>]
let evolutionTests =
    testList "EvolutionTests" [
        genEvolutionTest "Added string field" ({Id=456}:RecordWithId) ({Id=456; NewField="Hello" }:RecordWithNewField)
        genEvolutionTest "New Record" ({Id=456; Title="Hello World!!!"}:OldRecord) ({Id=456; Caption="Hello World!!!"; Description="Not Yet Described"}:NewRecord)
        //genEvolutionTest "New Enum" (TestState.Green) (NewTestState.Blue)

    ]|> testLabel "Serde"

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

    ]|> testLabel "Serde"   