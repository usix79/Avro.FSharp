module SerdeTests

open System.IO
open System.Collections.Generic
open System.Linq
open Avro
open Avro.IO
open Avro.FSharp
open Avro.FSharp.Schema
open Expecto
open Expecto.Flip
open Foo.Bar

let genTest'<'T> comparer name (data:'T)  =
    test name {
        match generateSchemaAndReflector [||] typeof<'T> with
        | Ok (schema, reflector) -> 
            let schema = Schema.Parse(schema.ToString())
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
        match generateSchemaAndReflector [||] typeof<'TSource> with
        | Ok (writerSchema, reflector) -> 
            let writerSchema = Schema.Parse(writerSchema.ToString())
            let writer = FSharpWriter<'TSource>(writerSchema, reflector)                
            use writerStream = new MemoryStream(256)
            writer.Write(data, BinaryEncoder(writerStream))

            match generateSchemaAndReflector [||] typeof<'TDest> with
            | Ok (readerSchema, reflector) -> 
                let readerSchema = Schema.Parse(readerSchema.ToString())
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
    ftestList "EvolutionTests" [
        //genEvolutionTest "Added nullable field" ({Id=456}:RecordWithId) ({Id=456; NewId=None }:RecordWithNewId)
        genEvolutionTest "Added string field" ({Id=456}:RecordWithId) ({Id=456; NewField="Hello" }:RecordWithNewField)

    ]|> testLabel "Serde"    