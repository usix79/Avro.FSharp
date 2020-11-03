module Foo.Bar

open Avro.FSharp.Annotations


type TestState =
    | Red = 2
    | Yellow = 1
    | Green = 0

type SimpleRecord = {
    Id : int
    Name : string
    Version : int64
}

type ParentRecord = {
    Chield1 : SimpleRecord
    Chield2 : SimpleRecord
}

type HierarchyRecord = {
    Title: string
    Details: SimpleRecord
    IsProcessed: bool
}

type Price = Price of decimal

type ItemRecord = {
    Id : int
    Name : string
    Price : Price
}

type RecordWithId = {
    Id : int
}

[<Aliases[|"Foo.Bar.RecordWithId"|]>] 
type RecordWithNewId = {
    Id : int
    //[<DefaultValue("null")>]
    NewId: int option
}

[<Aliases[|"Foo.Bar.RecordWithId"|]>] 
type RecordWithNewField = {
    Id : int
    [<DefaultValue("Hello")>]
    NewField: string
}

type RecordWithOption = {
    Id : int
    Id2 : int option
}

type GenericRecord<'T> = {
    Value : 'T
}

type BinaryTree =
    | Leaf of value:string
    | Node of left: BinaryTree * right: BinaryTree

type PriceRecord = {
    Id : int
    Caption: string
    [<Scale 3>]
    Price : decimal
}


type OldRecord = {
    Id : int
    Title: string
}


[<Aliases[|"Foo.Bar.OldRecord"|]>] 
type NewRecord = {
    Id : int
    [<Aliases [|"Title"; "Cap"|]>] 
    Caption: string
    [<DefaultValue "Not Yet Described">]
    Description : string
}

[<Aliases[|"Foo.Bar.TestState"|];DefaultValue("Blue")>] 
type NewTestState =
    | Red = 3
    | Yellow = 2
    | Blue = 10

// Snippet link: http://fssnip.net/kW
// Authors: Tomas Petricek & Phil Trelford

type [<Measure>] GBP
type [<Measure>] Q

type UnitPrice = decimal<GBP/Q>
type Amount = decimal<GBP>
type Name = string
type Code = string
type Quantity = decimal<Q>

type Product = Code * Name * UnitPrice

type Tender = 
  | Cash 
  | Card of string 
  | Voucher of Code 

type LineItem =
  | SaleItem of Product * Quantity
  | TenderItem of Tender * Amount
  | CancelItem of int

type Basket = list<LineItem>