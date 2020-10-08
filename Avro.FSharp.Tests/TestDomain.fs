module Foo.Bar

open Avro.FSharp.Annotations

type Price = Price of decimal

type TestState =
    | Red = 3
    | Yellow = 2
    | Green = 1

type SimpleRecord = {
    Id : int
    Name : string
    Version : int64
}

type ItemRecord = {
    Id : int
    Name : string
    Price : Price
}

type HierarchyRecord = {
    Title: string
    Details: SimpleRecord
    IsProcessed: bool

}

type BinaryTree =
    | Leaf of value:string
    | Node of left: BinaryTree * right: BinaryTree


type OldRecord = {
    Id : int
    Title: string
}

[<Aliases[|"Foo.Bar.OldRecord"|]>] 
type NewRecord = {
    [<DefaultValue "42">]
    Id : int
    [<Aliases [|"Title"; "Cap"|]>] 
    Caption: string
    [<Scale 3>]
    Price : decimal
}

[<Aliases[|"Foo.Bar.TestState"|]>] 
type NewTestState =
    | Red = 3
    | Yellow = 2
    | Green = 1
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