module internal Avro.FSharp.Utils

open System.Text.Json

[<RequireQualifiedAccess>]
module Option =
    let ofBool f (v:bool) = if v then Some(f()) else None

let rec traverse f list =

    let apply fResult xResult =
        match fResult,xResult with
        | Ok f, Ok x -> f x |> Ok
        | Error errs, Ok _ -> Error errs
        | Ok f, Error errs -> Error errs
        | Error errs1, Error errs2 -> Error (errs1 @ errs2)

    let (<*>) = apply
    let cons head tail = head :: tail

    match list with
    | [] -> Ok []
    | head::tail -> Ok cons <*> (f head |> Result.mapError List.singleton) <*> (traverse f tail)

let toJsonElement jsonString =
    let jsonString = sprintf """{"element":%s}""" jsonString
    let doc = JsonDocument.Parse(jsonString)
    doc.RootElement.GetProperty("element")