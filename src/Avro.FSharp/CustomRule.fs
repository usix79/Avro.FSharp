namespace Avro.FSharp


type CustomRule(type':System.Type, schema:string, writeCast:obj->obj, readCast:obj->obj) =
    member _.TargetType = type'
    member _.Schema = schema
    member _.WriteCast = writeCast
    member _.ReadCast = readCast