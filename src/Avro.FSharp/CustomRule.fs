namespace Avro.FSharp

open System
open System.Globalization

type CustomRule(type':System.Type, schema:string, writeCast:obj->obj, readCast:obj->obj) =
    member _.TargetType = type'
    member _.Schema = schema
    member _.WriteCast = writeCast
    member _.ReadCast = readCast


module CustomRules =
    let buidInRules = [|
        CustomRule(typeof<Guid>, 
            """{"type": "fixed", "name": "guid", "size": 16}""", 
            (fun v -> (v :?> Guid).ToByteArray() :> obj),
            (fun v -> (v :?> byte[]) |> Guid :> obj))
        CustomRule(typeof<Uri>, 
            """{"type": "string"}""", 
            (fun v -> v.ToString() :> obj),
            (fun v -> (v :?> string) |> Uri :> obj))
        CustomRule(typeof<DateTime>,
            """{"type": "string"}""", 
            (fun v -> (v :?> DateTime).ToString("O", CultureInfo.InvariantCulture) :> obj),
            (fun v -> DateTime.Parse((v :?> string), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) :> obj))
        CustomRule(typeof<DateTimeOffset>,
            """{"type": "string"}""", 
            (fun v -> (v :?> DateTimeOffset).ToString("O", CultureInfo.InvariantCulture) :> obj),
            (fun v -> DateTimeOffset.Parse((v :?> string), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) :> obj))
        CustomRule(typeof<TimeSpan>,
            """{"type": "string"}""", 
            (fun v -> (v :?> TimeSpan) |> Xml.XmlConvert.ToString :> obj),
            (fun v -> Xml.XmlConvert.ToTimeSpan(v :?> string) :> obj))
    |]