namespace Avro.FSharp

open System
open System.Globalization

type CustomRule = {
    TargetType: System.Type;
    Schema: string
    WriteCast: obj -> obj
    ReadCast: obj -> obj
}

module CustomRule =
    let buidInRules = [
        {
            TargetType = typeof<Guid> 
            Schema = """{"type": "fixed", "name": "guid", "size": 16}"""
            WriteCast = fun v -> (v :?> Guid).ToByteArray() :> obj
            ReadCast = fun v -> (v :?> byte[]) |> Guid :> obj
        }
        {
            TargetType = typeof<Uri>
            Schema = """{"type": "string"}"""
            WriteCast = fun v -> v.ToString() :> obj
            ReadCast = fun v -> (v :?> string) |> Uri :> obj
        }
        {
            TargetType = typeof<DateTime>
            Schema = """{"type": "string"}"""
            WriteCast = fun v -> (v :?> DateTime).ToString("O", CultureInfo.InvariantCulture) :> obj
            ReadCast = fun v -> DateTime.Parse((v :?> string), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) :> obj
        }
        {
            TargetType = typeof<DateTimeOffset>
            Schema = """{"type": "string"}"""
            WriteCast = fun v -> (v :?> DateTimeOffset).ToString("O", CultureInfo.InvariantCulture) :> obj
            ReadCast = fun v -> DateTimeOffset.Parse((v :?> string), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) :> obj
        }
        {
            TargetType = typeof<TimeSpan>
            Schema = """{"type": "string"}"""
            WriteCast = fun v -> (v :?> TimeSpan) |> Xml.XmlConvert.ToString :> obj
            ReadCast = fun v -> Xml.XmlConvert.ToTimeSpan(v :?> string) :> obj
        }
    ]