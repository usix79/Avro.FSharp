namespace Avro.FSharp

open System
open System.Globalization

type CustomRule = {
    TargetType: System.Type;
    Schema: string
    SerializationCast: obj -> obj
    DeserializationCast: obj -> obj
}

module CustomRule =
    let buidInRules = [
        {
            TargetType = typeof<Guid>
            Schema = """{"type": "fixed", "name": "guid", "size": 16}"""
            SerializationCast = fun v -> (v :?> Guid).ToByteArray() :> obj
            DeserializationCast = fun v -> (v :?> byte[]) |> Guid :> obj
        }
        {
            TargetType = typeof<Uri>
            Schema = """{"type": "string"}"""
            SerializationCast = fun v -> v.ToString() :> obj
            DeserializationCast = fun v -> (v :?> string) |> Uri :> obj
        }
        {
            TargetType = typeof<DateTime>
            Schema = """{"type": "string"}"""
            SerializationCast = fun v -> (v :?> DateTime).ToString("O", CultureInfo.InvariantCulture) :> obj
            DeserializationCast = fun v -> DateTime.Parse((v :?> string), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) :> obj
        }
        {
            TargetType = typeof<DateTimeOffset>
            Schema = """{"type": "string"}"""
            SerializationCast = fun v -> (v :?> DateTimeOffset).ToString("O", CultureInfo.InvariantCulture) :> obj
            DeserializationCast = fun v -> DateTimeOffset.Parse((v :?> string), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) :> obj
        }
        {
            TargetType = typeof<TimeSpan>
            Schema = """{"type": "string"}"""
            SerializationCast = fun v -> (v :?> TimeSpan) |> Xml.XmlConvert.ToString :> obj
            DeserializationCast = fun v -> Xml.XmlConvert.ToTimeSpan(v :?> string) :> obj
        }
    ]