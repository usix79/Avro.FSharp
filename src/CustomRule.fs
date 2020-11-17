namespace Avro.FSharp

open System

type CustomRule = {
    InstanceType: System.Type;
    SurrogateType: System.Type
    CastToSurrogate: obj -> obj
    CastFromSurrogate: obj -> obj
    StubValue: string   // JSON
}

module CustomRule =
    let buidInRules = [
        {
            InstanceType = typeof<Uri>
            SurrogateType = typeof<string>
            CastToSurrogate = fun v -> v.ToString() |> box
            CastFromSurrogate = fun v -> Uri(unbox(v)) |> box
            StubValue =  """ "" """ // JSON with empty string
        }
    ]