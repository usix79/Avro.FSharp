namespace Avro.FSharp.Annotations

type DefaultValueAttribute (defaultValue:string) =
    inherit System.Attribute()    
    member __.Value = defaultValue

type AliasesAttribute (aliases:string array) =
    inherit System.Attribute()
    member __.Aliases = aliases

type ScaleAttribute (scale:int) =
    inherit System.Attribute()
    member __.Scale = scale    