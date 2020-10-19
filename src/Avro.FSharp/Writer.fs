namespace Avro.FSharp

open Avro
open Avro.IO
open Avro.Generic
open System
open System.Numerics

type FSharpWriter<'T>(schema:Schema, reflector:SchemaReflector) =
    inherit DefaultWriter(schema)
    
    interface DatumWriter<'T> with
        member _.Schema: Schema = schema
        member this.Write(datum: 'T, encoder: IO.Encoder) = this.Write(datum, encoder)

    override  this.WriteRecord (schema:RecordSchema, value:obj, encoder:Encoder) =
        let fieldValues = reflector.GetFieldValues schema.Fullname value
        for i in 0 .. schema.Fields.Count - 1 do
            let field = schema.Fields.[i]
            let fieldValue = fieldValues.[i]
            try
                this.Write(field.Schema, fieldValue, encoder);
            with
            | ex -> raise <| AvroException(ex.Message + " in field " + field.Name, ex)
    
    override _.WriteEnum(schema:EnumSchema, value:obj, encoder:Encoder) =
        encoder.WriteEnum(schema.Ordinal(value.ToString()))

    override this.WriteArray (schema:ArraySchema, value:obj, encoder:Encoder) =
        encoder.WriteArrayStart()
        encoder.SetItemCount(reflector.GetArraySize value |> int64)
        for v in reflector.GetArrayItems value do
            encoder.StartItem()
            this.Write(schema.ItemSchema, v, encoder)
        encoder.WriteArrayEnd()

    override this.WriteMap (schema:MapSchema, value:obj, encoder:Encoder) =
        encoder.WriteMapStart()
        encoder.SetItemCount(reflector.GetMapSize value |> int64)
        for item in reflector.GetMapItems value do
            encoder.StartItem()
            encoder.WriteString(item.Key :?> string)
            this.Write(schema.ValueSchema, item.Value, encoder)
        encoder.WriteMapEnd()

    override this.WriteUnion(us:UnionSchema, value:obj, encoder:Encoder) =
        if us.Schemas.Count = 2 && us.[0].Tag = Schema.Type.Null 
        then // nullable
            match value with
            | null -> encoder.WriteUnionIndex(0)
            | _ -> 
                encoder.WriteUnionIndex(1)
                this.Write(us.[1], reflector.WriteCast value, encoder)
        else // descriminated union
            let idx = reflector.GetCaseIdx us.[0].Fullname value
            encoder.WriteUnionIndex(idx)
            this.Write(us.[idx], value, encoder)

    override _.WriteFixed(es:FixedSchema, value:obj, encoder:Encoder) =
        match reflector.WriteCast value with
        | :? array<byte> as bytes -> encoder.WriteFixed bytes
        | _ -> failwithf "fixed value should be casted to byte[] %A" value

    override _.WriteLogical(ls:LogicalSchema, value:obj, encoder:Encoder) =
        match ls.LogicalTypeName with
        | "decimal" -> 
            let value = value :?> decimal
            let scale = ls.GetProperty("scale") |> System.Int32.Parse
            let intValue =  (value * decimal (Math.Pow(10., float scale))) |> Decimal.Truncate |> BigInteger
            let byteValue = intValue.ToByteArray()
            Array.Reverse(byteValue)
            base.Write(ls.BaseSchema, byteValue, encoder)
        | _ -> base.Write(ls.BaseSchema, value, encoder)

    override this.Write<'TargetType>(value:obj, tag:Schema.Type, writer:Writer<'TargetType>) =
        match value with
        | :? 'TargetType as typedObj -> writer.Invoke(typedObj)
        | _ -> 
            match reflector.WriteCast value with
            | :? 'TargetType as typedObj -> writer.Invoke(typedObj)
            | _ -> raise <| this.TypeMismatch(value, tag.ToString(), (typeof<'TargetType>.ToString()))