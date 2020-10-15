namespace Avro.FSharp

open System
open Avro
open Avro.IO
open Avro.Generic
open Avro.Specific
open System
open System.Numerics
open System.Collections.Generic

type FSharpReader<'T>(writerSchema:Schema, readerSchema:Schema, reflector:SchemaReflector) =
    inherit DefaultReader(writerSchema, readerSchema)
    let rootReaderSchema = readerSchema

    let resolverType = 
        typeof<Avro.IO.Encoder>.Assembly.GetTypes()
        |> Array.find (fun t -> t.FullName = "Avro.IO.Resolver")
    
    let encodeDefaultValueMI = resolverType.GetMethod("EncodeDefaultValue")
    let encodeDefaultValue (enc, schema, jtok) = encodeDefaultValueMI.Invoke(null, [|enc; schema; jtok|])

    
    interface DatumReader<'T> with
        member _.WriterSchema: Schema = writerSchema
        member _.ReaderSchema: Schema = readerSchema
        member this.Read(reuse: 'T, decoder: IO.Decoder): 'T = this.Read(reuse, decoder)

    override this.ReadRecord(reuse:obj, writerSchema:RecordSchema, readerSchema:Schema, decoder:Decoder) =
        let recordSchema = readerSchema :?> RecordSchema
        let args = Array.zeroCreate recordSchema.Count
        for wf in writerSchema do
            try
                match recordSchema.TryGetFieldAlias wf.Name with
                | true, rf -> args.[rf.Pos] <- this.Read(reuse, wf.Schema, rf.Schema, decoder)
                | _ -> this.Skip(wf.Schema, decoder) 
            with 
            | ex -> raise <| AvroException(ex.Message + " in field " + wf.Name, ex)

        args
        |> Array.mapi (fun idx obj ->
            match obj with
            | null ->
                let rf = recordSchema.Fields.[idx]
                printfn "Field: %A" rf.Name
                use defaultStream = new IO.MemoryStream()
                let defaultEncoder = BinaryEncoder(defaultStream)
                let defaultDecoder = BinaryDecoder(defaultStream)
                encodeDefaultValue(defaultEncoder, rf.Schema, rf.DefaultValue) |> ignore
                defaultStream.Flush()
                defaultStream.Position <- 0L // reset for reading
                this.Read(null, rf.Schema, rf.Schema, defaultDecoder) 

            | _ -> obj
        )
        |> reflector.CreateRecord recordSchema.Fullname

    override _.ReadEnum(_:obj, writerSchema:EnumSchema, readerSchema:Schema, decoder:Decoder) =
        let es = readerSchema :?> EnumSchema;
        let symbol = writerSchema.[decoder.ReadEnum()]
        reflector.GetEnumValue es.Fullname symbol


    member private _.ReadArrayGeneric<'TItem> (decoder:Decoder) itemReader =
        
        let rec readBlock (array:'TItem array) startIdx blockSize =
            for i in 0 .. blockSize - 1 do
                array.[startIdx + i] <- itemReader()

            match decoder.ReadArrayNext() |> int with
            | 0 -> array
            | nextBlockSize ->
                let arrayReference = ref array
                Array.Resize(arrayReference, array.Length + nextBlockSize)
                readBlock arrayReference.Value array.Length nextBlockSize

        match decoder.ReadArrayStart() |> int with
        | 0 -> [||]
        | firstBlockSize -> readBlock (Array.zeroCreate firstBlockSize) 0 firstBlockSize

    override this.ReadArray(_:obj, writerSchema:ArraySchema, readerSchema:Schema, decoder:Decoder) =
        let rs = readerSchema :?> ArraySchema

        let itemReader () = 
            this.Read(null, writerSchema.ItemSchema, rs.ItemSchema, decoder)

        let array = this.ReadArrayGeneric decoder itemReader :> obj
                
        // cast result to target type if it is root schema
        if readerSchema = rootReaderSchema then reflector.ReadCast (typeof<'T>) array else array


    override this.ReadMap(_:obj, writerSchema:MapSchema, readerSchema:Schema, decoder:Decoder) =
        let rs = readerSchema :?> MapSchema
        
        let itemReader () = 
            let key = decoder.ReadString()
            let value = this.Read(null, writerSchema.ValueSchema, rs.ValueSchema, decoder)
            KeyValuePair(key, value)

        let array = this.ReadArrayGeneric decoder itemReader :> obj

        // cast result to target type if it is root schema
        if readerSchema = rootReaderSchema then reflector.ReadCast (typeof<'T>) array else array

    override _.ReadLogical(reuse:obj, writerSchema:LogicalSchema, readerSchema:Schema, d:Decoder) =
        let ls = readerSchema :?> LogicalSchema
        let value = base.Read(reuse, writerSchema.BaseSchema, ls.BaseSchema, d)
        match ls.LogicalTypeName with
        | "decimal" -> 
            let scale = ls.GetProperty("scale") |> System.Int32.Parse
            let bytesValue = value :?> byte[]
            Array.Reverse(bytesValue)
            let intValue = BigInteger(bytesValue)
            let quotient, remainder = BigInteger.DivRem (intValue, BigInteger.Pow(10|>BigInteger, scale))
            let decimalValue = (decimal quotient) + ((decimal remainder) / (decimal (Math.Pow(10., float scale))))
            decimalValue :> obj
        | _ -> writerSchema.LogicalType.ConvertToLogicalValue(value, ls)


    override _.ReadFixed(reuse:obj, writerSchema:FixedSchema, readerSchema:Schema, decoder:Decoder) =
        let bytes = (base.ReadFixed(obj, writerSchema, readerSchema, decoder) :?> GenericFixed).Value :> obj

        // cast result to target type if it is root schema
        if readerSchema = rootReaderSchema then (reflector.ReadCast (typeof<'T>) bytes) else bytes

    override _.ReadUnion(reuse:obj, writerSchema:UnionSchema, readerSchema:Schema, d:Decoder) =
        let u = base.ReadUnion(reuse, writerSchema, readerSchema, d)

        // cast result to target type if it is root schema
        if readerSchema = rootReaderSchema then (reflector.ReadCast (typeof<'T>) u) else u
