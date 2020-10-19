module Examples.KafkaSerde.Domain

type Cmd = 
   | Text of string
   | Batch of Cmd list

type Msg = {
    Id: int
    Cmd: Cmd
}