open System
open System.Diagnostics
open System.IO


let exec cmd args workDir =

    let cmdTxt = $"EXEC {cmd} {args} from {workDir}"
    Console.WriteLine cmdTxt

    let startInfo = ProcessStartInfo(cmd, args)
    startInfo.WorkingDirectory <- workDir
    startInfo.UseShellExecute <- false
    startInfo.CreateNoWindow <- true

    use proc = new Process()
    proc.StartInfo <- startInfo

    proc.Start() |> ignore
    proc.WaitForExit()

    match proc.ExitCode with
    | 0 -> Ok()
    | x -> Error($"{cmdTxt} exit with code {x}")

let dotnet = exec "dotnet"

let pack () =
    dotnet "build -c Release" "."
    |> Result.bind (fun _ -> dotnet "pack -c Release" ".")


let resolveTarget () =
    if fsi.CommandLineArgs.Length > 1 then
        fsi.CommandLineArgs.[1].ToUpperInvariant()
    else
        "BUILD"


match resolveTarget () with
| "CLEAN" -> dotnet "clean" "."
| "BUILD" -> dotnet "build" "."
| "TEST" -> dotnet "test" "."
| "PACK" -> pack ()
| _ -> Error "Usage: dotnet fsi make.fsx [CLEAN|BUILD|TEST|PACK]"
|> function
    | Ok() -> printfn "Done"
    | Error err -> printfn "Error: %s" err
