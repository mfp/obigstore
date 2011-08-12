open Printf
open Lwt

module S = Server.Make(Storage)(Protocol_payload.Version_0_0_0)

let port = ref 12050
let db_dir = ref None

let params =
  Arg.align
    [
      "-port", Arg.Set_int port, "PORT Port to listen at (default: 12050)";
    ]

let usage_message = "Usage: obigstore [options] [database dir]"

let _ = Sys.set_signal Sys.sigpipe Sys.Signal_ignore
let _ = Sys.set_signal Sys.sigint (Sys.Signal_handle (fun _ -> exit 0))

let () =
  Arg.parse
    params
    (function
       | s when !db_dir = None && s <> "" & s.[0] <> '-' -> db_dir := Some s
       | s -> eprintf "Unknown argument: %S\n%!" s;
              Arg.usage params usage_message;
              exit 1)
    usage_message;
  let addr = Unix.ADDR_INET (Unix.inet_addr_any, !port) in
    match !db_dir with
        None -> Arg.usage params usage_message;
                exit 1
      | Some dir ->
          let db = Storage.open_db dir in
            Lwt_unix.run (S.run_server db addr !port)
