open Lwt
open Printf
open Ob_util

module D = Protocol_client.Make(Protocol_payload.Version_0_0_0)
module Option = BatOption

let keyspace = ref ""
let tables = ref []
let server = ref "127.0.0.1"
let port = ref 12050
let output = ref "-"

let usage_message = "Usage: ob_dump -keyspace NAME [options]"

let params =
  Arg.align
    [
      "-keyspace", Arg.Set_string keyspace, "NAME Dump tables in keyspace NAME.";
      "-table", Arg.String (fun s -> tables := s :: !tables),
        "NAME Dump table NAME (default: all)";
      "-o", Arg.Set_string output, "FILE Dump to file FILE (default: '-')";
      "-server", Arg.Set_string server, "ADDR Connect to server at ADDR.";
      "-port", Arg.Set_int port, "N Connect to server port N (default: 12050)";
    ]

let dump db ~keyspace ~only_tables och =
  match_lwt D.get_keyspace db keyspace with
      None -> return ()
    | Some ks ->
        D.repeatable_read_transaction ks
          (fun tx ->
             let rec loop_dump offset progress =
               Progress_report.update progress (Lwt_io.position och) >>
               match_lwt D.dump tx ?only_tables ?offset () with
                   None -> return ()
                 | Some (data, cursor) ->
                     Lwt_io.LE.write_int och (String.length data) >>
                     Lwt_io.write och data >>
                     match cursor with
                       | Some _ -> loop_dump cursor progress
                       | None -> return ()
             in Progress_report.with_progress_report Lwt_io.stderr
                    (loop_dump None))

let () =
  Printexc.record_backtrace true;
  Arg.parse params ignore usage_message;
  if !keyspace = "" then begin
    Arg.usage params usage_message;
    exit 1
  end;
  let only_tables = match List.rev !tables with
      [] -> None
    | l -> Some l
  in Lwt_unix.run begin
    lwt output = match !output with
        "-" -> return Lwt_io.stdout
      | f -> Lwt_io.open_file
               ~flags:[ Unix.O_CREAT; Unix.O_WRONLY; Unix.O_TRUNC; ]
               ~perm:0o600 ~mode:Lwt_io.output f in
    let addr = Unix.ADDR_INET (Unix.inet_addr_of_string !server, !port) in
    lwt ich, och = Lwt_io.open_connection addr in
    let db = D.make ich och in
      try_lwt
        dump db ~keyspace:!keyspace ?only_tables output
      finally
        Lwt_io.close output
  end
