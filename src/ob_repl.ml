open Lwt
open Printf
open Request
open Repl_common
open Data_model

module Option = BatOption

exception Commit_exn
exception Abort_exn


module D = Protocol_client.Make(Protocol_payload.Version_0_0_0)

let keyspace = ref ""
let server = ref "127.0.0.1"
let port = ref 12050

let usage_message = "Usage: ob_repl -keyspace NAME [options]"

let params =
  Arg.align
    [
      "-keyspace", Arg.Set_string keyspace, "NAME Operate in keyspace NAME.";
      "-server", Arg.Set_string server, "ADDR Connect to server at ADDR.";
      "-port", Arg.Set_int port, "N Connect to server port N (default: 12050)";
    ]

let tx_level = ref 0

let print_prompt () =
  let ks = match !curr_keyspace with
      None -> "[]"
    | Some (name, _) -> sprintf "%S" name
  in
    printf "%s (%d) => %!" ks !tx_level

let print_list f l =
  List.iter (fun x -> print_endline (f x)) l

let pp_data fmt = Format.fprintf fmt "%S"

let rec pp_cols fmt = function
    [] -> ()
  | [ { name; data } ] -> Format.fprintf fmt "%a: %a" pp_data name pp_data data
  | { name; data } :: tl ->
      Format.fprintf fmt "%a: %a@ " pp_data name pp_data data;
      pp_cols fmt tl

let pprint_slice (last_key, key_data) =
  List.iter
    (fun kd -> Format.printf " %S: @[%a@]@." kd.key pp_cols kd.columns)
    key_data;
  Format.printf "Last_key: %s@."
    (Option.map_default (sprintf "%S") "<none>" last_key)

let execute ks db loop = let open Request in function
  | Register_keyspace _ | Get_keyspace _
  | Load _| Dump _ | Get_column _ | Get_column_values _ | Get_columns _
  | Get_slice_values _ -> return ()
  | List_keyspaces _ -> D.list_keyspaces db >|= print_list (sprintf "%S")
  | List_tables _ -> D.list_tables ks >|= print_list (sprintf "%S")
  | Table_size_on_disk { Table_size_on_disk.table; _ } ->
      D.table_size_on_disk ks table >|= printf "%Ld\n%!"
  | Key_range_size_on_disk { Key_range_size_on_disk.table; range; _ } ->
      D.key_range_size_on_disk ks table ?first:range.first ?up_to:range.up_to >|=
      printf "%Ld\n%!"
  | Begin _ ->
      begin try_lwt
        D.repeatable_read_transaction ks
          (fun ks ->
             incr tx_level;
             try_lwt
               loop ()
             with Commit_exn ->
               print_endline "Committing";
               return ()
             finally
               decr tx_level;
               return ())
      with Abort_exn -> print_endline "Aborting"; return () end
  | Abort _ -> raise_lwt Abort_exn
  | Commit _ -> raise_lwt Commit_exn
  | Get_keys { Get_keys.table; max_keys; key_range; _ } ->
      D.get_keys ks table ?max_keys key_range >|=
      print_list (sprintf "%S")
  | Count_keys { Count_keys.table; key_range; } ->
      D.count_keys ks table key_range >|= printf "%Ld\n%!"
  | Get_slice { Get_slice.table; max_keys; max_columns; key_range; column_range; } ->
      D.get_slice ks table ?max_keys ?max_columns key_range column_range >|=
      pprint_slice
  | Put_columns { Put_columns.table; key; columns } ->
      D.put_columns ks table key columns
  | Delete_columns { Delete_columns.table; key; columns; } ->
      D.delete_columns ks table key columns
  | Delete_key { Delete_key.table; key; } ->
      D.delete_key ks table key

let () =
  Printexc.record_backtrace true;
  Arg.parse params ignore usage_message;
  if !keyspace = "" then begin
    Arg.usage params usage_message;
    exit 1
  end;
  Lwt_unix.run begin
    let addr = Unix.ADDR_INET (Unix.inet_addr_of_string !server, !port) in
    lwt ich, och = Lwt_io.open_connection addr in
    let db = D.make ich och in
    let lexbuf = Lexing.from_channel stdin in
    lwt ks = D.register_keyspace db !keyspace in
    let rec loop () =
      begin try_lwt
        print_prompt ();
        match Repl_gram.input Repl_lex.token lexbuf with
            Command req ->
              execute ks db loop req
          | Directive () -> print_endline "directive"; return ()
          | Nothing -> print_endline "Nothing"; return ()
          | Error s -> printf "Error: %s\n%!" s; return ()
      with
        | Parsing.Parse_error ->
            print_endline "Parse error";
            Lexing.flush_input lexbuf;
            return ()
        | End_of_file | Abort_exn | Commit_exn as e -> raise_lwt e
        | e ->
            printf "Got exception: %s\n%!" (Printexc.to_string e);
            return ()
      end >>
      loop () in
    let rec outer_loop () =
      try_lwt
        loop ()
      with
          End_of_file -> exit 0
        | Abort_exn | Commit_exn ->
            print_endline "Not inside a transaction";
            outer_loop ()
    in
      curr_keyspace := Some (D.keyspace_name ks, D.keyspace_id ks);
      outer_loop ()
  end
