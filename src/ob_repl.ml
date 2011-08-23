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

module Timing =
struct
  let timing = ref false

  (* counters used for .timing *)
  let cnt_keys = ref 0L
  let cnt_cols = ref 0L
  let cnt_put_keys = ref 0L
  let cnt_put_cols = ref 0L
  (* Refs used here for .timing because need to rewrite these during
   * transactions *)
  let t0 = ref 0.
  let syst0 = ref 0.
  let aborted = ref false
  let cnt_ign_read = ref false

  let toggle () = timing := not !timing
  let enabled () = !timing

  let set_time_ref () =
    aborted := false;
    cnt_ign_read := false;
    t0 := Unix.gettimeofday ();
    syst0 := Sys.time ()

  let get_time_delta () =
    if !aborted then None
    else Some (Unix.gettimeofday () -. !t0, Sys.time () -. !syst0)

  let ignore_read_count () =
    cnt_ign_read := true

  let is_read_count_ignored () = !cnt_ign_read

  let abort_timing () = aborted := true
end

let puts fmt = printf (fmt ^^ "\n%!")

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


module Directives =
struct
  let directive_tbl = Hashtbl.create 13
  let cmd_desc_tbl = Hashtbl.create 13

  let help_message = function
      `Ignore_args (cmd, _, h) -> sprintf ".%-24s  %s" cmd h
    | `Exactly (cmd, _, _, (args, h))
    | `At_least (cmd, _, _, (args, h)) ->
        sprintf ".%-24s  %s" (sprintf "%s %s" cmd args) h

  let show_directive_help d =
    let cmd = match d with
        `Ignore_args (s, _, _) | `Exactly (s, _, _, _)
      | `At_least (s, _, _, _) -> s
    in
      puts "usage:";
      puts "%s %s" cmd (help_message d)

  let eval_directive cmd args =
    try
      match Hashtbl.find directive_tbl cmd with
          `Ignore_args (_, f, _) -> f ()
        | `Exactly (_, f, n, msg) as x ->
            if List.length args <> n then
              show_directive_help x
            else
              f args
        | `At_least (_, f, n, msg) as x ->
            if List.length args < n then
              show_directive_help x
            else
              f args
    with Not_found -> puts "Unknown directive"

  let ign_args ~name ~help f =
    Hashtbl.replace directive_tbl name (`Ignore_args (name, f, help))

  let exactly n ~name ~help f =
    Hashtbl.replace directive_tbl name (`Exactly (name, f, n, help))

  let at_least n ~name ~help f =
    Hashtbl.replace directive_tbl name (`At_least (name, f, n, help))

  let show_help = function
      [] ->
        let l = Hashtbl.fold
                  (fun _ cmd l -> help_message cmd :: l)
                  directive_tbl []
        in List.iter (puts "%s") (List.sort compare l)
    | cmd :: _ ->
        try
          puts (Hashtbl.find cmd_desc_tbl cmd)
        with Not_found -> puts "Unknown command %s" cmd

  let () = at_least 0
             ~name:"help"
             ~help:("CMD", "Show this message or help about CMD") show_help

  let () =
    ign_args ~name:"timing" ~help:"Toggle timing of commands"
      (fun () ->
         Timing.toggle ();
         puts "Timing is %s" (if Timing.enabled () then "on" else "off"))
end

let execute ks db loop =
  let ret f v = return (fun () -> f v) in
  let ret_nothing () = return (fun () -> ()) in

  let open Request in function

  | Register_keyspace _ | Get_keyspace _
  | Load _| Dump _ | Get_column _ | Get_column_values _ | Get_columns _
  | Get_slice_values _ -> ret_nothing ()
  | List_keyspaces _ -> D.list_keyspaces db >>= ret (print_list (sprintf "%S"))
  | List_tables _ -> D.list_tables ks >>= ret (print_list (sprintf "%S"))
  | Table_size_on_disk { Table_size_on_disk.table; _ } ->
      D.table_size_on_disk ks table >>= ret (printf "%Ld\n%!")
  | Key_range_size_on_disk { Key_range_size_on_disk.table; range; _ } ->
      D.key_range_size_on_disk ks table ?first:range.first ?up_to:range.up_to >>=
      ret (printf "%Ld\n%!")
  | Begin _ ->
      begin try_lwt
        D.repeatable_read_transaction ks
          (fun ks ->
             incr tx_level;
             try_lwt
               loop ()
             with Commit_exn ->
               print_endline "Committing";
               Timing.set_time_ref ();
               Timing.ignore_read_count ();
               return ()
             finally
               decr tx_level;
               return ())
      with Abort_exn -> print_endline "Aborting";
                        Timing.abort_timing ();
                        return ()
      end >>=
      ret_nothing
  | Abort _ -> raise_lwt Abort_exn
  | Commit _ -> raise_lwt Commit_exn
  | Get_keys { Get_keys.table; max_keys; key_range; _ } ->
      lwt keys = D.get_keys ks table ?max_keys key_range in
        Timing.cnt_keys := Int64.(add !Timing.cnt_keys (of_int (List.length keys)));
        ret (print_list (sprintf "%S")) keys
  | Count_keys { Count_keys.table; key_range; } ->
      D.count_keys ks table key_range >>= ret (printf "%Ld\n%!")
  | Get_slice { Get_slice.table; max_keys; max_columns; key_range; column_range; } ->
      lwt slice =
        D.get_slice ks table ?max_keys ?max_columns key_range column_range in
      let nkeys = List.length (snd slice) in
      let ncols =
        List.fold_left (fun s kd -> s + List.length kd.columns) 0 (snd slice)
      in
        Timing.cnt_keys := Int64.(add !Timing.cnt_keys (of_int nkeys));
        Timing.cnt_cols := Int64.(add !Timing.cnt_cols (of_int ncols));
        ret pprint_slice slice
  | Put_columns { Put_columns.table; key; columns } ->
      Timing.cnt_put_keys := Int64.add !Timing.cnt_put_keys 1L;
      Timing.cnt_put_cols :=
        Int64.(add !Timing.cnt_put_cols (of_int (List.length columns)));
      D.put_columns ks table key columns >>=
      ret_nothing
  | Delete_columns { Delete_columns.table; key; columns; } ->
      Timing.cnt_put_keys := Int64.add !Timing.cnt_put_keys 1L;
      Timing.cnt_put_cols :=
        Int64.(add !Timing.cnt_put_cols (of_int (List.length columns)));
      D.delete_columns ks table key columns >>=
      ret_nothing
  | Delete_key { Delete_key.table; key; } ->
      Timing.cnt_put_keys := Int64.add !Timing.cnt_put_keys 1L;
      D.delete_key ks table key >>=
      ret_nothing

let execute ks db loop req =
  let keys = !Timing.cnt_keys in
  let cols = !Timing.cnt_cols in
  let put_keys = !Timing.cnt_put_keys in
  let put_cols = !Timing.cnt_put_cols in
    Timing.set_time_ref ();
    lwt print = execute ks db loop req in
      match Timing.get_time_delta () with
          None -> print (); return ()
        | Some _ when not (Timing.enabled ()) -> print (); return ()
        | Some (dt, sysdt) ->
            let d_keys = Int64.(to_int (sub !Timing.cnt_keys keys)) in
            let d_cols = Int64.(to_int (sub !Timing.cnt_cols cols)) in
            let d_put_keys = Int64.(to_int (sub !Timing.cnt_put_keys put_keys)) in
            let d_put_cols = Int64.(to_int (sub !Timing.cnt_put_cols put_cols)) in
            let if_not_zero ?(cond = true) fmt = function
                0 -> []
              | n when not cond -> []
              | n -> [sprintf fmt n] in
            let if_read_cnt_not_zero =
              if_not_zero ~cond:(not (Timing.is_read_count_ignored ())) in
            let did_what =
              String.concat ", "
                (List.concat
                   [
                     if_read_cnt_not_zero "read %d keys" d_keys;
                     if_read_cnt_not_zero "read %d columns" d_cols;
                     if_not_zero "wrote %d keys" d_put_keys;
                     if_not_zero "wrote %d columns" d_put_cols;
                   ])
            in
               print ();
               puts "%s in %8.5fs (sys %8.5fs)" did_what dt sysdt;
               return ()

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
          | Directive (directive, args) ->
              Directives.eval_directive directive args;
              return ()
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
