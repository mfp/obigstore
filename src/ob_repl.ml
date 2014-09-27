(*
 * Copyright (C) 2011-2012 Mauricio Fernandez <mfp@acm.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version,
 * with the special exception on linking described in file LICENSE.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *)

open Lwt
open Printf
open Obs_protocol
open Obs_request
open Obs_repl_common
open Obs_data_model

module Option = BatOption
module Array = struct include BatArray include Array end

exception Commit_exn
exception Abort_exn
exception Reload_keyspace of string

exception Need_reconnect of string option

let keyspace = ref ""
let server = ref "localhost"
let port = ref "12050"
let simple_repl = ref false
let dir = ref ""

let usage_message = "Usage: ob_repl [options]"

let params =
  Arg.align
    [
      "-keyspace", Arg.Set_string keyspace, "NAME Operate in keyspace NAME.";
      "-server", Arg.Set_string server, "ADDR Connect to server at ADDR (default: localhost)";
      "-port", Arg.Set_string port, "PORT Connect to server port or service name PORT (default: 12050)";
      "-dir", Arg.Set_string dir, "DIR Use DB at DIR.";
      "-simple", Arg.Set simple_repl, " Simple mode for use with rlwrap and similar.";
    ]

let tx_level = ref 0
let debug_requests = ref false

let install_key_codec table print parse =
  Hashtbl.replace key_codecs table (print, parse)

module Timing =
struct
  let timing = ref true

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

let prompt () =
  let ks = match !curr_keyspace with
      None -> "[]"
    | Some (name, _) -> sprintf "%S" name
  in
    [ ks; sprintf " (%d) > " !tx_level ]

let print_list f l =
  List.iter (fun x -> print_endline (f x)) l

let pp_col ~strict fmt = function
    Binary s | Malformed_BSON s -> Obs_pp.pp_datum ~strict fmt s
  | BSON x -> Obs_bson.pp_bson ~strict fmt x

let pp_cols ~strict fmt =
  Obs_pp.pp_list
    (fun fmt {name; data; _} ->
       Format.fprintf fmt "%a: %a"
         (Obs_pp.pp_key ~strict) name (pp_col ~strict) data)
    fmt

let rec pp_lines f fmt = function
    [] -> ()
  | x :: tl ->
      Format.fprintf fmt "%a@." f x;
      pp_lines f fmt tl

let pprint_slice ~strict ?(pp_key = Obs_pp.pp_key) fmt (last_key, key_data) =
  if fmt == Format.std_formatter then
    Format.printf "%s@." (String.make 78 '-');
  Format.fprintf fmt "{@\n";
  Obs_pp.pp_list ~delim:(format_of_string ",@\n@.")
    (fun fmt kd ->
       Format.fprintf fmt " @[<2>%a:@\n{@[<1> %a }@]@]"
         (pp_key ~strict) kd.key (pp_cols ~strict) kd.columns)
    fmt key_data;
  Format.fprintf fmt "@.}@.";
  if fmt == Format.std_formatter then
    Format.printf "%s@." (String.make 78 '-');
  Format.printf "Last key: %a@."
    (fun fmt k -> match k with
         None -> Format.printf "<none>"
       | Some k -> pp_key ~strict:false fmt k)
    last_key

let pretty_printer_of_codec c =
  let module C = (val c : Obs_key_encoding.CODEC_OPS) in
    (fun ~strict fmt k ->
       try
         if not strict then
           Format.fprintf fmt "%s" (C.pp (C.decode_string k))
         else begin
           (* we want semi-strict JSON (module comments), so we output the
            * decoded version as a comment *)
           Format.fprintf fmt "@[<0>/* %s */@ %a@]"
             (C.pp (C.decode_string k))
             (Obs_pp.pp_key ~strict:true) k
         end
       with _ -> Obs_pp.pp_key ~strict:true fmt k)

module Directives =
struct
  let directive_tbl = Hashtbl.create 13
  let cmd_desc_tbl = Hashtbl.create 13

  let quick_ref =
    "\
     KEYSPACES                  List keyspaces\n\
     TABLES                     List tables in present keyspace\n\
     STATS                      Show load statistics\n\
     \n\
     SIZE                       List all tables and their approx sizes\n\
     SIZE table                 Show approx size of table\n\
     SIZE table[x:y]            Show approx size of table between keys x and y\n\
     COMPACT                    Compact whole keyspace\n\
     COMPACT table              Compact whole table\n\
     COMPACT table[x:y]         Compact table range\n\
     \n\
     BEGIN                      Start a transaction block\n\
     COMMIT                     Commit current transaction\n\
     ABORT                      Abort current transaction\n\
     LOCK name1 name2...        Acquire exclusive locks with given names\n\
     LOCK SHARED name1 name2... Acquire shared locks with given names\n\
     WATCH table key1, key2     Watch keys key1 and key2 in table\n\
     WATCH table key c1, c2     Watch columns c1 and c2 from key in table\n\
\n\
     LISTEN topic               Subscribe to topic.\n\
     UNLISTEN topic             Unsubscribe to topic.\n\
     NOTIFY topic               Send async notification to topic.\n\
     AWAIT                      Wait for a notification.\n\
\n\
     COUNT table                Count keys in table\n\
     COUNT table[x:y]           Count keys in table between keys x and y\n\
\n\
     GET KEYS table             Get all keys in table.\n\
     GET KEYS table[/10]        Get up to 10 keys from table\n\
     GET KEYS table[~/10]       Get up to 10 keys from table, in reverse order\n\
     GET KEYS table[k1:k2/10]   Get up to 10 keys from table between k1 and k2\n\
     GET KEYS table[k2~k1/10]   Same as above in reverse order\n\
     GET KEYS table[/100] TO tablekeys.txt
                           Write first (up to) 100 keys to 'tablekeys.txt'.\n\
\n\
     GET table[key]             Get all columns for key in table\n\
     GET table[/100][c1, c2] TO foo.json
                           Output the values of the c1 and c2 columns for
                           up to 100 keys to 'foo.json'.\n\
     GET table[key][c1,c2]      Get columns c1 and c2 from key in table\n\
     GET table[k1:k2/10]        Get all columns for up to 10 keys between k1 and k2\n\
     GET table[k2~k1/10]        Similar to above in reverse order\n\
     GET table[k1:k2/10][c1,c2,c5:c9/4]
                           Get up to 4 columns taken from c1, c2, and the
                           range from c5 to c9, for up to 10 keys
                           between k1 and k2\n\
     GET table[k1:k2/10][a,b] / x = \"foo\" && \"bar1\" < y <= \"bar9\"
                           Get columns a and b for up to 10 keys between k1
                           and k2, whose x column has value 'foo' and
                           whose y column is comprised between 'bar1' and
                           'bar9'.\n\
     \n\
     PUT table[key][c1:\"v1\", c2:\"v2\"]
                           Put columns c1 and c2 into given key in table\n\
     PUT table[key][\"@c1\": { int: 1, int64: 42L, float: 3.14, bool: true, data: \"foo\"}]
                           Put structured column @c1 into given key in table\n\
\n\
     DELETE table[key][col]     Delete column col for given key in table\n\
     DELETE table[key]          Delete all columns for given key in table\n\
     DELETE table[k1:k2]        Delete all columns for given key range in table\n\
\n\
     DUMP                       Trigger server-side database dump.\n\
     DUMP LOCAL                 Dump database in new (timestamped) directory.\n\
     DUMP LOCAL TO dirname      Dump database to directory 'dirname'.\n\
\n\
     DEBUG txs                  List ongoing transactions.\n\
     DEBUG tx 42                List tables modified by ongoing transaction 42.\n\
"

  let help_message = function
      `Ignore_args (cmd, _, h) -> sprintf ".%-24s  %s" (cmd ^ ";") h
    | `Exactly (cmd, _, _, (args, h))
    | `At_least (cmd, _, _, (args, h)) ->
        sprintf ".%-24s  %s" (sprintf "%s %s;" cmd args) h
    | `One_of (cmd, _, l, (args, h)) ->
        sprintf ".%-24s  %s\n%25s  Valid values for %s: %s"
          (sprintf "%s %s;" cmd args) h "" args (String.concat ", " l)

  let show_directive_help d =
    puts "usage:";
    print_endline (help_message d)

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
        | `One_of (_, f, l, _) as x ->
            if List.length args <> 1 ||
               not (List.mem (List.hd args) l) then
              show_directive_help x
            else
              f (List.hd args)
    with Not_found -> puts "Unknown directive"

  let ign_args ~name ~help f =
    Hashtbl.replace directive_tbl name (`Ignore_args (name, f, help))

  let exactly n ~name ~help f =
    Hashtbl.replace directive_tbl name (`Exactly (name, f, n, help))

  let at_least n ~name ~help f =
    Hashtbl.replace directive_tbl name (`At_least (name, f, n, help))

  let one_of l ~name ~help f =
    Hashtbl.replace directive_tbl name (`One_of (name, f, l, help))

  let show_help = function
      [] ->
        let l = Hashtbl.fold
                  (fun _ cmd l -> help_message cmd :: l)
                  directive_tbl []
        in
          puts "Directives:";
          List.iter print_endline (List.sort compare l);
          puts "\nQuick command reference:\n%s" quick_ref
    | cmd :: _ ->
        try
          puts (Hashtbl.find cmd_desc_tbl cmd)
        with Not_found -> puts "No help available for command %s" cmd

  let () = at_least 0
             ~name:"help"
             ~help:("CMD", "Show this message or help about CMD") show_help

  let bool_to_s = function true -> "on" | _ -> "off"

  let () =
    ign_args ~name:"timing" ~help:"Toggle timing of commands"
      (fun () ->
         Timing.toggle ();
         puts "Timing is %s" (bool_to_s (Timing.enabled ())))

  let () =
    one_of ["request"] ~name:"debug"
      ~help:("WHAT", "Toggle debugging of WHAT.")
      (function
         | "request" ->
             debug_requests := not !debug_requests;
             puts "Request debugging is %s" (bool_to_s !debug_requests)
         | _ -> ())

  let () = exactly 1 ~name:"read"
             ~help:("FILE", "Execute commands in FILE.")
             (fun _ ->
                (* .read is handled directly by the exec loop *)
                ())

  let () = exactly 1 ~name:"keyspace"
             ~help:("KEYSPACE", "Switch to KEYSPACE.")
             (function
                | [ks] -> raise (Reload_keyspace ks)
                | _ -> (* should not happen *)())

  let list =
    Hashtbl.fold (fun name _ l -> name :: l) directive_tbl []
end

module Make(D : Obs_data_model.S) =
struct
  exception Keyspace_not_set

  let get = function
      None -> raise Keyspace_not_set
    | Some ks -> ks

  let execute ?(fmt=Format.std_formatter) ks db loop r =
    let open Request in
    let ret f v = return (fun () -> f v) in
    let ret_nothing () = return (fun () -> ()) in
    (* if not writing to stdout, we use "strict" JSON with everything quoted *)
    let strict = fmt != Format.std_formatter in

    if !debug_requests then Format.printf "Request:@\n%a@." Request.pp r;

    match r with

    | Register_keyspace _ | Get_keyspace _
    | Load _| Dump _ | Get_column _ | Get_column_values _ | Get_columns _
    | Get_slice_values _ | Get_slice_values_timestamps _ | Exist_keys _
    | Raw_dump_file_digest _ | Raw_dump_list_files _
    | Raw_dump_release _ | Release_keyspace _ | Watch_prefixes _ -> ret_nothing ()
    | Trigger_raw_dump _ ->
        D.Raw_dump.dump db >>=
        ret (fun _ -> printf "Raw dump saved\n%!")
    | List_keyspaces _ -> D.list_keyspaces db >>= ret (print_list (sprintf "%S"))
    | List_tables _ ->
        (D.list_tables (get ks) :> string list Lwt.t) >>= ret (print_list (sprintf "%S"))
    | Table_size_on_disk { Table_size_on_disk.table; _ } ->
        D.table_size_on_disk (get ks) table >>= ret (printf "%Ld\n%!")
    | Key_range_size_on_disk { Key_range_size_on_disk.table; range; _ } ->
        D.key_range_size_on_disk (get ks) table ?first:range.first ?up_to:range.up_to >>=
        ret (printf "%Ld\n%!")
    | Begin _ ->
        begin try_lwt
          D.repeatable_read_transaction (get ks)
            (fun ks ->
               incr tx_level;
               try_lwt
                 loop (Some ks)
               with Commit_exn ->
                 print_endline "Committing";
                 Timing.set_time_ref ();
                 Timing.ignore_read_count ();
                 return_unit
               finally
                 decr tx_level;
                 return_unit)
        with
          | Abort_exn ->
              print_endline "Aborting";
              Timing.abort_timing ();
              return_unit
          | Deadlock ->
              print_endline "Deadlock detected, aborting";
              Timing.abort_timing ();
              return_unit
        end >>=
        ret_nothing
    | Abort _ -> raise_lwt Abort_exn
    | Commit _ -> raise_lwt Commit_exn
    | Lock { Lock.names; shared; _ } -> D.lock (get ks) ~shared names >>= ret_nothing
    | Get_transaction_id
        { Get_transaction_id.keyspace } -> begin
        match_lwt D.transaction_id (get ks) with
            None -> print_endline "Not within a transaction."; ret_nothing ()
          | Some (cur, outer) ->
              printf "Current transaction: %d   outermost: %d\n%!" cur outer;
              ret_nothing ()
        end
    | Watch_keys { Watch_keys.table; keys; } -> D.watch_keys (get ks) table keys >>= ret_nothing
    | Watch_columns { Watch_columns.table; columns; } ->
        D.watch_columns (get ks) table columns >>= ret_nothing
    | Get_keys { Get_keys.table; max_keys; key_range; _ } ->
        let pp_keys =
          try (fst (Hashtbl.find Obs_repl_common.key_codecs table))
          with Not_found -> Obs_pp.pp_datum
        in
          lwt keys = D.get_keys (get ks) table ?max_keys (krange' key_range) in
            Timing.cnt_keys := Int64.(add !Timing.cnt_keys (of_int (List.length keys)));
            ret (pp_lines (pp_keys ~strict) fmt) keys
    | Count_keys { Count_keys.table; key_range; } ->
        D.count_keys (get ks) table (krange' key_range) >>= ret (printf "%Ld\n%!")
    | Get_slice { Get_slice.table; max_keys; max_columns; key_range; column_range;
                  predicate; } ->
        lwt slice =
          D.get_bson_slice (get ks) table ?max_keys ?max_columns (krange' key_range)
            ?predicate (crange' column_range) in
        let nkeys = List.length (snd slice) in
        let ncols =
          List.fold_left (fun s kd -> s + List.length kd.columns) 0 (snd slice)
        in
          Timing.cnt_keys := Int64.(add !Timing.cnt_keys (of_int nkeys));
          Timing.cnt_cols := Int64.(add !Timing.cnt_cols (of_int ncols));
          let pp_key =
            try
              Some (fst (Hashtbl.find Obs_repl_common.key_codecs table))
            with Not_found -> None
          in
            ret (pprint_slice ~strict ?pp_key fmt) slice
    | Put_columns { Put_columns.table; data } ->
        let keys = List.length data in
        let columns = List.fold_left (fun s (_, l) -> List.length l + s) 0 data in
          Timing.cnt_put_keys := Int64.add !Timing.cnt_put_keys (Int64.of_int keys);
          Timing.cnt_put_cols := Int64.(add !Timing.cnt_put_cols (of_int columns));
          D.put_multi_columns (get ks) table data >>=
          ret_nothing
    | Delete_columns { Delete_columns.table; key; columns; } ->
        Timing.cnt_put_keys := Int64.add !Timing.cnt_put_keys 1L;
        Timing.cnt_put_cols :=
          Int64.(add !Timing.cnt_put_cols (of_int (List.length columns)));
        D.delete_columns (get ks) table key columns >>=
        ret_nothing
    | Delete_key { Delete_key.table; key; } ->
        Timing.cnt_put_keys := Int64.add !Timing.cnt_put_keys 1L;
        D.delete_key (get ks) table key >>=
        ret_nothing
    | Delete_keys { Delete_keys.table; key_range; } ->
        D.delete_keys (get ks) table (krange' key_range) >>=
        ret_nothing
    | Stats _ ->
        let open Load_stats_ in
        let open Rates in
        lwt stats = D.load_stats (get ks) in
        let pr_avg (period, r) =
          puts "";
          puts "%d-second average rates" period;
          puts "%.0f transactions" r.transactions;
          puts "%.0f writes, %.0f reads" r.writes r.reads;
          puts "%.0f columns written, %.0f columns read" r.cols_wr r.cols_rd;
          puts "%s written, %s read"
            (Obs_util.format_speed 0. 1. (Int64.of_float r.bytes_wr))
            (Obs_util.format_speed 0. 1. (Int64.of_float r.bytes_rd));
          puts "%.0f seeks, %.0f near seeks" r.seeks r.near_seeks; in
        let dt = stats.uptime in
          puts "Totals:";
          puts "Uptime: %.1fs" dt;
          puts "%Ld transactions (%.0f/s)"
            stats.total_transactions (Int64.to_float stats.total_transactions /. dt);
          puts "%Ld writes (%.0f/s), %Ld reads (%.0f/s)"
            stats.total_writes (Int64.to_float stats.total_writes /. dt)
            stats.total_reads (Int64.to_float stats.total_reads /. dt);
          puts "%Ld columns written (%.0f/s), %Ld columns read (%.0f/s)"
            stats.total_cols_wr (Int64.to_float stats.total_cols_wr /. dt)
            stats.total_cols_rd (Int64.to_float stats.total_cols_rd /. dt);
          puts "%s written (%s/s), %s read (%s/s)"
            (Obs_util.format_size 1.0 stats.total_bytes_wr)
            (Obs_util.format_size (1.0 /. dt) stats.total_bytes_wr)
            (Obs_util.format_size 1.0 stats.total_bytes_rd)
            (Obs_util.format_size (1.0 /. dt) stats.total_bytes_rd);
          puts "%Ld seeks (%.0f/s), %Ld near seeks (%.0f/s)"
            stats.total_seeks
            (Int64.to_float stats.total_seeks /. dt)
            stats.total_near_seeks
            (Int64.to_float stats.total_near_seeks /. dt);
          List.iter pr_avg stats.averages;
          ret_nothing ()
      | Listen { Listen.topic } -> D.listen (get ks) topic >>= ret_nothing
      | Unlisten { Unlisten.topic } -> D.unlisten (get ks) topic >>= ret_nothing
      | Listen_prefix { Listen_prefix.topic } -> D.listen_prefix (get ks) topic >>= ret_nothing
      | Unlisten_prefix { Unlisten_prefix.topic } -> D.unlisten_prefix (get ks) topic >>= ret_nothing
      | Notify { Notify.topic } -> D.notify (get ks) topic >>= ret_nothing
      | Await _ ->
          lwt l = D.await_notifications (get ks) in
            ret (print_list (sprintf "%S")) l
      | Get_property { Get_property.property } ->
          begin match_lwt D.get_property db property with
              None -> print_endline "Unknown property"; ret_nothing ()
            | Some x -> print_endline x; ret_nothing ()
          end
      | Compact_keyspace _ ->
          D.compact (get ks) >>
          ret print_endline "OK"
      | Compact_table { Compact_table.table; from_key; to_key; _ } ->
          D.compact_table (get ks) table ?from_key ?to_key () >>
          ret print_endline "OK"
      | List_transactions _ ->
          lwt txs = D.list_transactions (get ks) in
            printf "%10s  %20s  wanted  held\n" "id" "started_at";
            List.iter
              (fun tx ->
                 let fmt_time t =
                   let tm = Unix.gmtime t in
                     sprintf "%04d-%02d-%02dT%02d:%02d:%02d"
                       (1900 + tm.tm_year) (1 + tm.tm_mon) tm.tm_mday
                       tm.tm_hour tm.tm_min tm.tm_sec in

                 let fmt_lock_list l =
                   String.concat "," @@
                   List.map
                     (fun (name, kind) -> match kind with
                        | `SHARED -> sprintf "%S" name
                        | `EXCLUSIVE -> sprintf "EXC%S" name)
                     l
                 in
                   printf "%10Ld  %20s %s  %s\n"
                     tx.tx_id (fmt_time tx.started_at)
                     (fmt_lock_list tx.wanted_locks)
                     (fmt_lock_list tx.held_locks))
              txs;
            ret print_endline ""
      | Changed_tables { Changed_tables.tx_id; _ } ->
          lwt l = D.changed_tables (get ks) tx_id in
            List.sort String.compare l |> List.iter print_endline;
            ret print_endline ""

  let execute ?(fmt = Format.std_formatter) ks db loop req =
    let keys = !Timing.cnt_keys in
    let cols = !Timing.cnt_cols in
    let put_keys = !Timing.cnt_put_keys in
    let put_cols = !Timing.cnt_put_cols in
      Timing.set_time_ref ();
      lwt print = execute ~fmt ks db loop req in
        match Timing.get_time_delta () with
            None -> print (); return_unit
          | Some _ when not (Timing.enabled ()) -> print (); return_unit
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
              let join l = String.concat ", " (List.concat l) in
              let did_what =
                join
                  [
                    if_read_cnt_not_zero "read %d keys" d_keys;
                    if_read_cnt_not_zero "read %d columns" d_cols;
                    if_not_zero "wrote %d keys" d_put_keys;
                    if_not_zero "wrote %d columns" d_put_cols;
                  ] in
              let read_col_rate = truncate (float d_cols /. dt) in
              let write_col_rate = truncate (float d_put_cols /. dt) in
              let rate_info =
                join
                  [ if_not_zero "read %d columns/second" read_col_rate;
                    if_not_zero "wrote %d columns/second" write_col_rate; ]
              in
                 print ();
                 puts "%s in %8.5fs (cpu %8.5fs)" did_what dt sysdt;
                 if rate_info <> "" then puts "%s" rate_info;
                 return_unit

  let phrase_history = ref []

  let get_phrase () =
    printf "%s%!" (String.concat "" (prompt ()));
    Lwt_io.read_line Lwt_io.stdin

  module Printer =
  struct
    open Obs_key_encoding

    let rec string_of_desc = function
        Simple_codec s -> s
      | Complex_codec (s, l) ->
          sprintf "%s(%s)" s (String.concat ", " (List.map string_of_desc l))

    let failwithfmt fmt = Printf.ksprintf failwith fmt

    module type PRIM_OPS =
    sig
      include CODEC_OPS
      val of_string : string -> key
    end

    module Bool =
    struct
      include Bool
      let of_string = function
          "true" | "t" | "1" -> true
        | "false" | "f" | "0" -> false
        | s -> failwithfmt "Invalid bool literal: %S" s
    end

    module Byte = struct include Byte let of_string = int_of_string end

    module Positive_int64 =
    struct
      include Positive_int64
      let of_string = Int64.of_string
    end

    module Positive_int64_complement =
    struct
      include Positive_int64_complement
      let of_string = Int64.of_string
    end

    module Stringz =
    struct
      include Stringz
      let of_string s = s
    end

    module Self_delimited_string =
    struct
      include Self_delimited_string
      let of_string s = s
    end

    let simple_printers =
      [ "bool", (module Bool : PRIM_OPS);
        "byte", (module Byte : PRIM_OPS);
        "pint64", (module Positive_int64 : PRIM_OPS);
        "pint64c", (module Positive_int64_complement : PRIM_OPS);
        "stringz", (module Stringz : PRIM_OPS);
        "sstring", (module Self_delimited_string : PRIM_OPS);
      ]

    let dummy = "\"" (* reset omlet's hl *)

    let first_error = List.find (function `Error _ -> true | _ -> false)

    let fst_error_or_arity_error n l =
      try
        first_error l
      with Not_found -> `Error (sprintf "Wrong arity: tuple%d expects %d arguments." n n)

    let rec build_codec = function
        Simple_codec s ->
          begin try
            `OK (module struct
                   include (val List.assoc s simple_printers : PRIM_OPS)
                 end : CODEC_OPS)
          with Not_found ->
            `Error (sprintf "Unknown codec %S" s)
          end
      | Complex_codec ("tuple2", l) -> begin
          match List.map build_codec l with
              [ `OK c1; `OK c2 ] ->
                `OK (module Tuple2
                       ((val c1 : CODEC_OPS))
                       ((val c2 : CODEC_OPS))
                       : CODEC_OPS)
            | l -> fst_error_or_arity_error 2 l
        end
      | Complex_codec ("tuple3", l) -> begin
          match List.map build_codec l with
              [ `OK c1; `OK c2; `OK c3 ] ->
                `OK (module Tuple3
                       ((val c1 : CODEC_OPS))
                       ((val c2 : CODEC_OPS))
                       ((val c3 : CODEC_OPS))
                       : CODEC_OPS)
            | l -> fst_error_or_arity_error 3 l
        end
      | Complex_codec ("tuple4", l) -> begin
          match List.map build_codec l with
              [ `OK c1; `OK c2; `OK c3; `OK c4] ->
                `OK (module Tuple4
                       ((val c1 : CODEC_OPS))
                       ((val c2 : CODEC_OPS))
                       ((val c3 : CODEC_OPS))
                       ((val c4 : CODEC_OPS))
                       : CODEC_OPS)
            | l -> fst_error_or_arity_error 4 l
        end
      | Complex_codec ("tuple5", l) -> begin
          match List.map build_codec l with
              [ `OK c1; `OK c2; `OK c3; `OK c4; `OK c5] ->
                `OK (module Tuple5
                       ((val c1 : CODEC_OPS))
                       ((val c2 : CODEC_OPS))
                       ((val c3 : CODEC_OPS))
                       ((val c4 : CODEC_OPS))
                       ((val c5 : CODEC_OPS))
                       : CODEC_OPS)
            | l -> fst_error_or_arity_error 5 l
        end
      | Complex_codec (x, _) -> `Error (sprintf "Unknown codec %S" x)

    let arity_error n = failwithfmt "Codec tuple%d expects %d arguments." n n

    let rec parse_value codec_desc v = match codec_desc, v with
        Simple_codec s, v ->
          begin try
            let module M = (val List.assoc s simple_printers : PRIM_OPS) in
            let module P =
              struct
                include M
                let v = match v with
                    Atom (Literal v) -> M.of_string v
                  | Atom Min_value -> M.min_value
                  | Atom Max_value -> M.max_value
                  | Tuple _ -> failwithfmt "Codec %S expects an atom, not a tuple" s
              end
            in (module P : PARSED_VALUE)
          with Not_found -> failwithfmt "Unknown codec %S" s
          end
      | Complex_codec ("tuple2", [ c1; c2 ]), Tuple [v1; v2] ->
          let p1 = parse_value c1 v1 in
          let p2 = parse_value c2 v2 in
          let module P =
            struct
              module P1 = (val p1 : PARSED_VALUE)
              module P2 = (val p2 : PARSED_VALUE)
              include Tuple2(P1)(P2)
              let v = (P1.v, P2.v)
            end
          in (module P : PARSED_VALUE)
      | Complex_codec ("tuple2", _), _ -> arity_error 2
      | Complex_codec ("tuple3", [ c1; c2; c3 ]), Tuple [v1; v2; v3] ->
          let p1 = parse_value c1 v1 in
          let p2 = parse_value c2 v2 in
          let p3 = parse_value c3 v3 in
          let module P =
            struct
              module P1 = (val p1 : PARSED_VALUE)
              module P2 = (val p2 : PARSED_VALUE)
              module P3 = (val p3 : PARSED_VALUE)
              include Tuple3(P1)(P2)(P3)
              let v = (P1.v, P2.v, P3.v)
            end
          in (module P : PARSED_VALUE)
      | Complex_codec ("tuple3", _), _ -> arity_error 3
      | Complex_codec ("tuple4", [ c1; c2; c3; c4 ]), Tuple [v1; v2; v3; v4] ->
          let p1 = parse_value c1 v1 in
          let p2 = parse_value c2 v2 in
          let p3 = parse_value c3 v3 in
          let p4 = parse_value c4 v4 in
          let module P =
            struct
              module P1 = (val p1 : PARSED_VALUE)
              module P2 = (val p2 : PARSED_VALUE)
              module P3 = (val p3 : PARSED_VALUE)
              module P4 = (val p4 : PARSED_VALUE)
              include Tuple4(P1)(P2)(P3)(P4)
              let v = (P1.v, P2.v, P3.v, P4.v)
            end
          in (module P : PARSED_VALUE)
      | Complex_codec ("tuple4", _), _ -> arity_error 4
      | Complex_codec ("tuple5", [ c1; c2; c3; c4; c5 ]), Tuple [v1; v2; v3; v4; v5] ->
          let p1 = parse_value c1 v1 in
          let p2 = parse_value c2 v2 in
          let p3 = parse_value c3 v3 in
          let p4 = parse_value c4 v4 in
          let p5 = parse_value c5 v5 in
          let module P =
            struct
              module P1 = (val p1 : PARSED_VALUE)
              module P2 = (val p2 : PARSED_VALUE)
              module P3 = (val p3 : PARSED_VALUE)
              module P4 = (val p4 : PARSED_VALUE)
              module P5 = (val p5 : PARSED_VALUE)
              include Tuple5(P1)(P2)(P3)(P4)(P5)
              let v = (P1.v, P2.v, P3.v, P4.v, P5.v)
            end
          in (module P : PARSED_VALUE)
      | Complex_codec ("tuple5", _), _ -> arity_error 5
      | Complex_codec (x, _), _ -> failwithfmt "Unknown codec %S" x
  end

  let save_printer ks table desc =
    match ks with
        None -> return_unit
      | Some ks ->
          D.put_columns ks (table_of_string "@meta.printers") table
            [ { name = "@printer";
                data = Printer.string_of_desc desc;
                timestamp = No_timestamp; } ]

  let rec inner_exec_loop get_phrase ?phrase db ks =
    let last_phrase = ref None in
    begin try_lwt
      lwt phrase = match phrase with
          None -> get_phrase ()
        | Some s -> return s in
      let () =
        last_phrase := Some phrase;
        (* we add to the phrase history before parsing, so that the phrase can
         * be corrected in case of syntax error *)
        phrase_history := phrase :: !phrase_history;
      in
      let loop ks = inner_exec_loop get_phrase db ks in
      let lexbuf = Lexing.from_string phrase in
        match Obs_repl_gram.input Obs_repl_lex.token lexbuf with
            Command (req, None) -> execute ks db loop req
          | Command (req, Some dst) ->
              let oc = open_out dst in
                try_lwt
                  let fmt =
                    Format.make_formatter
                      (Pervasives.output oc)
                      (fun () -> Pervasives.flush oc) in
                  lwt () = execute ~fmt ks db loop req in
                    return_unit
                finally
                  close_out oc;
                  return_unit
          | Directive ("read", [file]) -> begin
              (* we handle .read here directly, the declaration in Directives is
              * just for the help message *)
              try_lwt
                lwt ic = Lwt_io.open_file Lwt_io.input file in
                let get_phrase () =
                  lwt l = Lwt_io.read_line ic in puts "%s" l; return l in
                let loop ks = inner_exec_loop get_phrase db ks in
                  try_lwt
                    puts "Executing commands from %S" file;
                    loop ks
                  with End_of_file -> return_unit
                  finally
                    puts "Done reading commands from %S" file;
                    Lwt_io.close ic
              with Unix.Unix_error _ ->
                printf "Couldn't open file %S." file;
                return_unit
            end
          | Directive ("size", _) -> begin
              let t0 = Unix.gettimeofday () in
              let s0 = Sys.time () in
              lwt tables = D.list_tables (get ks) in
              lwt sizes =
                Lwt_list.map_p
                  (fun table -> lwt siz = D.table_size_on_disk (get ks) table in
                               return (string_of_table table, siz))
                  tables in
              let dt = Unix.gettimeofday () -. t0 in
              let sysdt = Sys.time () -. s0 in
                List.iter (fun (table, siz) -> printf "%20s\t%Ld\n" table siz) sizes;
                puts " in %8.5fs (cpu %8.5fs)" dt sysdt;
                return_unit
            end
          | Directive (directive, args) ->
              Directives.eval_directive directive args;
              return_unit
          | Nothing -> return_unit
          | Error s -> printf "Error: %s\n%!" s; return_unit
          | Dump_local destdir ->
              let module DUMP = Obs_dump.Make(struct
                                                include D
                                                include D.Raw_dump
                                              end ) in
              lwt raw_dump = D.Raw_dump.dump db in
              lwt _ = DUMP.dump_local raw_dump ?destdir in
                return_unit
          | Codec_directive (table, desc) ->
              match Printer.build_codec desc with
                  `Error s ->
                    printf "Couldn't install printer for table %S: %s\n%!" table s;
                    return_unit
                | `OK c ->
                    install_key_codec (table_of_string table)
                      (pretty_printer_of_codec c) (Printer.parse_value desc);
                    save_printer ks table desc
    with
      | Parsing.Parse_error ->
          print_endline "Parse error";
          return_unit
      | End_of_file | Deadlock | Abort_exn | Commit_exn | Reload_keyspace _ as e -> raise_lwt e
      | Obs_protocol.Error (Obs_protocol.Exception End_of_file)
      | Obs_protocol.Error (Obs_protocol.Closed) ->
          if !tx_level > 0 then begin
            print_endline "Lost connection: aborting transaction";
            (* we do not want the last phrase to be retried on reconnect since
             * we aborted the tx *)
            raise_lwt (Need_reconnect None)
          end else begin
            raise_lwt (Need_reconnect !last_phrase)
          end
      | e ->
          printf "Got exception: %s\n%!" (Printexc.to_string e);
          return_unit
    end >>
    inner_exec_loop get_phrase db ks

  let recover_saved_printers ks =
    lwt saved_printers =
      D.get_slice_values ks (table_of_string "@meta.printers")
        (`Continuous { first = None; up_to = None; reverse = false; })
        [ "@printer" ]
    in
      List.iter
        (function
           | (table, [Some printer]) -> begin
               let lexbuf = Lexing.from_string printer in
                 try
                   let desc = Obs_repl_gram.printer
                                Obs_repl_lex.token lexbuf
                   in match Printer.build_codec desc with
                       `Error s -> raise Parsing.Parse_error
                     | `OK c ->
                        install_key_codec (table_of_string table)
                          (pretty_printer_of_codec c) (Printer.parse_value desc);
                        printf "Recovered printer for table %s\n%!" table;
                 with Parsing.Parse_error ->
                   printf "Invalid printer expression for table %S, ignoring \
                           (fix with .printer).\n%!"
                     table
             end
           | _ -> ())
        (snd saved_printers);
      return_unit
end

let role = "guest"
let password = "guest"

module type OPS =
sig
  include Obs_data_model.S
  val get_db_and_ks : unit -> (db * keyspace option) Lwt.t
end

module Remote : OPS =
struct
  module R = Obs_protocol_client.Make(Obs_protocol_bin.Version_0_0_0)
  include Make(R)
  include R

  let get_db_and_ks () =
    match Unix.getaddrinfo !server !port [] with
    | [] -> raise (Invalid_argument "Supplied address and/or port cannot be resolved")
    | ais ->
      let rec try_ai ais = match ais with
        | [] -> raise (Need_reconnect (Some "No server at supplied address/port"))
        | ai::ais ->
          try_lwt
            let addr, data_address =
              (match ai.Unix.ai_addr with
               | Unix.ADDR_INET (h, p) -> Unix.ADDR_INET (h, p), Unix.ADDR_INET (h, p + 1)
               | _ -> raise (Need_reconnect (Some "No server at supplied address/port"))) in
            lwt ich, och = Lwt_io.open_connection addr in
            lwt db = R.make ~data_address ich och ~role ~password in
            if !keyspace = "" then
              return (db, None)
            else begin
              printf "Switching to keyspace %S\n%!" !keyspace;
              lwt ks = R.register_keyspace db !keyspace in
              recover_saved_printers ks >>
              return (db, Some ks)
            end
          with Unix.Unix_error _ as exn ->
            if ais = [] then raise exn
            else try_ai ais
      in try_ai ais
end

module Local : OPS =
struct
  include Obs_storage
  include Make(Obs_storage)

  let get_db_and_ks () =
    let db = Obs_storage.open_db !dir in
      if !keyspace = "" then
        return (db, None)
      else begin
        printf "Switching to keyspace %S\n%!" !keyspace;
        lwt ks = Obs_storage.register_keyspace db !keyspace in
          recover_saved_printers ks >>
          return (db, Some ks)
      end
end

let () =
  ignore (Sys.set_signal Sys.sigpipe Sys.Signal_ignore);
  Printexc.record_backtrace true;
  Arg.parse params ignore usage_message;
  Lwt_unix.run begin

    let db_module =
      match !dir with
        | "" -> (module Remote : OPS)
        | dir -> (module Local : OPS) in

    let module D   = (val db_module) in
    let module RUN = Make(D) in

    let rec outer_loop ?phrase db ks =
      try_lwt
        RUN.(inner_exec_loop get_phrase ?phrase db ks)
      with
          End_of_file -> exit 0
        | Abort_exn | Commit_exn ->
            print_endline "Not inside a transaction";
            outer_loop db ks
        | Reload_keyspace new_ks -> begin
            match !curr_keyspace with
                Some (x, _) when x = new_ks -> outer_loop db ks
              | _ ->
                  printf "Switching to keyspace %S\n%!" new_ks;
                  keyspace := new_ks;
                  lwt ks = D.register_keyspace db !keyspace in
                    curr_keyspace := Some (D.keyspace_name ks, D.keyspace_id ks);
                    (* clear codec table, load those saved in @meta.printers
                     * table *)
                    Hashtbl.clear key_codecs;
                    RUN.recover_saved_printers ks >>
                    outer_loop db (Some ks)
          end
        | Need_reconnect phrase ->
            let rec reconnect retry_phrase =
              try_lwt
                print_endline "Reconnecting to server...";
                lwt db, ks = D.get_db_and_ks () in
                  return (retry_phrase, db, ks)
              with exn ->
                printf "Failed (%s).\n%!" (Printexc.to_string exn);
                Lwt_unix.sleep 1.0 >>
                reconnect false in
            lwt retry_phrase, db, ks = reconnect true in
            let phrase = if retry_phrase then phrase else None in
              outer_loop ?phrase db ks in

    let () =
      puts "ob_repl";
      puts "Enter \".help\" for instructions.";
    in
    lwt db, ks = D.get_db_and_ks () in
      curr_keyspace := Option.map (fun ks -> (D.keyspace_name ks, D.keyspace_id ks)) ks;
      outer_loop db ks
  end
