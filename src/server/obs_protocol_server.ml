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

open Printf
open Lwt
open Obs_data_model
open Obs_protocol
open Obs_request

module PP = Extprot.Pretty_print

let data_protocol_version = (0, 0, 0)

let gen_section  = Lwt_log.Section.make "obigstore:server"
let req_section  = Lwt_log.Section.make "obigstore:server:req"
let resp_section = Lwt_log.Section.make "obigstore:server:resp"
let tx_section   = Lwt_log.Section.make "obigstore:server:tx"

type replication_wait = Await_reception | Await_commit

module TY : sig
  type ks_id = private int

  val ks_id_of_int : int -> ks_id
  val new_transaction_id : unit -> ks_id
end =
struct
  type ks_id = int

  (* we use even ids for "toplevel" keyspaces, and odd ones for
   * virtual keyspaces = transactions *)
  let new_transaction_id =
    let n = ref 1 in
      (fun () -> n := !n + 2; !n)

  let ks_id_of_int n = n
end

open TY

module Make
  (D : sig
     include Obs_data_model.S
     include Obs_data_model.BACKUP_SUPPORT with type backup_cursor := backup_cursor

     module Replication : Obs_replication.REPLICATION_SERVER
       with type db := db and type raw_dump := Raw_dump.raw_dump

     val use_thread_pool : db -> bool -> unit

     val throttling : db -> float
   end) =
struct
  open Request

  type request_id = string

  exception Abort_exn of request_id
  exception Commit_exn of request_id
  exception Abort_all_txs

  module H = Hashtbl.Make(struct
                              type t = ks_id
                              let hash (n : ks_id) = (n :> int)
                              let equal a b = (a == b)
                            end)

  let num_clients = ref 0

  let dummy_auto_yield () = return_unit

  let auto_yielder = ref dummy_auto_yield

  type client_id = int

  type t =
      {
        db : D.db;
        raw_dumps : (Int64.t, D.Raw_dump.raw_dump) Hashtbl.t;
        mutable raw_dump_seqno : Int64.t;
        replication_wait : replication_wait;
        async_req_region : Obs_lwt_region.region;
        max_concurrency_factor : int;
        mutable curr_concurrency_factor : int;
      }

  type client_state =
      {
        id : client_id;
        (* each keypsace holds a list of child ks ("virtual keyspaces") so
         * that we can release them on parent tx commit *)
        keyspaces : (keyspace * req_handler option * keyspace Lwt_sequence.t) H.t;
        ich : Lwt_io.input_channel;
        och : Lwt_io.output_channel;
        server : t;
        in_buf : string ref;
        out_buf : Obs_bytea.t;
        signal_error : (Request.request * string * int) option waiter;
        read_request : client_state -> (Request.request * string * int) option Lwt.t;
        payload_writer : (module Obs_protocol.PAYLOAD_WRITER);
      }

  and 'a waiter = 'a Lwt.t * 'a Lwt.u

  and keyspace =
    { ks_unique_id : ks_id;
      ks_tx_key : tx_data Lwt.key;
      ks_ks : D.keyspace;
      mutable ks_node : keyspace Lwt_sequence.node option;
    }

  and req_handler = (tx_request Lwt_stream.t * (tx_request option -> unit))

  and tx_request = (* request to be handler in TX context *)
      Exception of exn
    | Request of request_id * Request.request

  and tx_data = unit

  let find_or_add find add h k =
    try find h k with Not_found -> add h k

  let read_exactly c n =
    let s = String.create n in
      Lwt_io.read_into_exactly c.ich s 0 n >>
      return s

  (* the chunk of the region needed to perform a given request --- allows to
   * impose diff limits per type *)
  let request_slot_cost server ~request_size = function
      Load _ -> (request_size lsr 5) lsl 10
    | Put_columns { Put_columns.data; _ } ->
        min
          (server.curr_concurrency_factor - 1)
          ((List.length data + 1 + request_size lsr 5) lsl 9)
    | _ -> 1

  let with_raw_dump c id default f =
    try_lwt
      let raw_dump = Hashtbl.find c.server.raw_dumps id in
        f raw_dump
    with Not_found -> return default

  let rec pp_list pp fmt = function
      [] -> ()
    | x :: tl -> Format.fprintf fmt "%a,@ " pp x;
                 pp_list pp fmt tl

  let pp_request_id fmt id =
    if Obs_protocol.is_sync_req id then
      Format.fprintf fmt "<sync>"
    else
      Format.fprintf fmt "%s" Cryptokit.(transform_string (Hexa.encode ()) id)

  let pp_slice fmt (lastkey, kds) =
    let open Obs_data_model in
    let pp_data colname fmt data =
      if colname = "" || colname.[0] <> '@' then
        Format.fprintf fmt "%S" data
      else
        try
          let doc = Obs_bson.document_of_string data in
            Obs_bson.pp_bson ~strict:false fmt doc
        with Obs_bson.Malformed _ ->
          Format.fprintf fmt "%S" data in

    let pp_col fmt col =
      Format.fprintf fmt
        "{ @[name: %S;@ data: %a;@ timestamp: %s@]}"
        col.name (pp_data col.name) col.data
        (match col.timestamp with
             No_timestamp -> "auto"
           | Timestamp x -> Int64.to_string x) in
    let pp_kd fmt kd =
      Format.fprintf fmt
        "{ @[key: %S;@ last_column: %S;@ columns: @[%a@] }@]"
        kd.key kd.last_column (pp_list pp_col) kd.columns
    in
      Format.fprintf fmt "(@[%s,@ %a)@]"
        (match lastkey with None -> "None" | Some x -> sprintf "Some %S" x)
        (pp_list pp_kd) kds

  let maybe_pp c pp ~request_id x =
    Lwt_log.debug_f ~section:resp_section
      "Sending response for %s to %d:\n%s"
      (PP.pp pp_request_id request_id) c.id (PP.pp pp x) >>
    return x

  exception Denied

  let check_perms perms req =
    let open Obs_request.Request in
    match perms, req with
    | `Full_access, _ -> ()
    | `Replication,
      (Trigger_raw_dump _ | Raw_dump_release _ |
       Raw_dump_list_files _ | Raw_dump_file_digest _) -> ()
    | `Replication,
      (Register_keyspace _ | Get_keyspace _ | List_keyspaces _ | List_tables _
      | Table_size_on_disk _ | Key_range_size_on_disk _ | Begin _ | Commit _
      | Abort _ | Lock _ | Exist_keys _ | Get_keys _ | Count_keys _ | Get_slice _
      | Get_slice_values _ | Get_slice_values_timestamps _ | Get_columns _
      | Get_column_values _ | Get_column _ | Put_columns _ | Delete_columns _
      | Delete_key _ | Dump _ | Load _ | Stats _ | Listen _ | Unlisten _
      | Notify _ | Await _ | Release_keyspace _
      | Get_property _ | Watch_keys _ | Watch_columns _ | Delete_keys _
      | Watch_prefixes _ | Get_transaction_id _ | Listen_prefix _
      | Unlisten_prefix _ | Compact_keyspace _ | Compact_table _
      | List_transactions _ | Changed_tables _) -> raise Denied

  let rec service c perms =
    !auto_yielder () >>
    let module P = (val c.payload_writer : Obs_protocol.PAYLOAD_WRITER) in
    let throttling = D.throttling c.server.db in
    let new_factor = truncate (float c.server.max_concurrency_factor *. throttling) in
      c.server.curr_concurrency_factor <- new_factor;
      Obs_lwt_region.resize_region
        c.server.async_req_region c.server.curr_concurrency_factor;

      match_lwt Lwt.choose [fst c.signal_error; c.read_request c] with
          None -> service c perms
        | Some (r, request_id, len) ->
            let cost = request_slot_cost c.server ~request_size:len r in
              ignore begin
                Obs_lwt_region.run_in_region c.server.async_req_region cost
                  (fun () ->
                     begin try_lwt
                       check_perms perms r;
                       relay_to_handler c ~request_id r
                     with
                       | End_of_file | Lwt_io.Channel_closed _
                       | Unix.Unix_error((Unix.ECONNRESET | Unix.EPIPE), _, _)
                       | Denied -> begin
                           try_lwt
                             P.denied c.och ~request_id ()
                           with _ -> return_unit
                         end
                       | Abort_all_txs ->
                           (* catch exns that indicate that the connection
                            * is gone, and signal End_of_file *)
                           begin try
                             Lwt.wakeup_exn (snd c.signal_error) End_of_file
                           with _ -> ()
                           end;
                           return_unit
                       | e ->
                           let backtrace = Printexc.get_backtrace () in
                           Lwt_log.error_f ~section:gen_section
                             ~exn:e "Internal error\nrequest:\n%s\n%s"
                             (PP.pp Request.pp r)
                             backtrace >>
                           try_lwt
                             P.internal_error c.och ~request_id ()
                           with _ -> return_unit
                     end)
              end;
              (* here we block if the allowed number of async reqs is reached
               * until one of them is done. Using [cost] here ensures we block
               * if the request consumed all of [curr_concurrency_factor - 1]. *)
              Obs_lwt_region.run_in_region c.server.async_req_region cost (fun () -> return_unit) >>
              service c perms

  and relay_to_handler c ~request_id = function
    | List_tables { List_tables.keyspace; _ }
    | Table_size_on_disk { Table_size_on_disk.keyspace; _ }
    | Key_range_size_on_disk { Key_range_size_on_disk.keyspace; _ }
    | Begin { Begin.keyspace; _ }
    | Commit { Commit.keyspace; _ }
    | Abort { Abort.keyspace; _ }
    | Lock { Lock.keyspace; _ }
    | Get_transaction_id { Get_transaction_id.keyspace; _ }
    | Exist_keys { Exist_keys.keyspace; _ }
    | Get_keys { Get_keys.keyspace; _ }
    | Count_keys { Count_keys.keyspace; _ }
    | Get_slice { Get_slice.keyspace; _ }
    | Get_slice_values { Get_slice_values.keyspace; _ }
    | Get_slice_values_timestamps { Get_slice_values_timestamps.keyspace; _ }
    | Get_columns { Get_columns.keyspace; _ }
    | Get_column_values { Get_column_values.keyspace; _ }
    | Get_column { Get_column.keyspace; _ }
    | Put_columns { Put_columns.keyspace; _ }
    | Delete_columns { Delete_columns.keyspace; _ }
    | Delete_key { Delete_key.keyspace; _ }
    | Delete_keys { Delete_keys.keyspace; _ }
    | Dump { Dump.keyspace; _ }
    | Load { Load.keyspace; _ }
    | Stats { Stats.keyspace; _ }
    | Listen { Listen.keyspace; _ }
    | Unlisten { Unlisten.keyspace; _ }
    | Listen_prefix { Listen_prefix.keyspace; _ }
    | Unlisten_prefix { Unlisten_prefix.keyspace; _ }
    | Notify { Notify.keyspace; _ }
    | Await { Await.keyspace; _ }
    | Release_keyspace { Release_keyspace.keyspace; _ }
    | Watch_keys { Watch_keys.keyspace; _ }
    | Watch_prefixes { Watch_prefixes.keyspace; _ }
    | Watch_columns { Watch_columns.keyspace; _ } as r ->
        begin try
          let _, handler, _ = H.find c.keyspaces (ks_id_of_int keyspace) in
            match handler with
                None ->
                  (* toplevel (real) keyspace *)
                  Lwt_log.debug_f ~section:req_section "Toplevel respond for %s"
                    (PP.pp pp_request_id request_id) >>
                  respond c ~request_id r
              | Some (_, push) ->
                  (* virtual keyspace = transaction *)
                  lwt () =
                    Lwt_log.debug_f ~section:req_section "Tunnelled respond for %s"
                      (PP.pp pp_request_id request_id)
                  in
                    push (Some (Request (request_id, r)));
                    return_unit
        with Not_found ->
          (* will usually respond with a unknown_keyspace error *)
          Lwt_log.warning_f ~section:resp_section "No handler found for for %s %s"
            (PP.pp pp_request_id request_id)
            (PP.pp Request.pp r) >>
          respond c ~request_id r
        end
    | Register_keyspace _ | Get_keyspace _ | List_keyspaces _
    | Trigger_raw_dump _ | Raw_dump_release _ | Raw_dump_list_files _
    | Raw_dump_file_digest _ | Get_property _
    | Compact_keyspace _ | Compact_table _
    | List_transactions _ | Changed_tables _ as r ->
        respond c ~request_id r

  and respond ?txid ?buf c ~request_id r =
    let module P = (val c.payload_writer : Obs_protocol.PAYLOAD_WRITER) in
    Lwt_log.debug_f ~section:req_section
      "Got request %s from %d (tx: %s)\n%s"
      (PP.pp pp_request_id request_id)
      c.id
      (match txid with None -> "<none>"
         | Some (cur, outer) -> sprintf "cur:%d outer:%d" cur outer)
      (PP.pp Request.pp r) >>
    match r with
      Register_keyspace { Register_keyspace.name } ->
        lwt ks_ks = D.register_keyspace c.server.db name in
        (* we use even ids for "toplevel" keyspaces, and odd ones for
         * virtual keyspaces = transactions *)
        let ks_unique_id = ks_id_of_int (2 * D.keyspace_id ks_ks) in
        let ks = { ks_unique_id; ks_ks; ks_tx_key = Lwt.new_key (); ks_node = None } in
          H.add c.keyspaces ks_unique_id (ks, None, Lwt_sequence.create ());
          Lwt_log.info_f ~section:gen_section
            "Registered namespace %S:%d" name (ks_unique_id :> int) >>
          P.return_keyspace ?buf c.och ~request_id (ks_unique_id :> int)
    | Get_keyspace { Get_keyspace.name; } -> begin
        match_lwt D.get_keyspace c.server.db name with
            None -> P.return_keyspace_maybe ?buf c.och ~request_id None
          | Some ks_ks ->
              (* we use even ids for "toplevel" keyspaces, and odd ones for
               * virtual keyspaces = transactions *)
              let ks_unique_id = ks_id_of_int (2 * D.keyspace_id ks_ks) in
              let ks = { ks_ks; ks_unique_id; ks_tx_key = Lwt.new_key (); ks_node = None } in
                H.add c.keyspaces ks_unique_id (ks, None, Lwt_sequence.create ());
                P.return_keyspace_maybe ?buf c.och ~request_id
                  (Some ks_unique_id :> int option)
      end
    | Release_keyspace { Release_keyspace.keyspace } ->
        begin try
          let ks, _, _ = H.find c.keyspaces (ks_id_of_int keyspace) in
            remove_keyspace c ks
        with Not_found -> () end;
        P.return_ok ?buf c.och ~request_id ()
    | List_keyspaces _ ->
        D.list_keyspaces c.server.db >>=
          P.return_keyspace_list ?buf c.och ~request_id
    | List_tables { List_tables.keyspace } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             (D.list_tables ks.ks_ks :> string list Lwt.t) >>=
             P.return_table_list ?buf c.och ~request_id)
    | Table_size_on_disk { Table_size_on_disk.keyspace; table; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.table_size_on_disk ks.ks_ks table >>=
             P.return_table_size_on_disk ?buf c.och ~request_id)
    | Key_range_size_on_disk { Key_range_size_on_disk.keyspace; table; range; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.key_range_size_on_disk ks.ks_ks table
               ?first:range.first ?up_to:range.up_to >>=
             P.return_key_range_size_on_disk ?buf c.och ~request_id)
    | Begin { Begin.keyspace; tx_type; sync; } ->
        with_keyspace c keyspace ~request_id
          (fun x -> respond_to_begin ?buf c x ~request_id tx_type sync)
    | Commit { Commit.keyspace; _ } ->
        (* only commit if we're inside a tx *)
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             begin match Lwt.get ks.ks_tx_key with
                 None -> P.return_ok ?buf c.och ~request_id ()
               | Some _ -> raise_lwt (Commit_exn request_id)
             end)
    | Abort { Abort.keyspace; _ } ->
        (* only abort if we're inside a tx *)
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             begin match Lwt.get ks.ks_tx_key with
                 None -> P.return_ok ?buf c.och ~request_id ()
               | Some _ ->
                   Lwt_log.debug_f ~section:gen_section
                     "Abort req_id %S causes Abort_exn" request_id >>
                   raise_lwt (Abort_exn request_id)
             end)
    | Lock { Lock.keyspace; names; shared; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             try_lwt
               D.lock ks.ks_ks ~shared names >>
               P.return_ok ?buf c.och ~request_id ()
             with Deadlock ->
               P.deadlock ?buf c.och ~request_id () >>
               raise_lwt (Abort_exn request_id))
    | Watch_keys { Watch_keys.keyspace; table; keys } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.watch_keys ks.ks_ks table keys >>
             P.return_ok ?buf c.och ~request_id ())
    | Watch_prefixes { Watch_prefixes.keyspace; table; prefixes; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.watch_prefixes ks.ks_ks table prefixes >>
             P.return_ok ?buf c.och ~request_id ())
    | Watch_columns { Watch_columns.keyspace; table; columns; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.watch_columns ks.ks_ks table columns; >>
             P.return_ok ?buf c.och ~request_id ())
    | Get_keys { Get_keys.keyspace; table; max_keys; key_range; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.get_keys ks.ks_ks table ?max_keys (krange' key_range) >>=
             P.return_keys ?buf c.och ~request_id)
    | Exist_keys { Exist_keys.keyspace; table; keys; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.exist_keys ks.ks_ks table keys >>=
             P.return_exist_result ?buf c.och ~request_id)
    | Count_keys { Count_keys.keyspace; table; key_range; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.count_keys ks.ks_ks table (krange' key_range) >>=
             P.return_key_count ?buf c.och ~request_id)
    | Get_slice { Get_slice.keyspace; table; max_keys; max_columns;
                  decode_timestamps; key_range; predicate; column_range; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.get_slice ks.ks_ks table ?max_keys ?max_columns ~decode_timestamps
               (krange' key_range) ?predicate (crange' column_range) >>=
             maybe_pp c pp_slice ~request_id >>=
             P.return_slice ?buf c.och ~request_id)
    | Get_slice_values { Get_slice_values.keyspace; table; max_keys;
                         key_range; columns; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.get_slice_values ks.ks_ks table ?max_keys (krange' key_range) columns >>=
             P.return_slice_values ?buf c.och ~request_id)
    | Get_slice_values_timestamps
        { Get_slice_values_timestamps.keyspace; table; max_keys; key_range; columns; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.get_slice_values_with_timestamps ks.ks_ks table ?max_keys
               (krange' key_range) columns >>=
             P.return_slice_values_timestamps ?buf c.och ~request_id)
    | Get_columns { Get_columns.keyspace; table; max_columns;
                    decode_timestamps; key; column_range; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.get_columns ks.ks_ks table ?max_columns ~decode_timestamps
               key (crange' column_range) >>=
             P.return_columns ?buf c.och ~request_id)
    | Get_column_values { Get_column_values.keyspace; table; key; columns; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.get_column_values ks.ks_ks table key columns >>=
             P.return_column_values ?buf c.och ~request_id)
    | Get_column { Get_column.keyspace; table; key; column; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.get_column ks.ks_ks table key column >>=
             P.return_column ?buf c.och ~request_id)
    | Put_columns { Put_columns.keyspace; table; data; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.put_multi_columns ks.ks_ks table data >>=
             P.return_ok ?buf c.och ~request_id)
    | Delete_columns { Delete_columns.keyspace; table; key; columns; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.delete_columns ks.ks_ks table key columns >>=
             P.return_ok ?buf c.och ~request_id)
    | Delete_key { Delete_key.keyspace; table; key; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.delete_key ks.ks_ks table key >>=
             P.return_ok ?buf c.och ~request_id)
    | Delete_keys { Delete_keys.keyspace; table; key_range; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.delete_keys ks.ks_ks table (krange' key_range) >>=
             P.return_ok ?buf c.och ~request_id)
    | Dump { Dump.keyspace; only_tables; cursor; format; } ->
        let offset = match cursor with
            None -> None
          | Some c -> D.cursor_of_string c in
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             lwt x =
               D.dump ks.ks_ks ?format ?only_tables ?offset () >|= function
                   None -> None
                 | Some (data, None) -> Some (data, None)
                 | Some (data, Some cursor) ->
                     Some (data, Some (D.string_of_cursor cursor))
             in P.return_backup_dump ?buf c.och ~request_id x)
    | Load { Load.keyspace; data; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.load ks.ks_ks data >>=
             P.return_backup_load_result ?buf c.och ~request_id)
    | Stats { Stats.keyspace; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) -> D.load_stats ks.ks_ks >>=
                          P.return_load_stats ?buf c.och ~request_id)
    | Listen { Listen.keyspace; topic; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.listen ks.ks_ks topic >>
             P.return_ok ?buf c.och ~request_id ())
    | Unlisten { Unlisten.keyspace; topic; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.unlisten ks.ks_ks topic >>
             P.return_ok ?buf c.och ~request_id ())
    | Listen_prefix { Listen_prefix.keyspace; topic; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.listen_prefix ks.ks_ks topic >>
             P.return_ok ?buf c.och ~request_id ())
    | Unlisten_prefix { Unlisten_prefix.keyspace; topic; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.unlisten_prefix ks.ks_ks topic >>
             P.return_ok ?buf c.och ~request_id ())
    | Notify { Notify.keyspace; topic; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.notify ks.ks_ks topic >>
             P.return_ok ?buf c.och ~request_id ())
    | Await { Await.keyspace; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.await_notifications ks.ks_ks >>=
             P.return_notifications ?buf c.och ~request_id)
    | Trigger_raw_dump { Trigger_raw_dump.mode; } ->
        (* FIXME: catch errors in Raw_dump.dump, signal to client *)
        let open Raw_dump_mode in
        let mode      = match mode with
                          | Sync -> `Sync
                          | Async -> `Async
                          | No_stream -> `No_stream in
        lwt raw_dump  = D.Raw_dump.dump ~mode c.server.db in
        lwt timestamp = D.Raw_dump.timestamp raw_dump in
        lwt localdir  = D.Raw_dump.localdir raw_dump in
        let dump_id   = c.server.raw_dump_seqno in
          c.server.raw_dump_seqno <- Int64.add 1L dump_id;
          Hashtbl.add c.server.raw_dumps dump_id raw_dump;
          P.return_raw_dump_id_timestamp_dir ?buf c.och ~request_id
            (dump_id, timestamp, localdir)
    | Raw_dump_release { Raw_dump_release.id; keep_files; } ->
        with_raw_dump c id ()
          (fun raw_dump ->
             Hashtbl.remove c.server.raw_dumps id;
             D.Raw_dump.release raw_dump ~keep_files) >>=
        P.return_ok ?buf c.och ~request_id
    | Raw_dump_list_files { Raw_dump_list_files.id } ->
        with_raw_dump c id [] D.Raw_dump.list_files >>=
        P.return_raw_dump_files ?buf c.och ~request_id
    | Raw_dump_file_digest { Raw_dump_file_digest.id; file; } ->
        with_raw_dump c id None
          (fun d -> D.Raw_dump.file_digest d file) >>=
        P.return_raw_dump_file_digest ?buf c.och ~request_id
    | Get_property { Get_property.property; } ->
        D.get_property c.server.db property >>=
        P.return_property ?buf c.och ~request_id
    | Get_transaction_id { Get_transaction_id.keyspace } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.transaction_id ks.ks_ks >>=
             P.return_tx_id ?buf c.och ~request_id)
    | Compact_keyspace { Compact_keyspace.keyspace } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.compact ks.ks_ks >>=
             P.return_ok ?buf c.och ~request_id)
    | Compact_table { Compact_table.keyspace; table; from_key; to_key } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.compact_table ks.ks_ks table ?from_key ?to_key ()>>=
             P.return_ok ?buf c.och ~request_id)
    | List_transactions { List_transactions.keyspace; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.list_transactions ks.ks_ks >>=
             P.return_transaction_list ?buf c.och ~request_id)
    | Changed_tables { Changed_tables.keyspace; tx_id; } ->
        with_keyspace c keyspace ~request_id
          (fun (ks, _, _) ->
             D.changed_tables ks.ks_ks tx_id >>=
             P.return_changed_tables ?buf c.och ~request_id)

  and respond_to_begin ?buf c (ks, _, children) ~request_id tx_type sync =
    let module P = (val c.payload_writer : Obs_protocol.PAYLOAD_WRITER) in
    let transaction_f = match tx_type with
        Tx_type.Repeatable_read -> D.repeatable_read_transaction
      | Tx_type.Read_committed -> D.read_committed_transaction in
    let commit_request_id = ref "" in
      try_lwt
        (* we get the request_id from the request that forced the commit
         * (carried by Commit_exn) *)
        let (req_stream, pushf) as handler = Lwt_stream.create () in
        let outer_ks_id = ks.ks_unique_id in
        lwt commit_reqid =
          transaction_f
            ~sync:(match sync with
                     | Sync_mode.Sync -> `Sync
                     | Sync_mode.Async -> `Async)
            ks.ks_ks begin fun ks' ->
            let ks =
              { ks_unique_id = new_transaction_id ();
                ks_tx_key = ks.ks_tx_key; ks_ks = ks';
                ks_node = None;
              } in
            lwt () =
              Lwt_log.debug_f ~section:gen_section
                "Registering virtual (TX) keyspace %d for %d"
                (ks.ks_unique_id :> int) (outer_ks_id :> int) in
            (* register it in the parent's children set *)
            let node = Lwt_sequence.add_l ks children in
              ks.ks_node <- Some node;
              try_lwt
                (* install handler *)
                H.add c.keyspaces ks.ks_unique_id (ks, Some handler, Lwt_sequence.create ());
                P.return_keyspace ?buf c.och ~request_id (ks.ks_unique_id :> int) >>
                lwt txid = D.transaction_id ks.ks_ks in
                Lwt_log.debug_f ~section:tx_section
                  "BEGIN transaction by client %d, id %s"
                  c.id
                  (match txid with
                     | None -> "<none>"
                     | Some (cur, outer) -> sprintf "cur:%d outer:%d" cur outer) >>
                let rec handle_reqs () =
                  match_lwt Lwt_stream.get req_stream with
                    | Some (Request (request_id, r)) ->
                        (* we respond in a background thread so as to process
                         * reqs in parallel but need to capture exns (in
                         * particular, Abort_exn and Commit_exn) *)
                        ignore begin
                          try_lwt
                            Lwt.with_value ks.ks_tx_key (Some ())
                              (fun () -> respond ?txid c ~request_id r)
                          with e ->
                              pushf (Some (Exception e));
                              return_unit
                        end;
                        handle_reqs ()
                    | Some (Exception (Commit_exn req_id)) ->
                        commit_request_id := req_id;
                        Lwt_log.debug_f ~section:resp_section
                          "Returning after Commit_exn in keyspace %d, req %s"
                          (ks.ks_unique_id :> int)
                          (PP.pp pp_request_id req_id) >>
                        return req_id
                    | Some (Exception e) -> raise_lwt e
                    | None -> fail (Failure "TX request stream canceled")
                in handle_reqs ()
              finally
                lwt () = Lwt_log.debug_f ~section:resp_section
                           "Commit or abort ks %d" (ks.ks_unique_id :> int)
                in
                  remove_keyspace c ks;
                  return_unit
          end
        in P.return_ok ?buf c.och ~request_id:commit_reqid ()
      with
          | Abort_exn request_id ->
              Lwt_log.debug_f ~section:resp_section
                "Abort_exn for request_id %S caught, sending OK" request_id >>
              P.return_ok ?buf c.och ~request_id ()
          | Obs_data_model.Dirty_data -> P.dirty_data ?buf c.och ~request_id:!commit_request_id ()

  and with_keyspace c ks_idx ~request_id f =
    try
      f (H.find c.keyspaces (ks_id_of_int ks_idx))
    with Not_found ->
      let module P = (val c.payload_writer : Obs_protocol.PAYLOAD_WRITER) in
        P.unknown_keyspace c.och ~request_id ()

  (* remove keyspace and its children (recursively) from keyspace table *)
  and remove_keyspace c ks =
    try
      let _, _, children = H.find c.keyspaces ks.ks_unique_id in
        H.remove c.keyspaces ks.ks_unique_id;
        BatOption.may Lwt_sequence.remove ks.ks_node;
        Lwt_sequence.iter_l (remove_keyspace c) children
    with Not_found -> ()

  let setup_auto_yield t c =
    incr num_clients;
    if !num_clients >= 2 then begin
      D.use_thread_pool t.db true;
      let f = Lwt_unix.auto_yield 0.5e-3 in
        auto_yielder := f
    end;
    let db = t.db in
      Lwt_gc.finalise
        (fun _ ->
           decr num_clients;
           if !num_clients <= 1 then begin
             D.use_thread_pool db false;
             auto_yielder := dummy_auto_yield
           end;
           return_unit)
        c

  let make ?(replication_wait = Await_commit) ?(max_async_reqs = 5000) db =
    let max_concurrency_factor = max 1000 (1000 * max_async_reqs) in
      {
        db;
        raw_dump_seqno = 0L;
        raw_dumps = Hashtbl.create 13;
        replication_wait;
        max_concurrency_factor;
        async_req_region = Obs_lwt_region.make_region max_concurrency_factor;
        curr_concurrency_factor = max_concurrency_factor;
      }

  let client_id = ref 0

  let service_client server protocol ich och perms =
    let module PROTO = (val protocol : Obs_protocol.SERVER_FUNCTIONALITY) in
    let read_request c = PROTO.read_request c.in_buf c.ich c.och in
    let payload_writer = (module PROTO : Obs_protocol.PAYLOAD_WRITER) in
    let c =
      {
        id = (incr client_id; !client_id);
        keyspaces = H.create 13;
        ich; och; server;
        out_buf = Obs_bytea.create 1024;
        in_buf = ref (String.create 128);
        signal_error = Lwt.task ();
        payload_writer; read_request;
      }
    in setup_auto_yield server c;
       Lwt_log.debug_f ~section:req_section "New client %d" c.id >>
       try_lwt
         service c perms
       with Unix.Unix_error (Unix.ECONNRESET, _, _) ->
         raise_lwt End_of_file
       finally
         H.iter
           (fun _ (_, handler, _) -> match handler with
                None -> ()
              | Some (_, push) -> push (Some (Exception Abort_all_txs)))
         c.keyspaces;
         return_unit

  let send_response_code code och =
    write_checksummed_int32_le och (data_response_code code)

  let handle_get_file ~debug server req_ch ich och =
    lwt dump_id = Lwt_io.LE.read_int64 req_ch in
    lwt offset = Lwt_io.LE.read_int64 req_ch in
    lwt name_siz = Lwt_io.LE.read_int req_ch in
    lwt name = Obs_protocol.read_exactly req_ch name_siz in
      try_lwt
        let dump = Hashtbl.find server.raw_dumps dump_id in
          match_lwt D.Raw_dump.open_file dump ~offset name with
              None -> send_response_code `Unknown_file och
            | Some ic ->
                try_lwt
                  send_response_code `OK och >>
                  let buf = String.create 16384 in
                  let rec loop_copy_data () =
                    match_lwt Lwt_io.read_into ic buf 0 16384 with
                        0 -> return_unit
                      | n -> Lwt_io.write_from_exactly och buf 0 n >>
                             loop_copy_data ()
                  in loop_copy_data ()
                finally
                  Lwt_io.close ic
      with Not_found -> send_response_code `Unknown_dump och

  let rec write_and_update_crc crc och buf off len =
    if len <= 0 then return_unit
    else begin
      lwt n = Lwt_io.write_from och buf off len in
        Obs_crc32c.update crc buf off len;
        write_and_update_crc crc och buf (off + n) (len - n)
    end

  let handle_get_updates ~debug server req_ch ich och =
    lwt dump_id = Lwt_io.LE.read_int64 req_ch in
    try_lwt
      let crc = Obs_crc32c.create () in
      let dump = Hashtbl.find server.raw_dumps dump_id in
      lwt stream = D.Replication.get_update_stream dump in

      let rec forward_updates () =
        match_lwt D.Replication.get_update stream with
            None -> write_checksummed_int64_le och (-1L)
          | Some update ->
              begin try_lwt
                lwt buf, off, len = D.Replication.get_update_data update in
                  Lwt_log.debug_f ~section:gen_section "Sending update (%d bytes)" len >>
                  write_checksummed_int64_le och (Int64.of_int len) >>

                  let rec copy_data () =
                    write_and_update_crc crc och buf off len >>
                    Lwt_io.write och (Obs_crc32c.unsafe_masked_result crc) >>
                    Lwt_io.flush och >>
                    match_lwt read_checksummed_int ich with
                        None -> raise_lwt Corrupted_data_header
                      | Some 0 -> return_unit
                      | Some (1 | _) -> copy_data ()
                  in
                    Obs_crc32c.reset crc;
                    copy_data () >>
                    match server.replication_wait with
                        Await_reception ->
                          D.Replication.ack_update update >>
                          lwt _ = read_checksummed_int ich in
                            return_unit
                      | Await_commit ->
                          match_lwt read_checksummed_int ich with
                              Some 0 -> D.Replication.ack_update update
                            | Some (1 | _) -> D.Replication.nack_update update
                            | None -> raise_lwt Corrupted_data_header
              with exn ->
                D.Replication.nack_update update >>
                (* release no longer needed dump resources (e.g. files) *)
                D.Raw_dump.release dump ~keep_files:false >>
                raise_lwt exn
              end >>
              forward_updates ()
      in
        send_response_code `OK och >>
        forward_updates ()
    with Not_found -> send_response_code `Unknown_dump och

  let lwt_io_of_string ~mode str = Lwt_io.of_bytes ~mode (Lwt_bytes.of_string str)

  let service_data_client server ?(debug=false) ich och perms =
    try_lwt
      lwt (major, minor, bugfix) = data_conn_handshake ich och in
      match_lwt read_checksummed_int ich with
          None -> raise_lwt Corrupted_data_header
        | Some req_len ->
            lwt req = Obs_protocol.read_exactly ich req_len in
            lwt req_crc = Obs_protocol.read_exactly ich 4 in
              if Obs_crc32c.string_masked req <> req_crc then raise Corrupted_data_header;
              let req_ch = lwt_io_of_string Lwt_io.input req in
              (* make sure we get a compile-time error if we ever change the
               * type witness *)
              let ()     = match perms with | `Full_access | `Replication -> () in
                match_lwt Lwt_io.LE.read_int req_ch >|= data_request_of_code with
                  | `Get_file -> handle_get_file ~debug server req_ch ich och
                  | `Get_updates -> handle_get_updates ~debug server req_ch ich och
                  | _ -> return_unit
        with Unix.Unix_error (Unix.ECONNRESET, _, _) ->
          raise_lwt End_of_file

end

