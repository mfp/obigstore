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

let data_protocol_version = (0, 0, 0)

type replication_wait = Await_reception | Await_commit

module TY : sig
  type ks_id = private int

  val ks_id_of_int : int -> ks_id
  val new_unique_id : unit -> ks_id
end =
struct
  type ks_id = int

  let new_unique_id =
    let n = ref 0 in
      (fun () -> incr n; !n)

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
   end)
  (P : PAYLOAD) =
struct
  open Request

  type request_id = string

  exception Abort_exn of request_id
  exception Commit_exn of request_id

  module H = Hashtbl.Make(struct
                              type t = ks_id
                              let hash (n : ks_id) = (n :> int)
                              let equal a b = (a == b)
                            end)

  module Notif_queue : sig
    type 'a t
    val empty : 'a t
    val push : 'a -> 'a t -> 'a t
    val iter : ('a -> unit) -> 'a t  -> unit
  end =
  struct
    type 'a t = 'a list

    let empty = []

    (* TODO: further "compression" by keeping set of previous notifications? *)
    let push x = function
        y :: _ as l when y = x -> l
      | l -> x :: l

    let iter f t = List.iter f (List.rev t)
  end

  let num_clients = ref 0

  let dummy_auto_yield () = return ()

  let auto_yielder = ref dummy_auto_yield

  type client_id = int
  type topic = string
  type keyspace_name = string

  type subscription_descriptor = { subs_ks : keyspace_name; subs_topic : topic }

  type subs_stream = string Lwt_stream.t * (string option -> unit)
  type mutable_string_set = (string, unit) Hashtbl.t

  type t =
      {
        db : D.db;
        subscriptions : (subscription_descriptor, subs_stream H.t) Hashtbl.t;
        raw_dumps : (Int64.t, D.Raw_dump.raw_dump) Hashtbl.t;
        mutable raw_dump_seqno : Int64.t;
        replication_wait : replication_wait;
      }

  type client_state =
      {
        id : client_id;
        keyspaces : (keyspace * req_handler Stack.t) H.t;
        ich : Lwt_io.input_channel;
        och : Lwt_io.output_channel;
        server : t;
        mutable in_buf : string;
        out_buf : Obs_bytea.t;
        debug : bool;
        mutable pending_reqs : int;
        wait_for_pending_reqs : unit Lwt_condition.t;
        async_req_region : Lwt_util.region;
        signal_error : Request.request option Lwt.t * Request.request option Lwt.u;
      }

  and keyspace =
    { ks_unique_id : ks_id;
      ks_tx_key : tx_data Lwt.key;
      ks_ks : D.keyspace;
      ks_subs_stream : subs_stream;
      ks_subscriptions : mutable_string_set;
  }

  and req_handler = request_id:request_id -> Request.request -> unit Lwt.t

  and tx_data =
      {
        mutable tx_notifications : string Notif_queue.t;
      }

  let find_or_add find add h k =
    try find h k with Not_found -> add h k

  let read_exactly c n =
    let s = String.create n in
      Lwt_io.read_into_exactly c.ich s 0 n >>
      return s

  (* the chunk of the region needed to perform a given request --- allows to
   * impose diff limits per type *)
  let request_slot_cost = function
      Load _ -> 100
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
    let pp_col fmt col =
      Format.fprintf fmt
        "{ @[name: %S;@ data: %S;@ timestamp: %s@]}"
        col.name col.data
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
    if c.debug then begin
      Format.eprintf
        "Sending response for %a to %d:@\n @[%a@]@."
        pp_request_id request_id c.id pp x;
    end;
    return x

  let rec service c =
    !auto_yielder () >>
    lwt request_id, len, crc =
      match_lwt read_header c.ich with
          Header x -> return x
        | Corrupted_header ->
            (* we can't even trust the request_id, so all that's left is
             * dropping the connection *)
            raise_lwt (Error Corrupted_frame)
    in
      match_lwt
        Lwt.choose [fst c.signal_error; read_request c ~request_id len crc]
      with
          None -> service c
        | Some r ->
            ignore begin
              c.pending_reqs <- c.pending_reqs + 1;
              try_lwt
                Lwt_util.run_in_region c.async_req_region
                  (request_slot_cost r)
                  (fun () ->
                     begin try_lwt
                       relay_to_handler c ~request_id r
                     with
                       | End_of_file
                       | Unix.Unix_error((Unix.ECONNRESET | Unix.EPIPE), _, _) ->
                           (* catch exns that indicate that the connection
                            * is gone, and signal End_of_file *)
                           begin try
                             Lwt.wakeup_exn (snd c.signal_error) End_of_file
                           with _ -> ()
                           end;
                           return ()
                       | e ->
                           Format.eprintf
                             "Internal error %s@\n\
                              request:@\n\
                              %a@."
                             (Printexc.to_string e)
                             Request.pp r;
                           try_lwt
                             P.internal_error c.och ~request_id ()
                           with _ -> return ()
                     end)
              finally
                c.pending_reqs <- c.pending_reqs - 1;
                if c.pending_reqs = 0 then
                  Lwt_condition.broadcast c.wait_for_pending_reqs ();
                return ()
            end;
            (* here we block if the allowed number of async reqs is reached
             * until one of them is done *)
            Lwt_util.run_in_region c.async_req_region 1 (fun () -> return ()) >>
            service c

  and relay_to_handler c ~request_id = function
    | List_tables { List_tables.keyspace; _ }
    | Table_size_on_disk { Table_size_on_disk.keyspace; _ }
    | Key_range_size_on_disk { Key_range_size_on_disk.keyspace; _ }
    | Begin { Begin.keyspace; _ }
    | Commit { Commit.keyspace; _ }
    | Abort { Abort.keyspace; _ }
    | Lock { Lock.keyspace; _ }
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
    | Dump { Dump.keyspace; _ }
    | Load { Load.keyspace; _ }
    | Stats { Stats.keyspace; _ }
    | Listen { Listen.keyspace; _ }
    | Unlisten { Unlisten.keyspace; _ }
    | Notify { Notify.keyspace; _ }
    | Await { Await.keyspace; _ }
    | Release_keyspace { Release_keyspace.keyspace; _ } as r ->
        begin try
          let handlers = snd (H.find c.keyspaces (ks_id_of_int keyspace)) in
            if Stack.is_empty handlers then
              respond c ~request_id r
            else begin
              let handler = Stack.top handlers in
                handler ~request_id r
            end
        with Not_found ->
          (* will usually respond with a unknown_keyspace error *)
          respond c ~request_id r
        end
    | Register_keyspace _ | Get_keyspace _ | List_keyspaces _
    | Trigger_raw_dump _ | Raw_dump_release _ | Raw_dump_list_files _
    | Raw_dump_file_digest _ as r ->
        respond c ~request_id r

  and read_request c ~request_id len crc =
    if String.length c.in_buf < len then c.in_buf <- String.create len;
    Lwt_io.read_into_exactly c.ich c.in_buf 0 len >>
    lwt crc2 = read_exactly c 4 in
    let crc2' = Obs_crc32c.substring c.in_buf 0 len in
    let gb n = Char.code c.in_buf.[n] in
    let format_id = gb 0 + (gb 1 lsl 8) + (gb 2 lsl 16) + (gb 3 lsl 24) in
      Obs_crc32c.xor crc2 crc;
      if crc2 <> crc2' then begin
        P.bad_request c.och ~request_id () >>
        return None
      end else begin
        match Obs_protocol_payload.Obs_request_serialization.of_format_id format_id with
            `Extprot -> begin
              let m =
                try Some (Extprot.Conv.deserialize Request.read ~offset:4 c.in_buf)
                with _ -> None
              in match m with
                  None -> P.bad_request c.och ~request_id () >> return None
                | Some _ as x -> return x
            end
          | `Raw -> P.bad_request c.och ~request_id () >> return None
          | `Unknown -> P.unknown_serialization c.och ~request_id () >> return None
      end

  and respond ?buf c ~request_id r =
    if c.debug then
      Format.eprintf "Got request %a from %d@\n @[%a@]@."
        pp_request_id request_id c.id Request.pp r;
    match r with
      Register_keyspace { Register_keyspace.name } ->
        lwt ks_ks = D.register_keyspace c.server.db name in
        let ks_unique_id = ks_id_of_int (D.keyspace_id ks_ks) in
        let ks =
          { ks_unique_id; ks_ks; ks_tx_key = Lwt.new_key ();
            ks_subs_stream = Lwt_stream.create ();
            ks_subscriptions = Hashtbl.create 13;
          }
        in
          H.add c.keyspaces ks_unique_id (ks, Stack.create ());
          P.return_keyspace ?buf c.och ~request_id (ks_unique_id :> int)
    | Get_keyspace { Get_keyspace.name; } -> begin
        match_lwt D.get_keyspace c.server.db name with
            None -> P.return_keyspace_maybe ?buf c.och ~request_id None
          | Some ks_ks ->
              let ks_unique_id = ks_id_of_int (D.keyspace_id ks_ks) in
              let ks =
                { ks_ks; ks_unique_id; ks_tx_key = Lwt.new_key ();
                  ks_subs_stream = Lwt_stream.create ();
                  ks_subscriptions = Hashtbl.create 13;
                }
              in
                H.add c.keyspaces ks_unique_id (ks, Stack.create ());
                P.return_keyspace_maybe ?buf c.och ~request_id
                  (Some ks_unique_id :> int option)
      end
    | Release_keyspace { Release_keyspace.keyspace } -> begin
        try_lwt
          let keyspace = fst (H.find c.keyspaces (ks_id_of_int keyspace)) in
            terminate_keyspace_subs c.server keyspace;
            return ()
        with Not_found -> return ()
        finally
          P.return_ok ?buf c.och ~request_id ()
      end
    | List_keyspaces _ ->
        D.list_keyspaces c.server.db >>=
          P.return_keyspace_list ?buf c.och ~request_id
    | List_tables { List_tables.keyspace } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             (D.list_tables ks.ks_ks :> string list Lwt.t) >>=
             P.return_table_list ?buf c.och ~request_id)
    | Table_size_on_disk { Table_size_on_disk.keyspace; table; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.table_size_on_disk ks.ks_ks table >>=
             P.return_table_size_on_disk ?buf c.och ~request_id)
    | Key_range_size_on_disk { Key_range_size_on_disk.keyspace; table; range; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.key_range_size_on_disk ks.ks_ks table
               ?first:range.first ?up_to:range.up_to >>=
             P.return_key_range_size_on_disk ?buf c.och ~request_id)
    | Begin { Begin.keyspace; tx_type; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             let is_outermost, parent_tx_data, tx_data =
               match Lwt.get ks.ks_tx_key with
                   None ->
                     let d = { tx_notifications = Notif_queue.empty } in
                       (true, d, d)
                 | Some d -> (false, d, { tx_notifications = d.tx_notifications; }) in
             let transaction_f = match tx_type with
                 Tx_type.Repeatable_read -> D.repeatable_read_transaction
               | Tx_type.Read_committed -> D.read_committed_transaction in
             let handlers = snd (H.find c.keyspaces (ks_id_of_int keyspace)) in
               try_lwt
                 (* we'll put the request_id from the request that forced the
                  * commit (carried by Commit_exn) in finishing_reqid *)
                 let finishing_reqid = ref "" in
                 lwt () =
                   transaction_f ks.ks_ks
                     (fun _ ->
                        Lwt.with_value ks.ks_tx_key (Some tx_data)
                          (fun () ->
                             try_lwt
                               (* install handler *)
                               let req_stream, pushf = Lwt_stream.create () in
                               let handler ~request_id r =
                                 (* we keep a ref to the stream because
                                  * otherwise it can be collected! pushf
                                  * only has a weak ref to it *)
                                 ignore req_stream;
                                 pushf (Some (`Req (request_id, r)));
                                 return ()
                               in
                                 Stack.push handler handlers;
                                 P.return_ok ?buf c.och ~request_id () >>
                                 let rec handle_reqs () =
                                   match_lwt Lwt_stream.get req_stream with
                                       (Some `Req (request_id, r)) ->
                                         (* we respond in a background thread
                                          * so as to process reqs in parallel
                                          * but need to capture exns (in
                                          * particular, Abort_exn and
                                          * Commit_exn) *)
                                         ignore begin
                                           try_lwt
                                             respond c ~request_id r
                                           with e ->
                                             pushf (Some (`Exn e));
                                             return ()
                                         end;
                                         handle_reqs ()
                                     | Some (`Exn e) ->
                                         raise_lwt e
                                     | None -> return ()
                                 in handle_reqs () >>
                                    return ()
                             with Commit_exn req_id ->
                               finishing_reqid := req_id;
                               return ()
                             finally
                               let _ : req_handler = Stack.pop handlers in
                                 return ())) >>
                     P.return_ok ?buf c.och ~request_id:!finishing_reqid ()
                 in
                   (* we deliver notifications _after_ actual commit
                    * (otherwise, new data wouldn't be found if another client
                    *  performs a query right away) *)
                   begin if is_outermost then
                     Notif_queue.iter (notify c.server ks.ks_ks) tx_data.tx_notifications
                   else parent_tx_data.tx_notifications <- tx_data.tx_notifications
                   end;
                   return ()
               with Abort_exn request_id ->
                   P.return_ok ?buf c.och ~request_id ())
    | Commit { Commit.keyspace; _ } ->
        (* only commit if we're inside a tx *)
        with_keyspace c keyspace ~request_id
          (fun ks ->
             begin match Lwt.get ks.ks_tx_key with
                 None -> P.return_ok ?buf c.och ~request_id ()
               | Some _ -> raise_lwt (Commit_exn request_id)
             end)
    | Abort { Abort.keyspace; _ } ->
        (* only abort if we're inside a tx *)
        with_keyspace c keyspace ~request_id
          (fun ks ->
             begin match Lwt.get ks.ks_tx_key with
                 None -> P.return_ok ?buf c.och ~request_id ()
               | Some _ -> raise_lwt (Abort_exn request_id)
             end)
    | Lock { Lock.keyspace; names; shared; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             try_lwt
               D.lock ks.ks_ks ~shared names >>
               P.return_ok ?buf c.och ~request_id ()
             with Error Deadlock ->
               P.deadlock ?buf c.och ~request_id () >>
               raise_lwt (Abort_exn request_id))
    | Get_keys { Get_keys.keyspace; table; max_keys; key_range; } ->
        with_keyspace c keyspace ~request_id
          (fun ks -> D.get_keys ks.ks_ks table ?max_keys (krange' key_range) >>=
                     P.return_keys ?buf c.och ~request_id)
    | Exist_keys { Exist_keys.keyspace; table; keys; } ->
        with_keyspace c keyspace ~request_id
          (fun ks -> D.exist_keys ks.ks_ks table keys >>=
                     P.return_exist_result ?buf c.och ~request_id)
    | Count_keys { Count_keys.keyspace; table; key_range; } ->
        with_keyspace c keyspace ~request_id
          (fun ks -> D.count_keys ks.ks_ks table (krange' key_range) >>=
                     P.return_key_count ?buf c.och ~request_id)
    | Get_slice { Get_slice.keyspace; table; max_keys; max_columns;
                  decode_timestamps; key_range; predicate; column_range; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.get_slice ks.ks_ks table ?max_keys ?max_columns ~decode_timestamps
               (krange' key_range) ?predicate (crange' column_range) >>=
             maybe_pp c pp_slice ~request_id >>=
             P.return_slice ?buf c.och ~request_id)
    | Get_slice_values { Get_slice_values.keyspace; table; max_keys;
                         key_range; columns; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.get_slice_values ks.ks_ks table ?max_keys (krange' key_range) columns >>=
             P.return_slice_values ?buf c.och ~request_id)
    | Get_slice_values_timestamps
        { Get_slice_values_timestamps.keyspace; table; max_keys; key_range; columns; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.get_slice_values_with_timestamps ks.ks_ks table ?max_keys
               (krange' key_range) columns >>=
             P.return_slice_values_timestamps ?buf c.och ~request_id)
    | Get_columns { Get_columns.keyspace; table; max_columns;
                    decode_timestamps; key; column_range; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.get_columns ks.ks_ks table ?max_columns ~decode_timestamps
               key (crange' column_range) >>=
             P.return_columns ?buf c.och ~request_id)
    | Get_column_values { Get_column_values.keyspace; table; key; columns; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.get_column_values ks.ks_ks table key columns >>=
             P.return_column_values ?buf c.och ~request_id)
    | Get_column { Get_column.keyspace; table; key; column; } ->
        with_keyspace c keyspace ~request_id
          (fun ks -> D.get_column ks.ks_ks table key column >>=
                     P.return_column ?buf c.och ~request_id)
    | Put_columns { Put_columns.keyspace; table; data; } ->
        with_keyspace c keyspace ~request_id
          (fun ks -> D.put_multi_columns ks.ks_ks table data >>=
                     P.return_ok ?buf c.och ~request_id)
    | Delete_columns { Delete_columns.keyspace; table; key; columns; } ->
        with_keyspace c keyspace ~request_id
          (fun ks -> D.delete_columns ks.ks_ks table key columns >>=
                     P.return_ok ?buf c.och ~request_id)
    | Delete_key { Delete_key.keyspace; table; key; } ->
        with_keyspace c keyspace ~request_id
          (fun ks -> D.delete_key ks.ks_ks table key >>=
                     P.return_ok ?buf c.och ~request_id)
    | Dump { Dump.keyspace; only_tables; cursor; format; } ->
        let offset = match cursor with
            None -> None
          | Some c -> D.cursor_of_string c in
        with_keyspace c keyspace ~request_id
          (fun ks ->
             lwt x =
               D.dump ks.ks_ks ?format ?only_tables ?offset () >|= function
                   None -> None
                 | Some (data, None) -> Some (data, None)
                 | Some (data, Some cursor) ->
                     Some (data, Some (D.string_of_cursor cursor))
             in P.return_backup_dump ?buf c.och ~request_id x)
    | Load { Load.keyspace; data; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.load ks.ks_ks data >>=
             P.return_backup_load_result ?buf c.och ~request_id)
    | Stats { Stats.keyspace; } ->
        with_keyspace c keyspace ~request_id
          (fun ks -> D.load_stats ks.ks_ks >>=
                     P.return_load_stats ?buf c.och ~request_id)
    | Listen { Listen.keyspace; topic; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             let ks_name = D.keyspace_name ks.ks_ks in
             let k = { subs_ks = ks_name; subs_topic = topic} in
             let tbl =
               find_or_add
                 Hashtbl.find
                 (fun h k -> let v = H.create 13 in Hashtbl.add h k v; v)
                 c.server.subscriptions k
             in
               if not (H.mem tbl ks.ks_unique_id) then begin
                 Hashtbl.replace ks.ks_subscriptions topic ();
                 H.add tbl ks.ks_unique_id ks.ks_subs_stream;
               end;
               P.return_ok ?buf c.och ~request_id ())
    | Unlisten { Unlisten.keyspace; topic; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             let ks_name = D.keyspace_name ks.ks_ks in
             let k = { subs_ks = ks_name; subs_topic = topic} in
               begin try
                 let tbl = Hashtbl.find c.server.subscriptions k in
                   H.remove tbl ks.ks_unique_id;
                   (* if there are no more listeners, remove the empty
                    * subscriber table *)
                   if H.length tbl = 0 then
                     Hashtbl.remove c.server.subscriptions k;
                   Hashtbl.remove ks.ks_subscriptions ks_name;
               with Not_found -> () end;
               P.return_ok ?buf c.och ~request_id ())
    | Notify { Notify.keyspace; topic; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             begin match Lwt.get ks.ks_tx_key with
                 None -> notify c.server ks.ks_ks topic
               | Some tx_data ->
                   tx_data.tx_notifications <-
                     Notif_queue.push topic tx_data.tx_notifications;
             end;
             P.return_ok ?buf c.och ~request_id ())
    | Await { Await.keyspace; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             let attempt () =
               let stream = fst ks.ks_subs_stream in
               (* we use get_available_up_to to limit the stack footprint
                * (get_available is not tail-recursive) *)
               lwt l = match Lwt_stream.get_available_up_to 500 stream with
                   [] -> begin
                     match_lwt Lwt_stream.get stream with
                         None -> return []
                       | Some x -> return [x]
                   end
                 | l -> return l
               in P.return_notifications ?buf c.och ~request_id l in
             (* if there's no subscription yet, wait until there's one *)
             (* TODO: use a condition to wait for a subscription without
              * polling *)
             let rec retry_await_if_needed () =
               try_lwt
                 attempt ()
               with Not_found ->
                 Lwt_unix.sleep 0.05 >>
                 retry_await_if_needed ()
             in retry_await_if_needed ())
    | Trigger_raw_dump { Trigger_raw_dump.record; } ->
      (* FIXME: catch errors in Raw_dump.dump, signal to client *)
      lwt raw_dump = D.Raw_dump.dump c.server.db in
      lwt timestamp = D.Raw_dump.timestamp raw_dump in
      let dump_id = c.server.raw_dump_seqno in
        c.server.raw_dump_seqno <- Int64.add 1L dump_id;
        Hashtbl.add c.server.raw_dumps dump_id raw_dump;
        P.return_raw_dump_id_and_timestamp ?buf c.och ~request_id
          (dump_id, timestamp)
    | Raw_dump_release { Raw_dump_release.id } ->
        with_raw_dump c id ()
          (fun raw_dump ->
             Hashtbl.remove c.server.raw_dumps id;
             D.Raw_dump.release raw_dump) >>=
        P.return_ok ?buf c.och ~request_id
    | Raw_dump_list_files { Raw_dump_list_files.id } ->
        with_raw_dump c id [] D.Raw_dump.list_files >>=
        P.return_raw_dump_files ?buf c.och ~request_id
    | Raw_dump_file_digest { Raw_dump_file_digest.id; file; } ->
        with_raw_dump c id None
          (fun d -> D.Raw_dump.file_digest d file) >>=
        P.return_raw_dump_file_digest ?buf c.och ~request_id

  and notify server ks topic =
    let ks_name = D.keyspace_name ks in
    let k = { subs_ks = ks_name; subs_topic = topic; } in
      begin try
        let subs = Hashtbl.find server.subscriptions k in
          H.iter (fun _ (_, pushf) -> pushf (Some topic)) subs
      with _ -> () end

  and with_keyspace c ks_idx ~request_id f =
    let ks =
      try Some (fst (H.find c.keyspaces (ks_id_of_int ks_idx)))
      with Not_found -> None
    in
      match ks with
          None -> P.unknown_keyspace c.och ~request_id ()
        | Some ks -> f ks

  and wait_for_pending_reqs c =
    if c.pending_reqs = 0 then
      return ()
    else begin
      Lwt_condition.wait c.wait_for_pending_reqs
    end

  and terminate_keyspace_subs server ks =
    let empty_topics =
      snd ks.ks_subs_stream None;
      let subs_ks = D.keyspace_name ks.ks_ks in
        Hashtbl.fold
          (fun subs_topic _ l ->
             let k =  { subs_ks; subs_topic; } in
             try
               let t = Hashtbl.find server.subscriptions k in
                 H.remove t ks.ks_unique_id;
                 if H.length t = 0 then k :: l
                 else l
             with Not_found -> l)
          ks.ks_subscriptions []
    in List.iter (Hashtbl.remove server.subscriptions) empty_topics

  let setup_auto_yield t c =
    incr num_clients;
    if !num_clients >= 2 then begin
      D.use_thread_pool t.db true;
      let f = Lwt_unix.auto_yield 0.5e-3 in
        auto_yielder := f
    end;
    let db = t.db in
      Gc.finalise
        (fun _ ->
           decr num_clients;
           if !num_clients <= 1 then begin
             D.use_thread_pool db false;
             auto_yielder := dummy_auto_yield
           end)
        c

  let make ?(replication_wait = Await_commit) db =
    {
      db;
      subscriptions = Hashtbl.create 13;
      raw_dump_seqno = 0L;
      raw_dumps = Hashtbl.create 13;
      replication_wait;
    }

  let client_id = ref 0

  let service_client server ?(max_async_reqs = 5000) ?(debug=false) ich och =
    let c =
      {
        id = (incr client_id; !client_id);
        keyspaces = H.create 13;
        ich; och; server; debug;
        out_buf = Obs_bytea.create 1024;
        in_buf = String.create 128;
        pending_reqs = 0;
        wait_for_pending_reqs = Lwt_condition.create ();
        async_req_region = Lwt_util.make_region max_async_reqs;
        signal_error = Lwt.task ();
      }
    in setup_auto_yield server c;
       try_lwt
         service c
       with Unix.Unix_error (Unix.ECONNRESET, _, _) ->
         raise_lwt End_of_file
       finally
         H.iter (fun _ (ks, _) -> terminate_keyspace_subs server ks) c.keyspaces;
         return ()

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
                send_response_code `OK och >>
                let buf = String.create 16384 in
                let rec loop_copy_data () =
                  match_lwt Lwt_io.read_into ic buf 0 16384 with
                      0 -> return ()
                    | n -> Lwt_io.write_from_exactly och buf 0 n >>
                           loop_copy_data ()
                in loop_copy_data ()
      with Not_found -> send_response_code `Unknown_dump och

  let rec write_and_update_crc crc och buf off len =
    if len <= 0 then return ()
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
                  if debug then
                    eprintf "Sending update (%d bytes)\n%!" len;
                  write_checksummed_int64_le och (Int64.of_int len) >>

                  let rec copy_data () =
                    write_and_update_crc crc och buf off len >>
                    Lwt_io.write och (Obs_crc32c.unsafe_result crc) >>
                    Lwt_io.flush och >>
                    match_lwt read_checksummed_int ich with
                        None -> raise_lwt Corrupted_data_header
                      | Some 0 -> return ()
                      | Some (1 | _) -> copy_data ()
                  in
                    Obs_crc32c.reset crc;
                    copy_data () >>
                    match server.replication_wait with
                        Await_reception ->
                          D.Replication.ack_update update >>
                          lwt _ = read_checksummed_int ich in
                            return ()
                      | Await_commit ->
                          match_lwt read_checksummed_int ich with
                              Some 0 -> D.Replication.ack_update update
                            | Some (1 | _) -> D.Replication.nack_update update
                            | None -> raise_lwt Corrupted_data_header
              with exn ->
                D.Replication.nack_update update >>
                (* release no longer needed dump resources (e.g. files) *)
                D.Raw_dump.release dump >>
                raise_lwt exn
              end >>
              forward_updates ()
      in
        send_response_code `OK och >>
        forward_updates ()
    with Not_found -> send_response_code `Unknown_dump och

  let service_data_client server ?(debug=false) ich och =
    try_lwt
      lwt (major, minor, bugfix) = data_conn_handshake ich och in
      match_lwt read_checksummed_int ich with
          None -> raise_lwt Corrupted_data_header
        | Some req_len ->
            lwt req = Obs_protocol.read_exactly ich req_len in
            lwt req_crc = Obs_protocol.read_exactly ich 4 in
              if Obs_crc32c.string req <> req_crc then raise Corrupted_data_header;
              let req_ch = Lwt_io.of_string Lwt_io.input req in
                match_lwt Lwt_io.LE.read_int req_ch >|= data_request_of_code with
                    `Get_file -> handle_get_file ~debug server req_ch ich och
                  | `Get_updates -> handle_get_updates ~debug server req_ch ich och
                  | _ -> return ()
        with Unix.Unix_error (Unix.ECONNRESET, _, _) ->
          raise_lwt End_of_file

end

