(*
 * Copyright (C) 2011 Mauricio Fernandez <mfp@acm.org>
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
open Obigstore_core
open Data_model
open Protocol
open Request

module Make
  (D : sig
     include Data_model.S
     include Data_model.BACKUP_SUPPORT with type backup_cursor := backup_cursor
     val use_thread_pool : db -> bool -> unit
   end)
  (P : PAYLOAD) =
struct
  open Request

  exception Abort_exn
  exception Commit_exn

  module H = Hashtbl.Make(struct
                            type t = int
                            let hash n = n
                            let equal a b = (a == b)
                          end)

  let num_clients = ref 0

  let dummy_auto_yield () = return ()

  let auto_yielder = ref dummy_auto_yield

  type client_state =
      {
        keyspaces : D.keyspace H.t;
        rev_keyspaces : int H.t;
        ich : Lwt_io.input_channel;
        och : Lwt_io.output_channel;
        db : D.db;
        mutable in_buf : string;
        out_buf : Bytea.t;
        debug : bool;
        mutable pending_reqs : int;
        wait_for_pending_reqs : unit Lwt_condition.t;
        async_req_region : Lwt_util.region;
      }

  let tx_key = Lwt.new_key ()

  let setup_auto_yield t =
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
        t

  let init ?(max_async_reqs = 5000) ?(debug=false) db ich och =
    let t =
      {
        keyspaces = H.create 13;
        rev_keyspaces = H.create 13;
        ich; och; db; debug;
        out_buf = Bytea.create 1024;
        in_buf = String.create 128;
        pending_reqs = 0;
        wait_for_pending_reqs = Lwt_condition.create ();
        async_req_region = Lwt_util.make_region max_async_reqs;
      }
    in setup_auto_yield t;
       t

  let read_exactly c n =
    let s = String.create n in
      Lwt_io.read_into_exactly c.ich s 0 n >>
      return s

  (* the chunk of the region needed to perform a given request --- allows to
   * impose diff limits per type *)
  let request_slot_cost = function
      Load _ -> 100
    | _ -> 1

  let rec service c =
    !auto_yielder () >>
    lwt request_id, len, crc = read_header c.ich in
      match_lwt read_request c ~request_id len crc with
          None -> service c
        | Some r ->
            if Protocol.is_sync_req request_id then begin
              begin try_lwt
                respond c ~buf:c.out_buf ~request_id r
              with
                | Abort_exn | Commit_exn | End_of_file as e -> raise_lwt e
                (* catch exns that indicate that the connection is gone,
                 * and signal End_of_file *)
                | Unix.Unix_error((Unix.ECONNRESET | Unix.EPIPE), _, _) ->
                    raise_lwt End_of_file
                | e ->
                Format.eprintf
                  "Internal error %s@\n\
                   request:@\n\
                   %a@."
                  (Printexc.to_string e)
                  Request.pp r;
                P.internal_error c.och ~request_id ()
              end >>
              service c
            end else begin
              ignore begin
                c.pending_reqs <- c.pending_reqs + 1;
                try_lwt
                  Lwt_util.run_in_region c.async_req_region
                    (request_slot_cost r)
                    (fun () -> respond c ~request_id r)
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
            end

  and read_request c ~request_id len crc =
    if String.length c.in_buf < len then c.in_buf <- String.create len;
    Lwt_io.read_into_exactly c.ich c.in_buf 0 len >>
    lwt crc2 = read_exactly c 4 in
    let crc2' = Crc32c.substring c.in_buf 0 len in
    let gb n = Char.code c.in_buf.[n] in
    let format_id = gb 0 + (gb 1 lsl 8) + (gb 2 lsl 16) + (gb 3 lsl 24) in
      Crc32c.xor crc2 crc;
      if crc2 <> crc2' then begin
        P.bad_request c.och ~request_id () >>
        return None
      end else begin
        match Protocol_payload.Request_serialization.of_format_id format_id with
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
    if c.debug then Format.eprintf "Got request %a@." Request.pp r;
    match r with
      Register_keyspace { Register_keyspace.name } ->
        lwt ks = D.register_keyspace c.db name in
        let idx =
          (* find keyspace idx in local table, register if not found *)
          try
            H.find c.rev_keyspaces (D.keyspace_id ks)
          with Not_found ->
            let idx = H.length c.keyspaces in
              H.add c.keyspaces idx ks;
              H.add c.rev_keyspaces (D.keyspace_id ks) idx;
              idx
        in
          P.return_keyspace ?buf c.och ~request_id idx
    | Get_keyspace { Get_keyspace.name; } -> begin
        match_lwt D.get_keyspace c.db name with
            None -> P.return_keyspace_maybe ?buf c.och ~request_id None
          | Some ks ->
              let idx =
                (* find keyspace idx in local table, register if not found *)
                try
                  H.find c.rev_keyspaces (D.keyspace_id ks)
                with Not_found ->
                  let idx = H.length c.keyspaces in
                    H.add c.keyspaces idx ks;
                    H.add c.rev_keyspaces (D.keyspace_id ks) idx;
                    idx
              in
                P.return_keyspace_maybe ?buf c.och ~request_id
                  (Some idx)
      end
    | List_keyspaces _ ->
        D.list_keyspaces c.db >>=
          P.return_keyspace_list ?buf c.och ~request_id
    | List_tables { List_tables.keyspace } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.list_tables ks >>=
             P.return_table_list ?buf c.och ~request_id)
    | Table_size_on_disk { Table_size_on_disk.keyspace; table; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.table_size_on_disk ks table >>=
             P.return_table_size_on_disk ?buf c.och ~request_id)
    | Key_range_size_on_disk { Key_range_size_on_disk.keyspace; table; range; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.key_range_size_on_disk ks table
               ?first:range.first ?up_to:range.up_to >>=
             P.return_key_range_size_on_disk ?buf c.och ~request_id)
    | Begin { Begin.keyspace; } ->
        (* FIXME: check if we have an open tx in another ks, and signal error
         * if so *)
        wait_for_pending_reqs c >>
        with_keyspace c keyspace ~request_id
          (fun ks ->
             try_lwt
               D.repeatable_read_transaction ks
                 (fun ks ->
                    Lwt.with_value tx_key (Some ())
                      (fun () ->
                         try_lwt
                           P.return_ok ?buf c.och ~request_id ()>>
                           service c
                         with Commit_exn ->
                           wait_for_pending_reqs c >>
                           P.return_ok ?buf c.och ~request_id ()))
             with Abort_exn ->
               wait_for_pending_reqs c >>
               P.return_ok ?buf c.och ~request_id ())
    | Commit _ ->
        (* only commit if we're inside a tx *)
        begin match Lwt.get tx_key with
            None -> P.return_ok ?buf c.och ~request_id ()
          | Some _ -> raise_lwt Commit_exn
        end
    | Abort _ ->
        (* only abort if we're inside a tx *)
        begin match Lwt.get tx_key with
            None -> P.return_ok ?buf c.och ~request_id ()
          | Some _ -> raise_lwt Abort_exn
        end
    | Lock { Lock.keyspace; name; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             try_lwt
               D.lock ks name >>
               P.return_ok ?buf c.och ~request_id ()
             with Error Deadlock ->
               P.deadlock ?buf c.och ~request_id () >>
               raise_lwt Abort_exn)
    | Get_keys { Get_keys.keyspace; table; max_keys; key_range; } ->
        with_keyspace c keyspace ~request_id
          (fun ks -> D.get_keys ks table ?max_keys key_range >>=
                     P.return_keys ?buf c.och ~request_id)
    | Count_keys { Count_keys.keyspace; table; key_range; } ->
        with_keyspace c keyspace ~request_id
          (fun ks -> D.count_keys ks table key_range >>=
                     P.return_key_count ?buf c.och ~request_id)
    | Get_slice { Get_slice.keyspace; table; max_keys; max_columns;
                  decode_timestamps; key_range; predicate; column_range; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.get_slice ks table ?max_keys ?max_columns ~decode_timestamps
               key_range ?predicate column_range >>=
             P.return_slice ?buf c.och ~request_id)
    | Get_slice_values { Get_slice_values.keyspace; table; max_keys;
                         key_range; columns; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.get_slice_values ks table ?max_keys key_range columns >>=
             P.return_slice_values ?buf c.och ~request_id)
    | Get_columns { Get_columns.keyspace; table; max_columns;
                    decode_timestamps; key; column_range; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.get_columns ks table ?max_columns ~decode_timestamps
               key column_range >>=
             P.return_columns ?buf c.och ~request_id)
    | Get_column_values { Get_column_values.keyspace; table; key; columns; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.get_column_values ks table key columns >>=
             P.return_column_values ?buf c.och ~request_id)
    | Get_column { Get_column.keyspace; table; key; column; } ->
        with_keyspace c keyspace ~request_id
          (fun ks -> D.get_column ks table key column >>=
                     P.return_column ?buf c.och ~request_id)
    | Put_columns { Put_columns.keyspace; table; data; } ->
        with_keyspace c keyspace ~request_id
          (fun ks -> D.put_multi_columns ks table data >>=
                     P.return_ok ?buf c.och ~request_id)
    | Delete_columns { Delete_columns.keyspace; table; key; columns; } ->
        with_keyspace c keyspace ~request_id
          (fun ks -> D.delete_columns ks table key columns >>=
                     P.return_ok ?buf c.och ~request_id)
    | Delete_key { Delete_key.keyspace; table; key; } ->
        with_keyspace c keyspace ~request_id
          (fun ks -> D.delete_key ks table key >>=
                     P.return_ok ?buf c.och ~request_id)
    | Dump { Dump.keyspace; only_tables; cursor; format; } ->
        let offset = match cursor with
            None -> None
          | Some c -> D.cursor_of_string c in
        with_keyspace c keyspace ~request_id
          (fun ks ->
             lwt x =
               D.dump ks ?format ?only_tables ?offset () >|= function
                   None -> None
                 | Some (data, None) -> Some (data, None)
                 | Some (data, Some cursor) ->
                     Some (data, Some (D.string_of_cursor cursor))
             in P.return_backup_dump ?buf c.och ~request_id x)
    | Load { Load.keyspace; data; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.load ks data >>=
             P.return_backup_load_result ?buf c.och ~request_id)
    | Stats { Stats.keyspace; } ->
        with_keyspace c keyspace ~request_id
          (fun ks -> D.load_stats ks >>=
                     P.return_load_stats ?buf c.och ~request_id)

  and with_keyspace c ks_idx ~request_id f =
    let ks = try Some (H.find c.keyspaces ks_idx) with Not_found -> None in
      match ks with
          None -> P.unknown_keyspace c.och ~request_id ()
        | Some ks -> f ks

  and wait_for_pending_reqs c =
    if c.pending_reqs = 0 then
      return ()
    else begin
      Lwt_condition.wait c.wait_for_pending_reqs
    end

  let service c =
    try_lwt
      service c
    with Unix.Unix_error (Unix.ECONNRESET, _, _) -> raise_lwt End_of_file
end

