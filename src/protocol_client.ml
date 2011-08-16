open Lwt
open Data_model
open Request
open Request

module Make(P : Protocol.PAYLOAD) =
struct
  let byte s n = Char.code s.[n]

  module H =
    Hashtbl.Make(struct
                   type t = string

                   let equal s1 s2 =
                     let len1 = String.length s1 in
                     let len2 = String.length s2 in
                       len1 = len2 && String_util.strneq s1 0 s2 0 len1

                   let hash s =
                     (byte s 0 lsl 0) lor
                     (byte s 1 lsl 4) lor
                     (byte s 2 lsl 8) lor
                     (byte s 3 lsl 12) lor
                     (byte s 4 lsl 16) lor
                     (byte s 5 lsl 20) lor
                     (byte s 6 lsl 24) lor
                     (byte s 7 lsl 30)
                 end)

  type payload_size = int
  type crc = string

  module type PENDING_RESPONSE =
  sig
    type result
    val read_result : Lwt_io.input_channel -> result Lwt.t
    val wakeup : result Lwt.u
  end

  type ret = [`OK | `EXN of exn]

  type db = {
    mutable closed : bool;
    (* exception raised when we try to perform further requests against a
     * closed db *)
    mutable closed_exn : exn;
    ich : Lwt_io.input_channel;
    och : Lwt_io.output_channel;
    buf : Bytea.t;
    mutex : Lwt_mutex.t;
    pending_reqs : (module PENDING_RESPONSE) H.t;
    wait_for_pending_responses : ret Lwt_condition.t;
    async_req_id : string;
  }

  type keyspace = { ks_name : string; ks_id : int; ks_db : db; }

  type transaction = keyspace

  type backup_cursor = string

  let try_find h k =
    try Some (H.find h k) with Not_found -> None

  let wakeup_safe u x = try Lwt.wakeup u x with _ -> ()
  let wakeup_exn_safe u exn = try Lwt.wakeup_exn u exn with _ -> ()

  let send_exn_to_waiters t exn =
    H.iter
      (fun _ req ->
         let module M = (val req : PENDING_RESPONSE) in
           wakeup_exn_safe M.wakeup exn)
      t.pending_reqs

  let close t =
    if not t.closed then begin
      t.closed <- true;
      ignore (try_lwt Lwt_io.abort t.ich >> Lwt_io.abort t.och with _ -> return ());
      send_exn_to_waiters t (Protocol.Error Protocol.Closed);
      H.clear t.pending_reqs;
    end

  let read_exactly ich n =
    let s = String.create n in
      Lwt_io.read_into_exactly ich s 0 n >>
      return s

  let rec get_response_loop t =
    lwt request_id, len, crc = Protocol.read_header t.ich in
    let pos = Lwt_io.position t.ich in
    let receiver = try_find t.pending_reqs request_id in
      match receiver with
          None ->
            (* skip response *)
            Protocol.skip t.ich (len + 4)
        | Some r ->
            let module R = (val r : PENDING_RESPONSE) in
            (* must read the trailing CRC even if there's an exn in f, lest we
             * lose synchronism *)
            lwt result =
              try_lwt
                lwt x = R.read_result t.ich in
                  return (`OK x)
              with e -> return (`EXN e) in
            let pos2 = Lwt_io.position t.ich in
            lwt crc2 = read_exactly t.ich 4 in
            let len' = Int64.(to_int (sub pos2 pos)) in
              if len' = len then begin
                begin match result with
                    `OK x ->
                      (* FIXME: should check CRC2 = CRC(payload) XOR CRC1 *)
                      H.remove t.pending_reqs request_id;
                      wakeup_safe R.wakeup x
                  | `EXN e ->
                      H.remove t.pending_reqs request_id;
                      wakeup_exn_safe R.wakeup e
                end;
                (* signal that we no longer have any pending responses *)
                if H.length t.pending_reqs = 0 then
                  Lwt_condition.broadcast t.wait_for_pending_responses `OK

              end else begin
                (* wrong length *)
                wakeup_exn_safe R.wakeup
                  (Protocol.Error
                     (Protocol.Inconsistent_length (len, len')));
                (* and we close the conn *)
                close t
              end;
              get_response_loop t

  let make ich och =
    let t =
      { ich; och; buf = Bytea.create 64;
        closed = false; closed_exn = Protocol.Error Protocol.Closed;
        mutex = Lwt_mutex.create (); pending_reqs = H.create 13;
        async_req_id = "\001\000\000\000\000\000\000\000";
        wait_for_pending_responses = Lwt_condition.create ();
      }
    in
      ignore begin try_lwt
        get_response_loop t
      with e ->
        let exn = Protocol.Error (Protocol.Exception e) in
          send_exn_to_waiters t exn;
          t.closed_exn <- exn;
          close t;
          Lwt_condition.broadcast t.wait_for_pending_responses (`EXN exn);
          return ()
      end;
      t

  let check_closed t =
    if t.closed then raise_lwt t.closed_exn
    else return ()

  let send_request t ~request_id req =
    Bytea.clear t.buf;
    Request.write (t.buf :> Extprot.Msg_buffer.t) req;
    Protocol.write_msg t.och request_id t.buf

  let sync_get_response (type a) t (f : Lwt_io.input_channel -> a Lwt.t) =
    let wait, wakeup = Lwt.task () in
    let module R =
      struct
        type result = a
        let read_result = f
        let wakeup = wakeup
      end
    in H.replace t.pending_reqs Protocol.sync_req_id (module R : PENDING_RESPONSE);
       wait

  let sync_request t req f =
    check_closed t >>
    Lwt_mutex.with_lock t.mutex
      (fun () -> send_request t ~request_id:Protocol.sync_req_id req >>
                 sync_get_response t f)

  let incr_async_req_id t =
    let rec loop_incr s n =
      if n >= String.length s then begin
        for i = 1 to String.length s - 1 do
          s.[i] <- '\000';
        done;
        s.[0] <- '\001';
      end else begin match byte s n with
        | m when m <> 255 -> s.[n] <- Char.unsafe_chr (m+1)
        | _ (* 255 *) -> s.[n] <- '\000'; loop_incr s (n+1)
      end
    in loop_incr t.async_req_id 0

  let new_async_req_id t =
    incr_async_req_id t;
    String.copy t.async_req_id

  let async_request (type a) t req f =
    check_closed t >>
    let wait, wakeup = Lwt.task () in
    let module R =
      struct
        type result = a
        let read_result = f
        let wakeup = wakeup
      end in
    let request_id = new_async_req_id t in
      H.replace t.pending_reqs request_id (module R : PENDING_RESPONSE);
      Lwt_mutex.with_lock t.mutex (fun () -> send_request t ~request_id req) >>
      wait

  let list_keyspaces t =
    sync_request t
      (List_keyspaces { List_keyspaces.prefix = "" })
      P.read_keyspace_list

  let register_keyspace t name =
    lwt ks_id =
      sync_request t
        (Register_keyspace { Register_keyspace.name; })
        P.read_keyspace
    in return { ks_id; ks_name = name; ks_db = t; }

  let get_keyspace t name =
    match_lwt
      sync_request t (Get_keyspace { Get_keyspace.name; }) P.read_keyspace_maybe
    with
        None -> return None
      | Some ks_id -> return (Some { ks_id; ks_name = name; ks_db = t; })

  let keyspace_name ks = ks.ks_name
  let keyspace_id ks = ks.ks_id

  let sync_request_ks ks req f = sync_request ks.ks_db req f
  let async_request_ks ks req f = async_request ks.ks_db req f

  let list_tables ks =
    sync_request_ks ks (List_tables { List_tables.keyspace = ks.ks_id; }) P.read_table_list

  let table_size_on_disk ks table =
    sync_request_ks ks
      (Table_size_on_disk { Table_size_on_disk.keyspace = ks.ks_id; table; })
      P.read_table_size_on_disk

  let key_range_size_on_disk ks ?first ?up_to table =
    sync_request_ks ks
      (Key_range_size_on_disk
         { Key_range_size_on_disk.keyspace = ks.ks_id; table;
           range = { first; up_to; } }; )
      P.read_key_range_size_on_disk

  let wait_for_pending_responses t =
    check_closed t >>
    if H.length t.pending_reqs <> 0 then begin
      match_lwt Lwt_condition.wait t.wait_for_pending_responses with
          `OK -> return ()
        | `EXN e -> raise_lwt e
    end else return ()

  let read_committed_transaction ks f =
    wait_for_pending_responses ks.ks_db >>
    sync_request_ks ks (Begin { Begin.keyspace = ks.ks_id }) P.read_ok >>
    try_lwt
      lwt y = f ks in
        wait_for_pending_responses ks.ks_db >>
        sync_request_ks ks (Commit { Commit.keyspace = ks.ks_id }) P.read_ok >>
        return y
    with e ->
      wait_for_pending_responses ks.ks_db >>
      sync_request_ks ks (Abort { Abort.keyspace = ks.ks_id }) P.read_ok >>
      raise_lwt e

  let repeatable_read_transaction = read_committed_transaction

  let get_keys ks table ?max_keys key_range =
    sync_request_ks ks
      (Get_keys { Get_keys.keyspace = ks.ks_id; table; max_keys; key_range; })
      P.read_keys

  let count_keys ks table key_range =
    sync_request_ks ks
      (Count_keys { Count_keys.keyspace = ks.ks_id; table; key_range; })
      P.read_key_count

  let get_slice ks table ?max_keys ?max_columns ?(decode_timestamps=false)
    key_range column_range =
    sync_request_ks ks
      (Get_slice { Get_slice.keyspace = ks.ks_id; table;
                   max_keys; max_columns; decode_timestamps;
                   key_range; column_range })
      P.read_slice

  let get_slice_values ks table ?max_keys key_range columns =
    sync_request_ks ks
      (Get_slice_values { Get_slice_values.keyspace = ks.ks_id; table;
                          max_keys; key_range; columns; })
      P.read_slice_values

  let get_columns ks table ?max_columns ?(decode_timestamps=false)
        key column_range =
    sync_request_ks ks
      (Get_columns { Get_columns.keyspace = ks.ks_id; table;
                     max_columns; decode_timestamps; key; column_range; })
      P.read_columns

  let get_column_values ks table key columns =
    sync_request_ks ks
      (Get_column_values { Get_column_values.keyspace = ks.ks_id; table;
                           key; columns; })
      P.read_column_values

  let get_column ks table key column =
    sync_request_ks ks
      (Get_column { Get_column.keyspace = ks.ks_id; table; key; column; })
      P.read_column

  let put_columns ks table key columns =
    async_request_ks ks
      (Put_columns { Put_columns.keyspace = ks.ks_id; table; key; columns; })
      P.read_ok

  let delete_columns ks table key columns =
    sync_request_ks ks
      (Delete_columns { Delete_columns.keyspace = ks.ks_id; table; key; columns; })
      P.read_ok

  let delete_key ks table key =
    sync_request_ks ks
      (Delete_key { Delete_key.keyspace = ks.ks_id; table; key; })
      P.read_ok

  let dump ks ?format ?only_tables ?offset () =
    async_request_ks ks
      (Dump { Dump.keyspace = ks.ks_id; only_tables; cursor = offset;
              format; })
      P.read_backup_dump

  let load ks data =
    async_request_ks ks (Load { Load.keyspace = ks.ks_id; data; })
      P.read_backup_load_result

  let string_of_cursor x = x
  let cursor_of_string x = Some x
end
