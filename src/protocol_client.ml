open Lwt
open Data_model
open Request
open Request

module Make(P : Protocol.PAYLOAD) =
struct
  type db = {
    mutable closed : bool;
    ich : Lwt_io.input_channel;
    och : Lwt_io.output_channel;
    buf : Bytea.t;
    mutex : Lwt_mutex.t;
  }

  type keyspace = { ks_name : string; ks_id : int; ks_db : db; }

  type transaction = keyspace

  let make ich och =
    { ich; och; buf = Bytea.create 64; closed = false;
      mutex = Lwt_mutex.create ();
    }

  let close t =
    if not t.closed then
      ignore (Lwt_io.close t.ich >> Lwt_io.close t.och);
    t.closed <- true

  let send_request t req =
    Bytea.clear t.buf;
    Request.write (t.buf :> Extprot.Msg_buffer.t) req;
    Protocol.write_msg t.och Protocol.sync_req_id t.buf

  let read_exactly ich n =
    let s = String.create n in
      Lwt_io.read_into_exactly ich s 0 n >>
      return s

  let get_response f ich =
    lwt request_id, len, crc = Protocol.read_header ich in
    let pos = Lwt_io.position ich in
    (* must read the trailing CRC even if there's an exn in f, lest we
     * lose synchro *)
    lwt result =
      try_lwt
        lwt x = f ich in
          return (`OK x)
      with e -> return (`EXN e) in
    let pos2 = Lwt_io.position ich in
    lwt crc2 = read_exactly ich 4 in
      match result with
          `OK x ->
            let len' = Int64.(to_int (sub pos2 pos)) in
              if len' <> len then
                raise_lwt (Protocol.Error
                             (Protocol.Inconsistent_length (len, len')))
              else
                (* FIXME: should check CRC2 = CRC(payload) XOR CRC1 *)
                  return x
        | `EXN e -> raise_lwt e

  let request t req f =
    Lwt_mutex.with_lock t.mutex
      (fun () -> send_request t req >>
                 get_response f t.ich)

  let list_keyspaces t =
    request t
      (List_keyspaces { List_keyspaces.prefix = "" })
      P.read_keyspace_list

  let register_keyspace t name =
    lwt ks_id =
      request t
        (Register_keyspace { Register_keyspace.name; })
        P.read_keyspace
    in return { ks_id; ks_name = name; ks_db = t; }

  let get_keyspace t name =
    match_lwt
      request t (Get_keyspace { Get_keyspace.name; }) P.read_keyspace_maybe
    with
        None -> return None
      | Some ks_id -> return (Some { ks_id; ks_name = name; ks_db = t; })

  let keyspace_name ks = ks.ks_name
  let keyspace_id ks = ks.ks_id

  let request_ks ks req f = request ks.ks_db req f

  let list_tables ks =
    request_ks ks (List_tables { List_tables.keyspace = ks.ks_id; }) P.read_table_list

  let table_size_on_disk ks table =
    request_ks ks
      (Table_size_on_disk { Table_size_on_disk.keyspace = ks.ks_id; table; })
      P.read_table_size_on_disk

  let key_range_size_on_disk ks ?first ?up_to table =
    request_ks ks
      (Key_range_size_on_disk
         { Key_range_size_on_disk.keyspace = ks.ks_id; table;
           range = { first; up_to; } }; )
      P.read_key_range_size_on_disk

  let read_committed_transaction ks f =
    (* FIXME: should have a recursive mutex and lock it here *)
    send_request ks.ks_db (Begin { Begin.keyspace = ks.ks_id }) >>
    get_response P.read_ok ks.ks_db.ich >>
    try_lwt
      lwt y = f ks in
        send_request ks.ks_db (Commit { Commit.keyspace = ks.ks_id }) >>
        get_response P.read_ok ks.ks_db.ich >>
        return y
    with e ->
      send_request ks.ks_db (Abort { Abort.keyspace = ks.ks_id }) >>
      get_response P.read_ok ks.ks_db.ich >>
      raise_lwt e

  let repeatable_read_transaction = read_committed_transaction

  let get_keys ks table ?max_keys key_range =
    request_ks ks
      (Get_keys { Get_keys.keyspace = ks.ks_id; table; max_keys; key_range; })
      P.read_keys

  let count_keys ks table key_range =
    request_ks ks
      (Count_keys { Count_keys.keyspace = ks.ks_id; table; key_range; })
      P.read_key_count

  let get_slice ks table ?max_keys ?max_columns ?(decode_timestamps=false)
    key_range column_range =
    request_ks ks
      (Get_slice { Get_slice.keyspace = ks.ks_id; table;
                   max_keys; max_columns; decode_timestamps;
                   key_range; column_range })
      P.read_slice

  let get_slice_values ks table ?max_keys key_range columns =
    request_ks ks
      (Get_slice_values { Get_slice_values.keyspace = ks.ks_id; table;
                          max_keys; key_range; columns; })
      P.read_slice_values

  let get_columns ks table ?max_columns ?(decode_timestamps=false)
        key column_range =
    request_ks ks
      (Get_columns { Get_columns.keyspace = ks.ks_id; table;
                     max_columns; decode_timestamps; key; column_range; })
      P.read_columns

  let get_column_values ks table key columns =
    request_ks ks
      (Get_column_values { Get_column_values.keyspace = ks.ks_id; table;
                           key; columns; })
      P.read_column_values

  let get_column ks table key column =
    request_ks ks
      (Get_column { Get_column.keyspace = ks.ks_id; table; key; column; })
      P.read_column

  let put_columns ks table key columns =
    request_ks ks
      (Put_columns { Put_columns.keyspace = ks.ks_id; table; key; columns; })
      P.read_ok

  let delete_columns ks table key columns =
    request_ks ks
      (Delete_columns { Delete_columns.keyspace = ks.ks_id; table; key; columns; })
      P.read_ok

  let delete_key ks table key =
    request_ks ks
      (Delete_key { Delete_key.keyspace = ks.ks_id; table; key; })
      P.read_ok
end
