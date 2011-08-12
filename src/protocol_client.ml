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
  }

  type keyspace = { ks_name : string; ks_id : int; ks_db : db; }

  type transaction = keyspace

  let make ich och = { ich; och; buf = Bytea.create 64; closed = false; }

  let close t =
    if not t.closed then
      ignore (Lwt_io.close t.ich >> Lwt_io.close t.och);
    t.closed <- true

  let send_request t req =
    Bytea.clear t.buf;
    Request.write (t.buf :> Extprot.Msg_buffer.t) req;
    Protocol.write_msg t.och Protocol.sync_req_id t.buf

  let get_response f ich =
    lwt request_id, len, crc = Protocol.read_header ich in
    (* FIXME: should check that f reads exactly [len] bytes *)
    lwt x = f ich in
    lwt crc2 = Lwt_io.read ~count:4 ich in
    (* FIXME: should check CRC2 = CRC(payload) XOR CRC1 *)
      return x

  let list_keyspaces t =
    send_request t (List_keyspaces { List_keyspaces.prefix = "" }) >>
    get_response P.read_keyspace_list t.ich

  let register_keyspace t name =
    send_request t (Register_keyspace { Register_keyspace.name; }) >>
    lwt ks_id = get_response P.read_keyspace t.ich in
      return { ks_id; ks_name = name; ks_db = t; }

  let get_keyspace t name =
    send_request t (Get_keyspace { Get_keyspace.name; }) >>
    match_lwt get_response P.read_keyspace_maybe t.ich with
        None -> return None
      | Some ks_id -> return (Some { ks_id; ks_name = name; ks_db = t; })

  let keyspace_name ks = ks.ks_name
  let keyspace_id ks = ks.ks_id

  let read_result ks f =
    get_response f ks.ks_db.ich

  let list_tables ks =
    send_request ks.ks_db (List_tables { Req.keyspace = ks.ks_id; }) >>
    read_result ks P.read_table_list

  let table_size_on_disk ks table =
    send_request ks.ks_db
      (Table_size_on_disk { Table_size_on_disk.keyspace = ks.ks_id; table; }) >>
    read_result ks P.read_table_size_on_disk

  let key_range_size_on_disk ks ?first ?up_to table =
    send_request ks.ks_db
      (Key_range_size_on_disk
         { Key_range_size_on_disk.keyspace = ks.ks_id; table;
           range = { first; up_to; } }; ) >>
    read_result ks P.read_key_range_size_on_disk

  let read_committed_transaction ks f =
    send_request ks.ks_db (Begin { Req.keyspace = ks.ks_id }) >>
    read_result ks P.read_ok >>
    try_lwt
      lwt y = f ks in
        send_request ks.ks_db (Commit { Req.keyspace = ks.ks_id }) >>
        read_result ks P.read_ok >>
        return y
    with e ->
      send_request ks.ks_db (Abort { Req.keyspace = ks.ks_id }) >>
      read_result ks P.read_ok >>
      raise_lwt e

  let repeatable_read_transaction = read_committed_transaction

  let get_keys ks table ?max_keys key_range =
    send_request ks.ks_db
      (Get_keys { Get_keys.keyspace = ks.ks_id; table; max_keys; key_range; }) >>
    read_result ks P.read_keys

  let count_keys ks table key_range =
    send_request ks.ks_db
      (Count_keys { Count_keys.keyspace = ks.ks_id; table; key_range; }) >>
    read_result ks P.read_key_count

  let get_slice ks table ?max_keys ?max_columns ?(decode_timestamps=false)
    key_range column_range =
    send_request ks.ks_db
      (Get_slice { Get_slice.keyspace = ks.ks_id; table;
                   max_keys; max_columns; decode_timestamps;
                   key_range; column_range }) >>
    read_result ks P.read_slice

  let get_slice_values ks table ?max_keys key_range columns =
    send_request ks.ks_db
      (Get_slice_values { Get_slice_values.keyspace = ks.ks_id; table;
                          max_keys; key_range; columns; }) >>
    read_result ks P.read_slice_values

  let get_columns ks table ?max_columns ?(decode_timestamps=false)
        key column_range =
    send_request ks.ks_db
      (Get_columns { Get_columns.keyspace = ks.ks_id; table;
                     max_columns; decode_timestamps; key; column_range; }) >>
    read_result ks P.read_columns

  let get_column_values ks table key columns =
    send_request ks.ks_db
      (Get_column_values { Get_column_values.keyspace = ks.ks_id; table;
                           key; columns; }) >>
    read_result ks P.read_column_values

  let get_column ks table key column =
    send_request ks.ks_db
      (Get_column { Get_column.keyspace = ks.ks_id; table; key; column; }) >>
    read_result ks P.read_column

  let put_columns ks table key columns =
    send_request ks.ks_db
      (Put_columns { Put_columns.keyspace = ks.ks_id; table; key; columns; }) >>
    read_result ks P.read_ok

  let delete_columns ks table key columns =
    send_request ks.ks_db
      (Delete_columns { Delete_columns.keyspace = ks.ks_id; table; key; columns; }) >>
    read_result ks P.read_ok

  let delete_key ks table key =
    send_request ks.ks_db
      (Delete_key { Delete_key.keyspace = ks.ks_id; table; key; }) >>
    read_result ks P.read_ok
end
