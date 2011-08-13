open Lwt
open Data_model
open Protocol
open Request

module Make(D : Data_model.S)(P : PAYLOAD) =
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
      }

  let tx_key = Lwt.new_key ()

  let setup_auto_yield t =
    incr num_clients;
    if !num_clients == 2 then begin
      let f = Lwt_unix.auto_yield 0.5e-3 in
        auto_yielder := f
    end;
    Gc.finalise
      (fun _ ->
         decr num_clients;
         if !num_clients <= 1 then auto_yielder := dummy_auto_yield)
      t

  let init ?(debug=false) db ich och =
    let t =
      {
        keyspaces = H.create 13;
        rev_keyspaces = H.create 13;
        ich; och; db; debug;
        out_buf = Bytea.create 1024;
        in_buf = String.create 128;
      }
    in setup_auto_yield t;
       t

  let read_exactly c n =
    let s = String.create n in
      Lwt_io.read_into_exactly c.ich s 0 n >>
      return s

  let rec service c =
    !auto_yielder () >>
    lwt request_id, len, crc = read_header c.ich in
      if request_id <> sync_req_id then begin
        (* ignore async request *)
        skip c.ich (len + 4) >> service c
      end else begin
        serve_sync_request c ~request_id len crc >>
        service c
      end


  and serve_sync_request c ~request_id len crc =
    if String.length c.in_buf < len then c.in_buf <- String.create len;
    Lwt_io.read_into_exactly c.ich c.in_buf 0 len >>
    lwt crc2 = read_exactly c 4 in
    let crc2' = Crc32c.substring c.in_buf 0 len in
      Crc32c.xor crc2 crc;
      if crc2 <> crc2' then
        P.bad_request c.och ~request_id ()
      else
        let m =
          try Some (Extprot.Conv.deserialize Request.read c.in_buf)
          with _ -> None
        in match m with
            None -> P.bad_request c.och ~request_id ()
          | Some r -> respond c ~request_id r

  and respond c ~request_id r =
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
          P.return_keyspace ~buf:c.out_buf c.och ~request_id idx
    | Get_keyspace { Get_keyspace.name; } -> begin
        match_lwt D.get_keyspace c.db name with
            None -> P.return_keyspace_maybe ~buf:c.out_buf c.och ~request_id None
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
                P.return_keyspace_maybe ~buf:c.out_buf c.och ~request_id
                  (Some idx)
      end
    | List_keyspaces _ ->
        D.list_keyspaces c.db >>=
          P.return_keyspace_list ~buf:c.out_buf c.och ~request_id
    | List_tables { List_tables.keyspace } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.list_tables ks >>=
             P.return_table_list ~buf:c.out_buf c.och ~request_id)
    | Table_size_on_disk { Table_size_on_disk.keyspace; table; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.table_size_on_disk ks table >>=
             P.return_table_size_on_disk ~buf:c.out_buf c.och ~request_id)
    | Key_range_size_on_disk { Key_range_size_on_disk.keyspace; table; range; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             D.key_range_size_on_disk ks table
               ?first:range.first ?up_to:range.up_to >>=
             P.return_key_range_size_on_disk ~buf:c.out_buf c.och ~request_id)
    | Begin { Begin.keyspace; } ->
        (* FIXME: check if we have an open tx in another ks, and signal error
         * if so *)
        with_keyspace c keyspace ~request_id
          (fun ks ->
             try_lwt
               D.repeatable_read_transaction ks
                 (fun tx ->
                    Lwt.with_value tx_key (Some tx)
                      (fun () ->
                         try_lwt
                           P.return_ok ~buf:c.out_buf c.och ~request_id ()>>
                           service c
                         with Commit_exn ->
                           P.return_ok ~buf:c.out_buf c.och ~request_id ()))
             with Abort_exn ->
               P.return_ok ~buf:c.out_buf c.och ~request_id ())
    | Commit _ ->
        (* only commit if we're inside a tx *)
        begin match Lwt.get tx_key with
            None -> P.return_ok ~buf:c.out_buf c.och ~request_id ()
          | Some _ -> raise_lwt Commit_exn
        end
    | Abort _ ->
        (* only abort if we're inside a tx *)
        begin match Lwt.get tx_key with
            None -> P.return_ok ~buf:c.out_buf c.och ~request_id ()
          | Some _ -> raise_lwt Abort_exn
        end
    | Get_keys { Get_keys.keyspace; table; max_keys; key_range; } ->
        with_tx c keyspace ~request_id
          (fun tx -> D.get_keys tx table ?max_keys key_range >>=
                     P.return_keys ~buf:c.out_buf c.och ~request_id)
    | Count_keys { Count_keys.keyspace; table; key_range; } ->
        with_tx c keyspace ~request_id
          (fun tx -> D.count_keys tx table key_range >>=
                     P.return_key_count ~buf:c.out_buf c.och ~request_id)
    | Get_slice { Get_slice.keyspace; table; max_keys; max_columns;
                  decode_timestamps; key_range; column_range; } ->
        with_tx c keyspace ~request_id
          (fun tx ->
             D.get_slice tx table ?max_keys ?max_columns ~decode_timestamps
               key_range column_range >>=
             P.return_slice ~buf:c.out_buf c.och ~request_id)
    | Get_slice_values { Get_slice_values.keyspace; table; max_keys;
                         key_range; columns; } ->
        with_tx c keyspace ~request_id
          (fun tx ->
             D.get_slice_values tx table ?max_keys key_range columns >>=
             P.return_slice_values ~buf:c.out_buf c.och ~request_id)
    | Get_columns { Get_columns.keyspace; table; max_columns;
                    decode_timestamps; key; column_range; } ->
        with_tx c keyspace ~request_id
          (fun tx ->
             D.get_columns tx table ?max_columns ~decode_timestamps
               key column_range >>=
             P.return_columns ~buf:c.out_buf c.och ~request_id)
    | Get_column_values { Get_column_values.keyspace; table; key; columns; } ->
        with_tx c keyspace ~request_id
          (fun tx ->
             D.get_column_values tx table key columns >>=
             P.return_column_values ~buf:c.out_buf c.och ~request_id)
    | Get_column { Get_column.keyspace; table; key; column; } ->
        with_tx c keyspace ~request_id
          (fun tx -> D.get_column tx table key column >>=
                     P.return_column ~buf:c.out_buf c.och ~request_id)
    | Put_columns { Put_columns.keyspace; table; key; columns } ->
        with_tx c keyspace ~request_id
          (fun tx -> D.put_columns tx table key columns >>=
                     P.return_ok ~buf:c.out_buf c.och ~request_id)
    | Delete_columns { Delete_columns.keyspace; table; key; columns; } ->
        with_tx c keyspace ~request_id
          (fun tx -> D.delete_columns tx table key columns >>=
                     P.return_ok ~buf:c.out_buf c.och ~request_id)
    | Delete_key { Delete_key.keyspace; table; key; } ->
        with_tx c keyspace ~request_id
          (fun tx -> D.delete_key tx table key >>=
                     P.return_ok ~buf:c.out_buf c.och ~request_id)

  and with_tx c keyspace_idx ~request_id f =
    match Lwt.get tx_key with
        None ->
          let ks = try Some (H.find c.keyspaces keyspace_idx)
                   with Not_found -> None
          in begin match ks with
              None -> P.unknown_keyspace c.och ~request_id ()
            | Some ks -> D.repeatable_read_transaction ks f
          end
      | Some tx -> f tx

  and with_keyspace c ks_idx ~request_id f =
    let ks = try Some (H.find c.keyspaces ks_idx) with Not_found -> None in
      match ks with
          None -> P.unknown_keyspace c.och ~request_id ()
        | Some ks -> f ks
end

