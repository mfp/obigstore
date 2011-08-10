
open Lwt
open Data_model
open Request

type error = Corrupted_frame | Bad_request | Unknown_keyspace | Other of int
type request_id = string

exception Error of error

let add_string dst s =
  Bytea.add_int32_le dst (String.length s);
  Bytea.add_string dst s

let add_frame_type = Bytea.add_int32_le

let add_option f dst = function
    None -> Bytea.add_byte dst 0
  | Some x -> Bytea.add_byte dst 1; f dst x

let skip_buf = String.create 4096

let rec skip ich count =
  match_lwt Lwt_io.read_into ich skip_buf 0 (min 4096 count) with
      n when n >= count -> return ()
    | n -> skip ich (count - n)

(* [request id : 8 byte]
 * [payload size: 4 byte]
 * [crc first 12 bytes]
 *    ...  payload  ...
 * [crc payload XOR crc first 12 bytes]
 * *)

let read_header ich =
  lwt head = Lwt_io.read ~count:8 ich in
  lwt crc = Lwt_io.read ~count:4 ich in
    if Crc32c.string head <> crc then
      raise_lwt (Error Corrupted_frame)
    else begin
      let get s n = Char.code (String.unsafe_get s n) in
      let v0 = get head 8 in
      let v1 = get head 9 in
      let v2 = get head 10 in
      let v3 = get head 11 in
        (* FIXME: check overflow (x86 only, not x86-64) *)
        return (String.sub head 0 8,
                v0 lor (v1 lsl 8) lor (v2 lsl 16) lor (v3 lsl 24),
                crc)
    end

let write_msg och req_id msg =
  (* FIXME: ensure req_id's size is 8 *)
  Lwt_io.atomic
    (fun och ->
      let header = Bytea.create 16 in
      let len = Bytea.length msg in
        Bytea.add_string header req_id;
        Bytea.add_int32_le header len;
        let crc = Crc32c.substring (Bytea.unsafe_string header) 0 12 in
          Bytea.add_string header crc;
          Lwt_io.write_from_exactly och (Bytea.unsafe_string header) 0 16 >>
          Lwt_io.write_from_exactly och (Bytea.unsafe_string msg) 0 len >>
          let crc2 = Crc32c.substring (Bytea.unsafe_string msg) 0 len in
            Crc32c.xor crc2 crc;
            Lwt_io.write och crc)
    och

type 'a writer =
  ?buf:Bytea.t -> Lwt_io.output_channel -> request_id:request_id -> 'a -> unit Lwt.t
type 'a reader = Lwt_io.input_channel -> 'a Lwt.t

module type PAYLOAD =
sig
  val bad_request : unit writer
  val unknown_keyspace : unit writer

  val return_keyspace : int writer
  val return_keyspace_list : string list writer
  val return_table_list : string list writer
  val return_table_size_on_disk : Int64.t writer
  val return_key_range_size_on_disk : Int64.t writer
  val return_keys : string list writer
  val return_key_count : Int64.t writer
  val return_slice : slice writer
  val return_slice_values : (key option * (key * string option list) list) writer
  val return_columns : (column_name * (column list)) option writer
  val return_column_values : string option list writer
  val return_column : (string * timestamp) option writer
  val return_ok : unit writer

  val read_keyspace : int reader
  val read_keyspace_list : string list reader
  val read_table_list : string list reader
  val read_table_size_on_disk : Int64.t reader
  val read_key_range_size_on_disk : Int64.t reader
  val read_keys : string list reader
  val read_key_count : Int64.t reader
  val read_slice : slice reader
  val read_slice_values : (key option * (key * string option list) list) reader
  val read_columns : (column_name * (column list)) option reader
  val read_column_values : string option list reader
  val read_column : (string * timestamp) option reader
  val read_ok : unit reader
end

module Make(D : Data_model.S)(P : PAYLOAD) =
struct
  open Request

  let sync_req_id = String.make 8 '\000'

  exception Abort_exn
  exception Commit_exn

  module H = Hashtbl.Make(struct
                            type t = int
                            let hash n = n
                            let equal a b = (a == b)
                          end)

  type client_state =
      {
        keyspaces : D.keyspace H.t;
        rev_keyspaces : int H.t;
        ich : Lwt_io.input_channel;
        och : Lwt_io.output_channel;
        db : D.db;
        out_buf : Bytea.t;
      }

  let tx_key = Lwt.new_key ()

  let init db ich och =
    {
      keyspaces = H.create 13;
      rev_keyspaces = H.create 13;
      ich; och; db;
      out_buf = Bytea.create 1024;
    }

  let rec service c =
    lwt request_id, len, crc = read_header c.ich in
      if request_id <> sync_req_id then begin
        (* ignore async request *)
        skip c.ich len >> service c
      end else
        service_request c ~request_id len crc

  and service_request c ~request_id len crc =
    lwt msg = Lwt_io.read c.ich ~count:len in
    lwt crc2 = Lwt_io.read c.ich ~count:4 in
    let crc2' = Crc32c.string msg in
      Crc32c.xor crc2 crc;
      if crc2 <> crc2' then
        P.bad_request c.och ~request_id ()
      else
        let m =
          try Some (Extprot.Conv.deserialize Request.read msg)
          with _ -> None
        in match m with
            None -> P.bad_request c.och ~request_id ()
          | Some r -> respond c ~request_id r

  and respond c ~request_id r =
    match r with
      Register_keyspace { Register_keyspace.name } ->
        let ks = D.register_keyspace c.db name in
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
          P.return_keyspace ~buf:c.out_buf c.och ~request_id idx >>
          service c
    | List_keyspaces _ ->
        P.return_keyspace_list ~buf:c.out_buf c.och ~request_id
          (D.list_keyspaces c.db) >>
        service c
    | List_tables { Req.keyspace } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             P.return_table_list ~buf:c.out_buf c.och ~request_id (D.list_tables ks))
    | Table_size_on_disk { Table_size_on_disk.keyspace; table; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             P.return_table_size_on_disk ~buf:c.out_buf c.och ~request_id
               (D.table_size_on_disk ks table))
    | Key_range_size_on_disk { Key_range_size_on_disk.keyspace; table; range; } ->
        with_keyspace c keyspace ~request_id
          (fun ks ->
             P.return_key_range_size_on_disk ~buf:c.out_buf c.och ~request_id
               (D.key_range_size_on_disk ks table
                  ?first:range.first ?up_to:range.up_to))
    | Begin { Req.keyspace; } ->
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
                           service c
                         with Commit_exn -> return ()))
             with Abort_exn -> return ())
    | Commit _ ->
        (* only commit if we're inside a tx *)
        begin match Lwt.get tx_key with
            None -> return ()
          | Some _ -> raise_lwt Commit_exn
        end
    | Abort _ ->
        (* only abort if we're inside a tx *)
        begin match Lwt.get tx_key with
            None -> return ()
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
              None -> P.unknown_keyspace c.och ~request_id () >> service c
            | Some ks -> D.repeatable_read_transaction ks f >> service c
          end
      | Some tx -> f tx >> service c

  and with_keyspace c ks_idx ~request_id f =
    let ks = try Some (H.find c.keyspaces ks_idx) with Not_found -> None in
      match ks with
          None -> P.unknown_keyspace c.och ~request_id () >> service c
        | Some ks -> f ks >> service c
end

