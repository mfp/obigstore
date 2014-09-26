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
open Obs_data_model

module Option = BatOption

module Obs_request_serialization =
struct
  type t = [`Raw | `Extprot | `Unknown]

  let of_format_id = function
      0 -> `Raw
    | 1 -> `Extprot
    | _ -> `Unknown

  let format_id = function
      `Raw -> 0
    | `Extprot -> 1
end

module type S =
sig
  include Obs_protocol.PAYLOAD_WRITER
  include Obs_protocol.PAYLOAD_READER

  val version : int * int * int

  val read_header : Lwt_io.input_channel -> Obs_protocol.header Lwt.t

  val write_msg :
    ?flush:bool ->
    Lwt_io.output Lwt_io.channel -> string -> Obs_bytea.t -> unit Lwt.t

  val read_request : string ref ->
    Lwt_io.input_channel -> Lwt_io.output_channel ->
    (Obs_request.Request.request * Obs_protocol.request_id * int) option Lwt.t
end

module Version_0_0_0 : S =
struct
  let version = (0, 0, 0)

  module E =
  struct
    open Obs_bytea

    let add_int32_le = add_int32_le
    let add_status = add_int32_le
    let add_int64_le = add_int64_le

    let add_raw_string b s = add_string b s

    let add_string b s =
      add_int32_le b (String.length s);
      add_string b s

    let add_list f b l =
      add_int32_le b (List.length l);
      List.iter (f b) l

    let add_tuple2 f g b (x, y) = f b x; g b y

    let add_option f b = function
        None -> add_int32_le b 0
      | Some x -> add_int32_le b 1;
                  f b x

    let add_bool b x = add_byte b (if x then 1 else 0)
  end

  module D =
  struct
    let get_int32_le = Lwt_io.LE.read_int
    let get_status = get_int32_le
    let get_int64_le = Lwt_io.LE.read_int64
    let get_float ich = Lwt_io.LE.read_int64 ich >|= Int64.float_of_bits

    let get_string ich =
      lwt count = get_int32_le ich in
      let s = String.create count in
        Lwt_io.read_into_exactly ich s 0 count >>
        return s

    let get_list ?(do_reverse = true) f ich =
      let rec loop_read_list acc f ich = function
          n when n > 0 -> lwt x = f ich in loop_read_list (x :: acc) f ich (n-1)
        | _ (* 0 *) -> return acc in
      lwt len = get_int32_le ich in
      if do_reverse then
        (loop_read_list [] f ich len >|= List.rev)
      else
        (loop_read_list [] f ich len)

    let get_tuple2 f g ich =
      lwt x = f ich in
      lwt y = g ich in
        return (x, y)

    let get_tuple3 f g h ich =
      lwt x = f ich in
      lwt y = g ich in
      lwt z = h ich in
        return (x, y, z)

    let get_option f ich =
      match_lwt get_int32_le ich with
          0 -> return None
        | _ -> lwt x = f ich in return (Some x)

    let get_bool ich =
      lwt c = Lwt_io.read_char ich in
        return (c <> '\000')
  end

  let null_timestamp = String.make 8 '\255'

  (* [request id : 8 byte]
   * [payload size: 4 byte]
   * [crc first 12 bytes]
   *    ...  payload  ...
   * [crc payload XOR crc first 12 bytes]
   * *)

  let read_header ich =
    let head = String.create 16 in
    Lwt_io.read_into_exactly ich head 0 16 >>
    let crc = String.sub head 12 4 in
      if Obs_crc32c.substring_masked head 0 12 <> crc then
        return Obs_protocol.Corrupted_header
      else begin
        let req_id =
          if Obs_string_util.cmp_substrings
               Obs_protocol.sync_req_id 0 8 head 0 8 = 0 then
            Obs_protocol.sync_req_id
          else
            String.sub head 0 8 in
        let get s n = Char.code (String.unsafe_get s n) in
        let v0 = get head 8 in
        let v1 = get head 9 in
        let v2 = get head 10 in
        let v3 = get head 11 in
          (* FIXME: check overflow (x86 only, not x86-64) *)
          return (Obs_protocol.Header
                    (req_id,
                     v0 lor (v1 lsl 8) lor (v2 lsl 16) lor (v3 lsl 24),
                     crc))
      end

  let write_msg ?(flush=false) och req_id msg =
    (* FIXME: ensure req_id's size is 8 *)
    Lwt_io.atomic
      (fun och ->
        let header = Obs_bytea.create 12 in
        let len = Obs_bytea.length msg in
          Obs_bytea.add_string header req_id;
          Obs_bytea.add_int32_le header len;
          let crc = Obs_crc32c.substring_masked (Obs_bytea.unsafe_string header) 0 12 in
            Lwt_io.write_from_exactly och (Obs_bytea.unsafe_string header) 0 12 >>
            Lwt_io.write_from_exactly och crc 0 4 >>
            Lwt_io.write_from_exactly och (Obs_bytea.unsafe_string msg) 0 len >>
            let crc2 = Obs_crc32c.substring_masked (Obs_bytea.unsafe_string msg) 0 len in
              Obs_crc32c.xor crc2 crc;
              Lwt_io.write och crc2 >>
              (if flush then Lwt_io.flush och else return_unit))
      och

  let writer f ?(buf=Obs_bytea.create 16) och ~request_id x =
    Obs_bytea.clear buf;
    let () = f buf x in
      write_msg och request_id buf

  let raise_error_status = let open Obs_protocol in function
      -1 -> raise_lwt (Error Bad_request)
    | -2 -> raise_lwt (Error Unknown_keyspace)
    | -3 -> raise_lwt (Error Unknown_serialization)
    | -4 -> raise_lwt (Error Internal_error)
    | -5 -> raise_lwt Deadlock
    | -6 -> raise_lwt Dirty_data
    | n -> raise_lwt (Error (Other n))

  let reader f ich =
    match_lwt D.get_status ich with
        0 -> f ich
      | n -> raise_error_status n

  let bad_request =
    writer (fun b () -> E.add_status b (-1))

  let unknown_keyspace =
    writer (fun b () -> E.add_status b (-2))

  let unknown_serialization =
    writer (fun b () -> E.add_status b (-3))

  let internal_error =
    writer (fun b () -> E.add_status b (-4))

  let deadlock =
    writer (fun b () -> E.add_status b (-5))

  let dirty_data =
    writer (fun b () -> E.add_status b (-6))

  let return_keyspace =
    writer (fun b ks ->
              E.add_status b 0;
              E.add_int32_le b ks)

  let read_keyspace = reader D.get_int32_le

  let return_keyspace_maybe =
    writer (fun b ks ->
              E.add_status b 0;
              E.add_option E.add_int32_le b ks)

  let read_keyspace_maybe = reader (D.get_option D.get_int32_le)

  let return_keyspace_list =
    writer (fun b l ->
              E.add_status b 0;
              E.add_list E.add_string b l)

  let read_keyspace_list = reader (D.get_list D.get_string)

  let return_table_list = return_keyspace_list

  let read_table_list = read_keyspace_list

  let return_table_size_on_disk =
    writer (fun b siz -> E.add_status b 0;
                         E.add_int64_le b siz)

  let read_table_size_on_disk = reader D.get_int64_le

  let return_key_range_size_on_disk = return_table_size_on_disk

  let read_key_range_size_on_disk = read_table_size_on_disk

  let return_keys = return_keyspace_list

  let read_keys = read_keyspace_list

  let return_key_count = return_table_size_on_disk

  let read_key_count = read_table_size_on_disk

  let add_timestamp b = function
      No_timestamp -> E.add_raw_string b null_timestamp
    | Timestamp t -> E.add_int64_le b t

  external decode_int64_le : string -> int -> Int64.t = "obigstore_decode_int64_le"

  let timestamp_buf = String.create 8

  let get_timestamp ich =
    Lwt_io.read_into_exactly ich timestamp_buf 0 8 >>
      match timestamp_buf with
          x when x = null_timestamp -> return No_timestamp
        | s -> return (Timestamp (decode_int64_le s 0))

  let add_column b { name; data; timestamp; } =
    E.add_string b name;
    E.add_string b data;
    add_timestamp b timestamp

  let get_column ich =
    lwt name = D.get_string ich in
    lwt data = D.get_string ich in
    lwt timestamp = get_timestamp ich in
      return { name; data; timestamp }

  let return_slice =
    writer
      (fun b (last_key, key_data_list) ->
         let add_key_data b { key; last_column; columns; } =
           E.add_string b key;
           E.add_string b last_column;
           E.add_list add_column b columns
         in
           E.add_status b 0;
           E.add_option E.add_string b last_key;
           E.add_list add_key_data b key_data_list)

  let read_slice =
    reader
      (fun ich ->
         let read_key_data ich =
           lwt key = D.get_string ich in
           lwt last_column = D.get_string ich in
           lwt columns = D.get_list get_column ich in
             return { key; last_column; columns; } in
         lwt last_key = D.get_option D.get_string ich in
         lwt key_data_list = D.get_list read_key_data ich in
           return (last_key, key_data_list))

  let return_slice_values =
    writer
      (fun b (last_key_opt, data_list) ->
         E.add_status b 0;
         E.add_option E.add_string b last_key_opt;
         E.add_list
           (E.add_tuple2
              E.add_string
              (E.add_list (E.add_option E.add_string)))
           b data_list)

  let read_slice_values =
    reader
      (fun ich ->
         lwt last_key_opt = D.get_option D.get_string ich in
         lwt data_list =
           D.get_list
             (D.get_tuple2
                D.get_string
                (D.get_list (D.get_option D.get_string)))
             ich
         in return (last_key_opt, data_list))

  let return_slice_values_timestamps =
    writer
      (fun b (last_key_opt, data_list) ->
         E.add_status b 0;
         E.add_option E.add_string b last_key_opt;
         E.add_list
           (E.add_tuple2
              E.add_string
              (E.add_list
                 (E.add_option
                    (E.add_tuple2 E.add_string E.add_int64_le))))
           b data_list)

  let read_slice_values_timestamps =
    reader
      (fun ich ->
         lwt last_key_opt = D.get_option D.get_string ich in
         lwt data_list =
           D.get_list
             (D.get_tuple2
                D.get_string
                (D.get_list
                   (D.get_option
                      (D.get_tuple2 D.get_string D.get_int64_le))))
             ich
         in return (last_key_opt, data_list))

  let return_columns =
    writer
      (fun b x ->
         E.add_status b 0;
         E.add_option
           (E.add_tuple2 E.add_string (E.add_list add_column)) b x)

  let read_columns =
    reader (D.get_option (D.get_tuple2 D.get_string (D.get_list get_column)))

  let return_column_values =
    writer (fun b x ->
              E.add_status b 0;
              E.add_list (E.add_option E.add_string) b x)

  let read_column_values =
    reader (D.get_list (D.get_option D.get_string))

  let return_column =
    writer (fun b x ->
              E.add_status b 0;
              E.add_option (E.add_tuple2 E.add_string add_timestamp) b x)

  let read_column =
    reader (D.get_option (D.get_tuple2 D.get_string get_timestamp))

  let return_ok = writer (fun b () -> E.add_status b 0)

  let read_ok ich =
    match_lwt D.get_status ich with 0 -> return_unit | n -> raise_error_status n

  let add_backup_dump =
    E.add_tuple2 E.add_string (E.add_option E.add_string)

  let get_backup_dump =
    D.get_tuple2 D.get_string (D.get_option D.get_string)

  let read_backup_dump = reader (D.get_option get_backup_dump)

  let return_backup_dump =
    writer
      (fun b x ->
         E.add_status b 0;
         E.add_option add_backup_dump b x)

  let return_backup_load_result =
    writer (fun b x ->
              E.add_status b 0;
              E.add_int32_le b (if x then 0 else -1))

  let read_backup_load_result =
    reader (fun ich ->
              D.get_status ich >|= function
                  0 -> true
                | _ -> false)

  let return_load_stats =
    writer
      (fun b stats ->
         E.add_status b 0;
         E.add_string b (Extprot.Conv.serialize Obs_request.Load_stats.write stats))

  let read_load_stats =
    reader
      (fun ich ->
         lwt s = D.get_string ich in
           return (Extprot.Conv.deserialize Obs_request.Load_stats.read s))

  let return_exist_result =
    writer
      (fun b l ->
         E.add_status b 0;
         E.add_list E.add_bool b l)

  let read_exist_result = reader (D.get_list D.get_bool)

  let read_notifications = reader (D.get_list D.get_string)

  let return_notifications =
    writer
      (fun b l ->
         E.add_status b 0;
         E.add_list E.add_string b l)

  let return_raw_dump_id_timestamp_dir =
    writer (fun b (id, timestamp, dir) ->
              E.add_status b 0;
              E.add_int64_le b id;
              E.add_int64_le b timestamp;
              E.add_string b dir)

  let read_raw_dump_id_timestamp_dir =
    reader (D.get_tuple3 D.get_int64_le D.get_int64_le D.get_string)

  let return_raw_dump_files =
    writer
      (fun b l ->
         E.add_status b 0;
         E.add_list (E.add_tuple2 E.add_string E.add_int64_le) b l)

  let read_raw_dump_files =
    reader (D.get_list (D.get_tuple2 D.get_string D.get_int64_le))

  let return_raw_dump_file_digest =
    writer
      (fun b digest -> E.add_status b 0;
                       E.add_option E.add_string b digest)

  let read_raw_dump_file_digest = reader (D.get_option D.get_string)

  let return_property =
    writer
      (fun b data ->
         E.add_status b 0;
         E.add_option E.add_string b data)

  let read_property =
    reader (D.get_option D.get_string)

  let return_tx_id =
    writer
      (fun b data ->
         E.add_status b 0;
         E.add_option (E.add_tuple2 E.add_int32_le E.add_int32_le) b data)

  let read_tx_id =
    reader (D.get_option (D.get_tuple2 D.get_int32_le D.get_int32_le))

  let read_request in_buf ich och =
    lwt request_id, len, crc =
      match_lwt read_header ich with
          Obs_protocol.Header x -> return x
        | Obs_protocol.Corrupted_header ->
            (* we can't even trust the request_id, so all that's left is
             * dropping the connection *)
            raise_lwt (Obs_protocol.Error Obs_protocol.Corrupted_frame) in
    let in_buf =
      if String.length !in_buf >= len then !in_buf
      else begin
        in_buf := String.create len;
        !in_buf
      end
    in
      Lwt_io.read_into_exactly ich in_buf 0 len >>
      lwt crc2 = Obs_protocol.read_exactly ich 4 in
      let crc2' = Obs_crc32c.substring_masked in_buf 0 len in
      let gb n = Char.code in_buf.[n] in
      let format_id = gb 0 + (gb 1 lsl 8) + (gb 2 lsl 16) + (gb 3 lsl 24) in
        Obs_crc32c.xor crc2 crc;
        if crc2 <> crc2' then begin
          bad_request och ~request_id () >>
          return None
        end else begin
          match Obs_request_serialization.of_format_id format_id with
              `Extprot -> begin
                try
                  let m = Extprot.Conv.deserialize
                            Obs_request.Request.read ~offset:4 in_buf
                  in return (Some (m, request_id, len))
                with _ -> bad_request och ~request_id () >> return None
              end
            | `Raw -> bad_request och ~request_id () >> return None
            | `Unknown -> unknown_serialization och ~request_id () >> return None
        end
end
