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

type error =
    Internal_error | Closed | Corrupted_frame | Bad_request
  | Unknown_serialization | Unknown_keyspace
  | Deadlock
  | Inconsistent_length of int * int  (* expected, actual *)
  | Other of int | Exception of exn

type request_id = string
type crc = string

type header =
    Header of (string * int * string)
  | Corrupted_header

exception Corrupted_data_header

exception Error of error

let string_of_error = function
    Internal_error -> "Internal_error"
  | Closed -> "Closed"
  | Corrupted_frame -> "Corrupted_frame"
  | Bad_request -> "Bad_request"
  | Unknown_serialization -> "Unknown_serialization"
  | Unknown_keyspace  -> "Unknown_keyspace"
  | Deadlock -> "Deadlock"
  | Inconsistent_length (exp, act) ->
      sprintf "(Inconsistent_length (%d, %d))" exp act
  | Other n -> sprintf "(Other %d)" n
  | Exception exn -> sprintf "(Exception (%s))" (Printexc.to_string exn)

let () =
  Printexc.register_printer
    (function Error x -> Some (sprintf "Obs_protocol.Error %s" (string_of_error x))
       | _ -> None)

let sync_req_id = String.make 8 '\000'

let is_sync_req x = x ==
  sync_req_id || Obs_string_util.strneq x 0 sync_req_id 0 8

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

let head = String.create 16

let read_header ich =
  Lwt_io.read_into_exactly ich head 0 16 >>
  let crc = String.sub head 12 4 in
    if Obs_crc32c.substring_masked head 0 12 <> crc then
      return Corrupted_header
    else begin
      let req_id =
        if Obs_string_util.cmp_substrings sync_req_id 0 8 head 0 8 = 0 then
          sync_req_id
        else
          String.sub head 0 8 in
      let get s n = Char.code (String.unsafe_get s n) in
      let v0 = get head 8 in
      let v1 = get head 9 in
      let v2 = get head 10 in
      let v3 = get head 11 in
        (* FIXME: check overflow (x86 only, not x86-64) *)
        return (Header
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
            (if flush then Lwt_io.flush och else return ()))
    och

type 'a writer =
  ?buf:Obs_bytea.t -> Lwt_io.output_channel -> request_id:request_id -> 'a -> unit Lwt.t
type 'a reader = Lwt_io.input_channel -> 'a Lwt.t

type backup_cursor = string
type raw_dump_timestamp = Int64.t

module type PAYLOAD =
sig
  val bad_request : unit writer
  val unknown_keyspace : unit writer
  val unknown_serialization : unit writer
  val internal_error : unit writer
  val deadlock : unit writer

  val return_keyspace : int writer
  val return_keyspace_maybe : int option writer
  val return_keyspace_list : string list writer
  val return_table_list : string list writer
  val return_table_size_on_disk : Int64.t writer
  val return_key_range_size_on_disk : Int64.t writer
  val return_keys : string list writer
  val return_key_count : Int64.t writer
  val return_slice : (string, string) slice writer
  val return_slice_values : (key option * (key * string option list) list) writer
  val return_slice_values_timestamps :
    (key option * (key * (string * Int64.t) option list) list) writer
  val return_columns : (column_name * (string column list)) option writer
  val return_column_values : string option list writer
  val return_column : (string * timestamp) option writer
  val return_ok : unit writer
  val return_backup_dump : (string * backup_cursor option) option writer
  val return_backup_load_result : bool writer
  val return_load_stats : Obs_load_stats.stats writer
  val return_exist_result : bool list writer
  val return_notifications : string list writer
  val return_raw_dump_id_and_timestamp : (Int64.t * raw_dump_timestamp) writer
  val return_raw_dump_files : (string * Int64.t) list writer
  val return_raw_dump_file_digest : string option writer
  val return_property : string option writer

  val read_keyspace : int reader
  val read_keyspace_maybe : int option reader
  val read_keyspace_list : string list reader
  val read_table_list : string list reader
  val read_table_size_on_disk : Int64.t reader
  val read_key_range_size_on_disk : Int64.t reader
  val read_keys : string list reader
  val read_key_count : Int64.t reader
  val read_slice : (string, string) slice reader
  val read_slice_values : (key option * (key * string option list) list) reader
  val read_slice_values_timestamps :
    (key option * (key * (string * Int64.t) option list) list) reader
  val read_columns : (column_name * (string column list)) option reader
  val read_column_values : string option list reader
  val read_column : (string * timestamp) option reader
  val read_ok : unit reader
  val read_backup_dump : (string * backup_cursor option) option reader
  val read_backup_load_result : bool reader
  val read_load_stats : Obs_load_stats.stats reader
  val read_exist_result : bool list reader
  val read_notifications : string list reader
  val read_raw_dump_id_and_timestamp : (Int64.t * raw_dump_timestamp) reader
  val read_raw_dump_files : (string * Int64.t) list reader
  val read_raw_dump_file_digest : string option reader
  val read_property : string option reader
end

type data_protocol_version = int * int * int
type data_request = [ `Get_file | `Get_updates ]
type data_response = [ `OK | `Unknown_dump | `Unknown_file ]

let data_response_code = function
    `OK -> 0
  | `Unknown_dump -> 1
  | `Unknown_file -> 2

let data_response_of_code = function
    0 -> `OK
  | 1 -> `Unknown_dump
  | 2 -> `Unknown_file
  | _ -> `Other

let data_request_of_code = function
    0 -> `Get_file
  | 1 -> `Get_updates
  | _ -> `Other

let data_request_code = function
    `Get_file -> 0
  | `Get_updates -> 1

let data_protocol_version = (0, 0, 0)

let data_conn_handshake ich och =
  let (self_major, self_minor, self_bugfix) = data_protocol_version in
  lwt () =
    Lwt_io.LE.write_int och self_major >>
    Lwt_io.LE.write_int och self_minor >>
    Lwt_io.LE.write_int och self_bugfix in
  lwt major = Lwt_io.LE.read_int ich in
  lwt minor = Lwt_io.LE.read_int ich in
  lwt bugfix = Lwt_io.LE.read_int ich in
    return (major, minor, bugfix)

let read_exactly ich n =
  let s = String.create n in
    Lwt_io.read_into_exactly ich s 0 n >>
    return s

let crc32c_of_int_le n =
  let b = Obs_bytea.create 4 in
    Obs_bytea.add_int32_le b n;
    Obs_crc32c.substring_masked (Obs_bytea.unsafe_string b) 0 4

let crc32c_of_int64_le n =
  let b = Obs_bytea.create 8 in
    Obs_bytea.add_int64_le b n;
    Obs_crc32c.substring_masked (Obs_bytea.unsafe_string b) 0 8

let write_checksummed_int32_le och n =
  Lwt_io.LE.write_int och n >>
  Lwt_io.write och (crc32c_of_int_le n)

let write_checksummed_int64_le och n =
  Lwt_io.LE.write_int64 och n >>
  Lwt_io.write och (crc32c_of_int64_le n)

let read_checksummed_int ich =
  lwt n = Lwt_io.LE.read_int ich in
  lwt crc = read_exactly ich 4 in
    if crc32c_of_int_le n <> crc then return None
    else return (Some n)

let read_checksummed_int64_le ich =
  lwt n = Lwt_io.LE.read_int64 ich in
  lwt crc = read_exactly ich 4 in
    if crc32c_of_int64_le n <> crc then return None
    else return (Some n)

open Obs_request

let range = function
    `Continuous c -> Key_range.Key_range c
  | `All -> Key_range.Key_range { first = None; up_to = None; reverse = false }
  | `Discrete l -> Key_range.Keys l

let krange = range

let crange = function
    `All -> Column_range.All_columns
  | `Discrete l ->
      Column_range.Column_range_union [Simple_column_range.Columns l]
  | `Continuous r ->
      Column_range.Column_range_union [Simple_column_range.Column_range r]
  | `Union l ->
      Column_range.Column_range_union
        (List.map
           (function
              | `Discrete l -> Simple_column_range.Columns l
              | `Continuous r -> Simple_column_range.Column_range r)
           l)

let krange' = function
    Key_range.Key_range c -> `Continuous c
  | Key_range.Keys l -> `Discrete l

let crange' = function
    Column_range.All_columns -> `All
  | Column_range.Column_range_union [Simple_column_range.Columns l] -> `Discrete l
  | Column_range.Column_range_union [Simple_column_range.Column_range r] ->
      `Continuous r
  | Column_range.Column_range_union l ->
      `Union
        (List.map
           (function
              | Simple_column_range.Columns l -> `Discrete l
              | Simple_column_range.Column_range r -> `Continuous r)
           l)
