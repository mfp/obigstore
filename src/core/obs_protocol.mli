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

(** Definitions and convenience functions for remote obigstore protocol. *)

exception Corrupted_data_header

type error =
    Internal_error
  | Closed
  | Corrupted_frame
  | Bad_request
  | Unknown_serialization
  | Unknown_keyspace
  | Deadlock
  | Inconsistent_length of int * int
  | Other of int
  | Exception of exn

type request_id = string
type crc = string

type header =
    Header of (request_id * int * crc)
  | Corrupted_header

exception Error of error

val string_of_error : error -> string
val sync_req_id : string
val is_sync_req : string -> bool

val skip : Lwt_io.input_channel -> int -> unit Lwt.t

val read_header : Lwt_io.input_channel -> header Lwt.t

val write_msg :
  ?flush:bool ->
  Lwt_io.output Lwt_io.channel -> string -> Obs_bytea.t -> unit Lwt.t

type 'a writer =
    ?buf:Obs_bytea.t ->
    Lwt_io.output_channel -> request_id:request_id -> 'a -> unit Lwt.t

type 'a reader = Lwt_io.input_channel -> 'a Lwt.t

type backup_cursor = string
type raw_dump_timestamp = Int64.t

module type PAYLOAD_WRITER =
sig
  val bad_request : unit writer
  val unknown_keyspace : unit writer
  val unknown_serialization : unit writer
  val internal_error : unit writer
  val deadlock : unit writer
  val dirty_data : unit writer

  val return_keyspace : int writer
  val return_keyspace_maybe : int option writer
  val return_keyspace_list : string list writer
  val return_table_list : string list writer
  val return_table_size_on_disk : Int64.t writer
  val return_key_range_size_on_disk : Int64.t writer
  val return_keys : string list writer
  val return_key_count : Int64.t writer
  val return_slice : (string, string) Obs_data_model.slice writer
  val return_slice_values :
    (Obs_data_model.key option * (Obs_data_model.key * string option list) list)
    writer
  val return_slice_values_timestamps :
    (Obs_data_model.key option * (Obs_data_model.key * (string * Int64.t) option list) list) writer
  val return_columns :
    (Obs_data_model.column_name * string Obs_data_model.column list) option writer
  val return_column_values : string option list writer
  val return_column : (string * Obs_data_model.timestamp) option writer
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
end

module type PAYLOAD_READER =
sig
  val read_keyspace : int reader
  val read_keyspace_maybe : int option reader
  val read_keyspace_list : string list reader
  val read_table_list : string list reader
  val read_table_size_on_disk : Int64.t reader
  val read_key_range_size_on_disk : Int64.t reader
  val read_keys : string list reader
  val read_key_count : Int64.t reader
  val read_slice : (string, string) Obs_data_model.slice reader
  val read_slice_values :
    (Obs_data_model.key option * (Obs_data_model.key * string option list) list)
    reader
  val read_slice_values_timestamps :
    (Obs_data_model.key option * (Obs_data_model.key * (string * Int64.t) option list) list) reader
  val read_columns :
    (Obs_data_model.column_name * string Obs_data_model.column list) option reader
  val read_column_values : string option list reader
  val read_column : (string * Obs_data_model.timestamp) option reader
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

val data_protocol_version : data_protocol_version

val data_response_code : data_response -> int
val data_response_of_code : int -> [ data_response | `Other ]

val data_request_code : data_request -> int
val data_request_of_code : int -> [ data_request | `Other ]

val data_conn_handshake :
  Lwt_io.input_channel -> Lwt_io.output_channel -> data_protocol_version Lwt.t

val read_exactly :
  Lwt_io.input_channel -> int -> string Lwt.t

val crc32c_of_int_le : int -> string

val write_checksummed_int32_le : Lwt_io.output_channel -> int -> unit Lwt.t
val write_checksummed_int64_le : Lwt_io.output_channel -> Int64.t -> unit Lwt.t

(** @return None if the checksum fails *)
val read_checksummed_int : Lwt_io.input_channel -> int option Lwt.t

(** @return None if the checksum fails *)
val read_checksummed_int64_le : Lwt_io.input_channel -> Int64.t option Lwt.t

(**/**)
val krange : string Obs_data_model.key_range -> Obs_request.Key_range.key_range
val crange : Obs_data_model.column_range -> Obs_request.Column_range.column_range

val krange' : Obs_request.Key_range.key_range -> string Obs_data_model.key_range
val crange' : Obs_request.Column_range.column_range -> Obs_data_model.column_range
