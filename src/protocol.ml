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

open Printf
open Lwt
open Data_model

type error =
    Internal_error | Closed | Corrupted_frame | Bad_request
  | Unknown_serialization | Unknown_keyspace
  | Deadlock
  | Inconsistent_length of int * int  (* expected, actual *)
  | Other of int | Exception of exn

type request_id = string

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
    (function Error x -> Some (sprintf "Protocol.Error %s" (string_of_error x))
       | _ -> None)

let sync_req_id = String.make 8 '\000'

let is_sync_req x = x ==
  sync_req_id || String_util.strneq x 0 sync_req_id 0 8

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
    if Crc32c.substring head 0 12 <> crc then
      raise_lwt (Error Corrupted_frame)
    else begin
      let req_id =
        if String_util.cmp_substrings sync_req_id 0 8 head 0 8 = 0 then
          sync_req_id
        else
          String.sub head 0 8 in
      let get s n = Char.code (String.unsafe_get s n) in
      let v0 = get head 8 in
      let v1 = get head 9 in
      let v2 = get head 10 in
      let v3 = get head 11 in
        (* FIXME: check overflow (x86 only, not x86-64) *)
        return (req_id,
                v0 lor (v1 lsl 8) lor (v2 lsl 16) lor (v3 lsl 24),
                crc)
    end

let write_msg ?(flush=true) och req_id msg =
  (* FIXME: ensure req_id's size is 8 *)
  Lwt_io.atomic
    (fun och ->
      let header = Bytea.create 12 in
      let len = Bytea.length msg in
        Bytea.add_string header req_id;
        Bytea.add_int32_le header len;
        let crc = Crc32c.substring (Bytea.unsafe_string header) 0 12 in
          Lwt_io.write_from_exactly och (Bytea.unsafe_string header) 0 12 >>
          Lwt_io.write_from_exactly och crc 0 4 >>
          Lwt_io.write_from_exactly och (Bytea.unsafe_string msg) 0 len >>
          let crc2 = Crc32c.substring (Bytea.unsafe_string msg) 0 len in
            Crc32c.xor crc2 crc;
            Lwt_io.write och crc2 >>
            (if flush then Lwt_io.flush och else return ()))
    och

type 'a writer =
  ?buf:Bytea.t -> Lwt_io.output_channel -> request_id:request_id -> 'a -> unit Lwt.t
type 'a reader = Lwt_io.input_channel -> 'a Lwt.t

type backup_cursor = string

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
  val return_slice : slice writer
  val return_slice_values : (key option * (key * string option list) list) writer
  val return_columns : (column_name * (column list)) option writer
  val return_column_values : string option list writer
  val return_column : (string * timestamp) option writer
  val return_ok : unit writer
  val return_backup_dump : (string * backup_cursor option) option writer
  val return_backup_load_result : bool writer

  val read_keyspace : int reader
  val read_keyspace_maybe : int option reader
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
  val read_backup_dump : (string * backup_cursor option) option reader
  val read_backup_load_result : bool reader
end
