
open Printf
open Lwt
open Data_model

type error = Corrupted_frame | Bad_request | Unknown_keyspace | Other of int
type request_id = string

exception Error of error

let string_of_error = function
  | Corrupted_frame -> "Corrupted_frame"
  | Bad_request -> "Bad_request"
  | Unknown_keyspace  -> "Unknown_keyspace"
  | Other n -> sprintf "(Other %d)" n

let () =
  Printexc.register_printer
    (function Error x -> Some (sprintf "Error %s" (string_of_error x))
       | _ -> None)

let sync_req_id = String.make 8 '\000'

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
