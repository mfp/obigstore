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

module L = LevelDB
module IT = L.Iterator

open Obs_data_model

type cursor =
  { bc_remaining_tables : table list; bc_key : key; bc_column : column_name; }

module DEC =
struct
  exception EOS

  type t = { data : string; mutable off : int; max : int; }

  let make data = { data; off = 0; max = String.length data; }

  let gb s n = Char.code (String.unsafe_get s n)

  let read_uint_32 t =
    if t.off + 4 > t.max then raise EOS;
    let off = t.off and s = t.data in
      t.off <- t.off + 4;
      gb s off + (gb s (off + 1) lsl 8) +
      (gb s (off + 2) lsl 16) + (gb s (off + 3) lsl 24)

  let get_vint t =
    let rec loop_get_vint t n shift off =
      if off >= t.max then raise EOS;
      let m = gb t.data off in
      let n = n lor ((m land 0x7F) lsl shift) in
        if m < 128 then (t.off <- off + 1; n)
        else loop_get_vint t n (shift + 7) (off+1)
    in loop_get_vint t 0 0 t.off

  let read_raw_string dst t len =
    if t.off + len > t.max then raise EOS;
    if String.length !dst < len then dst := String.create len;
    String.blit t.data t.off !dst 0 len;
    t.off <- t.off + len

  let read_string dst t =
    let len = get_vint t in
      read_raw_string dst t len

  let get_string t =
    let s = ref "" in
      read_string s t;
      !s

  let get_table t = table_of_string (get_string t)

  let get_timestamp t =
    let s = ref "" in
      read_raw_string s t 8;
      Obs_datum_encoding.decode_timestamp' !s

  let get_list f t =
    let rec loop_get_list f t acc = function
        0 -> List.rev acc
      | n -> let x = f t in loop_get_list f t (x :: acc) (n - 1) in
    let len = get_vint t in
      loop_get_list f t [] len
end

let string_of_cursor c =
  let b = Obs_bytea.create 13 in
  let add_string b s =
    Obs_bytea.add_vint b (String.length s);
    Obs_bytea.add_string b s
  in
    Obs_bytea.add_vint b (List.length c.bc_remaining_tables);
    List.iter (add_string b) (c.bc_remaining_tables :> string list);
    add_string b c.bc_key;
    add_string b c.bc_column;
    Obs_bytea.contents b

let cursor_of_string c =
    let src = DEC.make c in
    let bc_remaining_tables = DEC.get_list DEC.get_table src in
    let bc_key = DEC.get_string src in
    let bc_column = DEC.get_string src in
      Some { bc_remaining_tables; bc_key; bc_column; }

module type ENCODER =
sig
  type encoder

  val make : ?buffer:Obs_bytea.t -> unit -> encoder

  val add_datum : encoder -> table -> LevelDB.iterator ->
    key_buf:string -> key_len:int ->
    column_buf:string -> column_len:int ->
    timestamp_buf:Obs_datum_encoding.timestamp_buf ->
    value_buf:string ref -> unit

  val finish : encoder -> Obs_bytea.t
  val approximate_size : encoder -> int
  val is_empty : encoder -> bool
end

module Format_0 =
struct
  type encoder = Obs_bytea.t

  let format_id = 0

  let make ?(buffer = Obs_bytea.create 16) () =
    Obs_bytea.clear buffer;
    Obs_bytea.add_int32_le buffer format_id;
    buffer

  let add_datum dst table it
        ~key_buf ~key_len ~column_buf ~column_len ~timestamp_buf ~value_buf =
    Obs_bytea.add_vint dst (String.length (table : table :> string));
    Obs_bytea.add_string dst (table :> string);
    Obs_bytea.add_vint dst key_len;
    Obs_bytea.add_substring dst key_buf 0 key_len;
    Obs_bytea.add_vint dst column_len;
    Obs_bytea.add_substring dst column_buf 0 column_len;
    Obs_bytea.add_string dst (timestamp_buf : Obs_datum_encoding.timestamp_buf :> string);
    let len = IT.fill_value it value_buf in
      Obs_bytea.add_vint dst len;
      Obs_bytea.add_substring dst !value_buf 0 len

  let finish t = t

  let approximate_size = Obs_bytea.length

  let is_empty t = Obs_bytea.length t <= 4

  let load encode_datum_key writebatch src =
    let datum_key = Obs_bytea.create 13 in

    let load_datum src =
      let table = DEC.get_table src in
      let key = DEC.get_string src in
      let column = DEC.get_string src in
      let timestamp = DEC.get_timestamp src in
      let value_len = DEC.get_vint src in
        encode_datum_key datum_key ~table ~key ~column ~timestamp;
        L.Batch.put_substring writebatch
          (Obs_bytea.unsafe_string datum_key) 0 (Obs_bytea.length datum_key)
          src.DEC.data src.DEC.off value_len;
        src.DEC.off <- src.DEC.off + value_len

    in
      try
        while true do
          load_datum src
        done
      with DEC.EOS -> ()
end

let load encode_datum_key writebatch data =
  try
    let src = DEC.make data in
    let format = DEC.read_uint_32 src in
      match format with
          0 -> Format_0.load encode_datum_key writebatch src; true
        | _ -> false
  with DEC.EOS -> false

let encoder format = match format with
    _ -> (module Format_0 : ENCODER)
