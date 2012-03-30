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

(** Low-level encoding/decoding of datum_keys used in LevelDB storage. *)

(** Buffers used to decode timestamps. *)
type timestamp_buf = private string

type ks = int

(** Create a timestamp buffer. *)
val make_timestamp_buf : unit -> timestamp_buf

(** Decode the timestamp contained in the buffer. *)
val decode_timestamp : timestamp_buf -> Int64.t

(** Decode the timestamp contained in the string. *)
val decode_timestamp' : string -> Int64.t

(** Custom comparator used in LevelDB storage. *)
val custom_comparator : LevelDB.comparator

(** Apply the custom comparator to encoded datum keys. *)
val apply_custom_comparator : string -> string -> int

(** Prefix to all the LevelDB keys in the keyspace table. *)
val keyspace_table_prefix : string

(** [keyspace_table_key name] returns the LevelDB key for the keyspace named
  * [name]. *)
val keyspace_table_key : string -> string

(** Key guaranteed to be the last record in the DB. *)
val end_of_db_key : string

(** [decode_keyspace_table_name leveldbkey] returns the keyspace name if the
  * [leveldbkey] is a valid keyspace table key (i.e., is prefixed by
  * [keyspace_table_prefix]. *)
val decode_keyspace_table_name : string -> string option

module Keyspace_tables :
sig
  val ks_table_table_prefix : string
  val ks_table_table_prefix_for_ks : string -> string
  val ks_table_table_key : keyspace:string -> table:string -> string
  val decode_ks_table_key : string -> (string * string) option
end

(** Create a LevelDB datum key for the given
  * (keyspace, table, key, column, timestamp) tuple, storing it in the given
  * {!Obs_bytea} buffer, which will be cleared automatically. *)
val encode_datum_key :
  Obs_bytea.t -> ks ->
  table:int -> key:string -> column:string -> timestamp:Int64.t -> unit

(** Similar to [encode_datum_key], returning directly a string. *)
val encode_datum_key_to_string :
  ks -> table:int -> key:string -> column:string -> timestamp:Int64.t -> string

(** [encode_table_successor dst ks table] places the prefix of the first
  * datum_key after the table [table] in [dst], which will be cleared
  * beforehand. *)
val encode_table_successor : Obs_bytea.t -> ks -> int -> unit

(** Similar to {!encode_table_successor}, returning a string. *)
val encode_table_successor_to_string : ks -> int -> string

(** [get_datum_key_keyspace_id k] returns the keyspace id of the datum key
  * [k]. Note that a bogus value will be returned if [k] is not a datum key;
  * you'll have to use {!decode_datum_key} to make sure it is one. *)
val get_datum_key_keyspace_id : string -> int

(** [decode_datum_key ... key off]
  * Decode a datum key, filling the buffers corresponding to the different
  * parts (as well as the length references) if provided, for the key in [key]
  * starting at offset [off].
  * @return false if the key is not a datum_key, true otherwise *)
val decode_datum_key :
  table_r:int ref ->
  key_buf_r:string ref option ->
  key_len_r:int ref option ->
  column_buf_r:string ref option ->
  column_len_r:int ref option ->
  timestamp_buf:timestamp_buf option -> string -> int -> bool
