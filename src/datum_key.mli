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

(** Create a LevelDB datum key for the given
  * (keyspace, table, key, column, timestamp) tuple, storing it in the given
  * {!Bytea} buffer, which will be cleared automatically. *)
val encode_datum_key :
  Bytea.t -> ks ->
  table:string -> key:string -> column:string -> timestamp:Int64.t -> unit

(** Similar to [encode_datum_key], returning directly a string. *)
val encode_datum_key_to_string :
  ks -> table:string -> key:string -> column:string -> timestamp:Int64.t -> string

(** [encode_table_successor dst ks table] places the prefix of the first
  * datum_key after the table [table] in [dst], which will be cleared
  * beforehand. *)
val encode_table_successor : Bytea.t -> ks -> string -> unit

(** Similar to {!encode_table_successor}, returning a string. *)
val encode_table_successor_to_string : ks -> string -> string

(** [decode_datum_key ... key off]
  * Decode a datum key, filling the buffers corresponding to the different
  * parts (as well as the length references) if provided, for the key in [key]
  * starting at offset [off].
  * @return false if the key is not a datum_key, true otherwise *)
val decode_datum_key :
  table_buf_r:string ref option ->
  table_len_r:int ref option ->
  key_buf_r:string ref option ->
  key_len_r:int ref option ->
  column_buf_r:string ref option ->
  column_len_r:int ref option ->
  timestamp_buf:timestamp_buf option -> string -> int -> bool
