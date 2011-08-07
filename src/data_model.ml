
(** {2 Type definitions} *)

type table = string

type key = string

type column = { name : column_name; data : string; timestamp : timestamp; }

and column_name = string

and timestamp = No_timestamp | Timestamp of Int64.t

type key_data = { key : key; last_column : string; columns : column list }

type slice = key option * key_data list (** last_key * data *)

(** Range representing elements between [first] (inclusive) and [up_to]
  * (exclusive). If [first] is not provided, the range starts with the first
  * available element; likewise, if [up_to] is not provided, the elements
  * until the last one are selected. *)
type range =
  {
    first : string option;
    up_to : string option;
  }

type key_range =
    Key_range of range
  | Keys of string list

type column_range =
    All_columns
  | Columns of string list
  | Column_range of range

(* {2 Data model } *)

module type S =
sig
  type db
  type keyspace

  val list_keyspaces : db -> string list
  val register_keyspace : db -> string -> keyspace
  val get_keyspace : db -> string -> keyspace option

  val keyspace_name : keyspace -> string
  val keyspace_id : keyspace -> int

  val list_tables : keyspace -> table list

  (** @return approximate size on disk of the data for the given table. *)
  val table_size_on_disk : keyspace -> table -> Int64.t

  val key_range_size_on_disk :
    keyspace -> table -> ?first:string option -> ?up_to:string option -> Int64.t

  (** {3 Transactions} *)
  type transaction

  val read_committed_transaction :
    keyspace -> (transaction -> 'a Lwt.t) -> 'a Lwt.t

  val repeatable_read_transaction :
    keyspace -> (transaction -> 'a Lwt.t) -> 'a Lwt.t

  (** {3 Read operations} *)

  (** Return up to [max_keys] keys (default: [max_int]) in the given range. *)
  val get_keys :
    transaction -> table ->
    ?max_keys:int ->
    key_range -> string list Lwt.t

  (** Count the keys in the given range: [count_keys tx table range] is
    * functionality equivalent to [get_keys tx table range >|= List.length]
    * but somewhat more efficient, by a constant factor. *)
  val count_keys : transaction -> table -> key_range -> Int64.t Lwt.t

  (** [get_slice tx table ?max_keys ?max_columns ?decode_timestamp
    *  key_range column_range] returns a data slice corresponding to the keys
    * included in the [key_range] which contain at least one of the columns
    * specified in the [column_range].
    * @param max_keys return no more than [max_keys] keys
    * @param max_columns return no more than [max_columns] columns per key
    * @param decode_timestamp whether to decode the timestamp (default: false)
    *  *)
  val get_slice :
    transaction -> table ->
    ?max_keys:int -> ?max_columns:int -> ?decode_timestamps:bool ->
    key_range -> column_range -> slice Lwt.t

  (** [get_slice_values tx table key_range ["col1"; "col2"]]
    * returns [Some last_key, l] if at least a key was selected, where [l] is
    * an associative list whose elements are pairs containing the key and a list
    * of value options corresponding to the requested columns (in the order they
    * were given to [get_slice_values]). A key is selected if:
    * * it is specified in a [Keys l] range
    * * it exists in the given [Key_range r] range *)
  val get_slice_values :
    transaction -> table ->
    ?max_keys:int ->
    key_range -> column_name list ->
    (key option * (key * string option list) list) Lwt.t

  (** @return [Some last_column_name, column_list] if any column exists for the
    * selected key, [None] otherwise. *)
  val get_columns :
    transaction -> table ->
    ?max_columns:int -> ?decode_timestamps:bool ->
    key -> column_range ->
    (column_name * (column list)) option Lwt.t

  (** [get_column_values tx table key columns] returns the data associated to
    * the given [columns] (if existent). If [key] doesn't exist (that is, it has
    * got no associated columns), all the values will be [None]. *)
  val get_column_values :
    transaction -> table ->
    key -> column_name list ->
    string option list Lwt.t

  val get_column :
    transaction -> table ->
    key -> column_name -> (string * timestamp) option Lwt.t

  (** {3} Write operations *)

  val put_columns :
    transaction -> table -> key -> column list ->
    unit Lwt.t

  val delete_columns :
    transaction -> table -> key -> column_name list -> unit Lwt.t

  val delete_key : transaction -> table -> key -> unit Lwt.t
end
