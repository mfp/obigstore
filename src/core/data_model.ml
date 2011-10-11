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

(** {2 Type definitions} *)

type table = string

type key = string

type column = { name : column_name; data : string; timestamp : timestamp; }

and column_name = string

and timestamp = No_timestamp | Timestamp of Int64.t

type key_data = {
  key : key;
  last_column : string; (** Name of last column in the following list *)
  columns : column list;
}

type slice = key option * key_data list (** last_key * data *)

(** Range representing elements between [first] (inclusive if reverse is
  * false, exclusive otherwise) and [up_to]
  * (exclusive if reverse is false, inclusive otherwise). If [first] is not
  * provided, the range starts with the first available element (last, if
  * [reverse] is [true]); likewise, if [up_to] is not provided, the elements
  * until the last (first, if [reverse is [true]) one are selected.
  *
  * Summarizing:
  *
  * * if reverse is false, elements x such that  [first <= x < up_to]
  * * if reverse is true, elements x such that   [first > x >= up_to]
  *
  * where a side of the inequality disappears if the corresponding value
  * ([first] or [up_to]) is [None].
  * *)
type range =
  {
    first : string option;
    up_to : string option;
    reverse : bool;
  }

type key_range =
    Key_range of range
  | Keys of string list

type simple_column_range =
    Columns of string list
  | Column_range of range

type column_range =
    All_columns
  | Column_range_union of simple_column_range list

(** Predicate on the value of a colum *)
type column_val_rel =
  | Any (** don't care about the value *)
  | EQ of string | LT of string | GT of string
  | GE of string | LE of string
  | Between of string * bool * string * bool (** each bool indicating whether inclusive *)

type simple_row_predicate =
  | Column_val of string * column_val_rel (** [name, predicate] *)

type row_predicate_and = Satisfy_all of simple_row_predicate list (* all of them *)

type row_predicate = Satisfy_any of row_predicate_and list (* any of them *)

type backup_format = int

(* {2 Data model } *)

module type S =
sig
  type db
  type keyspace

  val list_keyspaces : db -> string list Lwt.t
  val register_keyspace : db -> string -> keyspace Lwt.t
  val get_keyspace : db -> string -> keyspace option Lwt.t

  val keyspace_name : keyspace -> string
  val keyspace_id : keyspace -> int

  (** List the tables that contained data at the time the outermost
    * transaction began. *)
  val list_tables : keyspace -> table list Lwt.t

  (** @return approximate size on disk of the data for the given table. *)
  val table_size_on_disk : keyspace -> table -> Int64.t Lwt.t

  (** @return approximate size on disk of the data for the given key range in
    * the specified table. *)
  val key_range_size_on_disk :
    keyspace -> ?first:string -> ?up_to:string -> table -> Int64.t Lwt.t

  (** {3 Transactions} *)
  val read_committed_transaction : keyspace -> (keyspace -> 'a Lwt.t) -> 'a Lwt.t

  val repeatable_read_transaction : keyspace -> (keyspace -> 'a Lwt.t) -> 'a Lwt.t

  (** Acquire a lock with the given name. The lock will be released
    * automatically when the outermost transaction is committed or aborted.
    * This is a NOP unless inside a transaction. *)
  val lock : keyspace -> string -> unit Lwt.t

  (** {3 Read operations} *)

  (** Return up to [max_keys] keys (default: [max_int]) in the given range. *)
  val get_keys :
    keyspace -> table ->
    ?max_keys:int ->
    key_range -> string list Lwt.t

  (** [exists_key ks table k] returns [true] if any column with the given
    * [key] exists in the given [table] within the keyspace [ks]. *)
  val exists_key : keyspace -> table -> string -> bool Lwt.t

  (** [exist_keys ks table keys] returns a list of bools indicating whether
    * each key in [keys] has got any column in the given [table] within the
    * keyspace [ks]. *)
  val exist_keys : keyspace -> table -> string list -> bool list Lwt.t

  (** Count the keys in the given range: [count_keys tx table range] is
    * functionality equivalent to [get_keys tx table range >|= List.length]
    * but somewhat faster, by a constant factor, and more memory-efficient. *)
  val count_keys : keyspace -> table -> key_range -> Int64.t Lwt.t

  (** [get_slice tx table ?max_keys ?max_columns ?decode_timestamp
    *  key_range ?predicate column_range] returns a data slice corresponding
    *  to the keys included in the [key_range] which contain at least one of
    *  the columns specified in the [column_range] and satisfy the
    *  [predicate].
    *
    * If the key range is [Keys l] and the column range is a [Column_range]
    * the columns will be returned:
    * * in lexicographic order, if the column range is not reverse
    * * in reverse lexicographic order, if the column range is reverse
    *
    * For the sake of efficiency, if the key range is [Key_range _], the
    * columns are selected:
    * * in lexicographic order, if the key range is not [reverse]
    * * in reverse lexicographic order, if the key range is [reverse]
    *
    * @param max_keys return no more than [max_keys] keys
    * @param max_columns return no more than [max_columns] columns per key
    * @param decode_timestamp whether to decode the timestamp (default: false)
    *  *)
  val get_slice :
    keyspace -> table ->
    ?max_keys:int -> ?max_columns:int -> ?decode_timestamps:bool ->
    key_range -> ?predicate:row_predicate -> column_range -> slice Lwt.t

  (** [get_slice_values tx table key_range ["col1"; "col2"]]
    * returns [Some last_key, l] if at least a key was selected, where [l] is
    * an associative list whose elements are pairs containing the key and a list
    * of value options corresponding to the requested columns (in the order they
    * were given to [get_slice_values]). A key is selected if:
    * * it is specified in a [Keys l] range
    * * it exists in the given [Key_range r] range *)
  val get_slice_values :
    keyspace -> table ->
    ?max_keys:int ->
    key_range -> column_name list ->
    (key option * (key * string option list) list) Lwt.t

  (** Similar to [get_slice_values], but returning the data and the
    * timestamp in microsends since the beginning of the Unix epoch. *)
  val get_slice_values_with_timestamps :
    keyspace -> table ->
    ?max_keys:int ->
    key_range -> column_name list ->
    (key option * (key * (string * Int64.t) option list) list) Lwt.t

  (** @return [Some last_column_name, column_list] if any column exists for the
    * selected key, [None] otherwise. *)
  val get_columns :
    keyspace -> table ->
    ?max_columns:int -> ?decode_timestamps:bool ->
    key -> column_range ->
    (column_name * (column list)) option Lwt.t

  (** [get_column_values tx table key columns] returns the data associated to
    * the given [columns] (if existent). If [key] doesn't exist (that is, it has
    * got no associated columns), all the values will be [None]. *)
  val get_column_values :
    keyspace -> table ->
    key -> column_name list ->
    string option list Lwt.t

  val get_column :
    keyspace -> table ->
    key -> column_name -> (string * timestamp) option Lwt.t

  (** {3} Write operations *)

  val put_columns :
    keyspace -> table -> key -> column list ->
    unit Lwt.t

  val put_multi_columns :
    keyspace -> table -> (key * column list) list -> unit Lwt.t

  val delete_columns :
    keyspace -> table -> key -> column_name list -> unit Lwt.t

  val delete_key : keyspace -> table -> key -> unit Lwt.t

  (** {3} Backup *)
  type backup_cursor

  val dump :
    keyspace ->
    ?format:backup_format ->
    ?only_tables:table list ->
    ?offset:backup_cursor -> unit ->
    (string * backup_cursor option) option Lwt.t

  (** [load tx data] returns [false] if the data is in an unknown format. *)
  val load : keyspace -> string -> bool Lwt.t

  (** Load statistics  *)
  val load_stats : keyspace -> Load_stats.stats Lwt.t
end

module type BACKUP_SUPPORT =
sig
  type backup_cursor
  val string_of_cursor : backup_cursor -> string
  val cursor_of_string : string -> backup_cursor option
end
