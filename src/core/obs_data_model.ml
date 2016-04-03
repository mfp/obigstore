(*
 * Copyright (C) 2011-2013 Mauricio Fernandez <mfp@acm.org>
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

(** Exception raised when inserting an invalid BSON [@column]. *)
exception Invalid_BSON_column of string

(** Exception raised when a watched key/column has been modified while a write
  * transaction is being performed. *)
exception Dirty_data

(** Exception raised by the {!lock} operation if a deadlock is detected. *)
exception Deadlock

(** Operation denied owning to insufficient permissions or invalid
 * credentials. *)
exception Denied

type table = string

let table_of_string x = x
let string_of_table x = x

type key = string

type 'data column = { name : column_name; data : 'data; timestamp : timestamp; }

and column_name = string

and timestamp = No_timestamp | Timestamp of Int64.t

type decoded_data =
    Binary of string | BSON of Obs_bson.document | Malformed_BSON of string

type ('key, 'data) key_data = {
  key : 'key;
  last_column : string; (** Name of last column in the following list *)
  columns : 'data column list;
}

type ('key, 'data) slice = 'key option * ('key, 'data) key_data list (** last_key * data *)

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
type 'key range =
  {
    first : 'key option;
    up_to : 'key option;
    reverse : bool;
  }

type 'key cont_or_discrete_range = [ `Continuous of 'key range | `Discrete of 'key list ]

type 'key key_range = [ 'key cont_or_discrete_range | `All]

type column_range = [ string key_range | `Union of string cont_or_discrete_range list ]

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

type raw_dump_timestamp = Int64.t

type tx_id  = Int64.t
type lock_kind = [`SHARED | `EXCLUSIVE]
type tx_info   =
    {
      tx_id : tx_id;
      started_at : float;
      wanted_locks : (string * lock_kind) list;
      held_locks : (string * lock_kind) list;
    }

type sync_mode = [`Sync | `Async]

(* {2 Data model } *)

module type RAW_DUMP =
sig
  type db
  type raw_dump

  (** Request that the current state of the DB be dumped. *)
  val dump : db -> mode:[`Sync | `Async | `No_stream] -> raw_dump Lwt.t

  (** Allow to release the resources associated to the dump (e.g., delete
    * the actual data). Further operations on the dump will fail. *)
  val release : raw_dump -> keep_files:bool -> unit Lwt.t

  val timestamp   : raw_dump -> raw_dump_timestamp Lwt.t
  val localdir    : raw_dump -> string Lwt.t
  val list_files  : raw_dump -> (string * Int64.t) list Lwt.t
  val file_digest : raw_dump -> string -> string option Lwt.t

  val open_file :
    raw_dump -> ?offset:Int64.t -> string ->
    Lwt_io.input_channel option Lwt.t
end

(** DB operations with opaque columns. *)
module type RAW_S =
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
  val read_committed_transaction :
    ?sync:sync_mode -> keyspace -> (keyspace -> 'a Lwt.t) -> 'a Lwt.t

  val repeatable_read_transaction :
    ?sync:sync_mode -> keyspace -> (keyspace -> 'a Lwt.t) -> 'a Lwt.t

  (** [transaction_id ks] returns the ID of the current and outermost
    * transaction (useful for logging and reporting), or None if not inside a
    * transaction. *)
  val transaction_id : keyspace -> (int * int) option Lwt.t

  (** Acquire locks with the given names. The locks will be released
    * automatically when the outermost transaction is committed or aborted.
    * This is a NOP unless inside a transaction.
    *
    * @param shared indicates whether shared or exclusive locks are to be
    * acquired *)
  val lock : keyspace -> shared:bool -> string list -> unit Lwt.t

  (** [watch_keys ks table keys] will make write transactions raise
    * [Dirty_data] if a column belonging to any of the given keys is modified
    * (added, updated or deleted) after the call to [watch_keys].
    *
    * It is used to perform optimistic concurrency control as follows:
    * {[
    *   let attempt () =
    *     read_committed_transaction ks begin fun ks ->
    *       watch_keys ks accounts [account_key] >>
    *       lwt n = get_column ks accounts account_key "balance" >|=
    *               fst >|= int_of_string
    *       in
    *         put_columns ks accounts account_key
    *           [ { name = "balance"; data = string_of_int (n + 1);
    *               timestamp = No_timestamp; } ]
    *     end in
    *   let rec retry_if_needed () =
    *     try_lwt attempt () with Dirty_data -> retry_if_needed ()
    *   in
    *      (* perform transaction *)
    *      retry_if_needed ()
    * ]}
    * *)
  val watch_keys : keyspace -> table -> string list -> unit Lwt.t

  (** [watch_columns ks table l] is similar to {!watch_keys}, but instead of
    * watching the whole keys, only the specified columns are considered, e.g.
    * [watch_keys ks table ["key1", ["col1", "col2"]; "key2", ["col2"]]]. *)
  val watch_columns : keyspace -> table -> (string * string list) list -> unit Lwt.t

  (** [watch_prefixes ks table prefixes] is similar to {!watch_keys}, but is
    * given a prefix of the keys to watch; e.g., if you do
    * [watch_prefixes ks tbl ["foo"; "bar"]], write transactions will abort
    * with [Dirty_data] if any keys starting with [foo] or [bar] (inclusive)
    * are modified by a concurrent transaction *)
  val watch_prefixes : keyspace -> table -> string list -> unit Lwt.t

  (** {3 Read operations} *)

  (** Return up to [max_keys] keys (default: [max_int]) in the given range. *)
  val get_keys :
    keyspace -> table ->
    ?max_keys:int ->
    string key_range -> string list Lwt.t

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
  val count_keys : keyspace -> table -> string key_range -> Int64.t Lwt.t

  (** [get_slice tx table ?max_keys ?max_columns ?decode_timestamp
    *  key_range ?predicate column_range] returns a data slice corresponding
    *  to the keys included in the [key_range] which contain at least one of
    *  the columns specified in the [column_range] and satisfy the
    *  [predicate].
    *
    * If the key range is [`Discrete l] and the column range is a [Column_range]
    * the columns will be returned:
    * * in lexicographic order, if the column range is not reverse
    * * in reverse lexicographic order, if the column range is reverse
    *
    * For the sake of efficiency, if the key range is [`Continuous _], the
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
    string key_range -> ?predicate:row_predicate -> column_range ->
    (string, string) slice Lwt.t

  (** [get_slice_values tx table key_range ["col1"; "col2"]]
    * returns [Some last_key, l] if at least a key was selected, where [l] is
    * an associative list whose elements are pairs containing the key and a list
    * of value options corresponding to the requested columns (in the order they
    * were given to [get_slice_values]). A key is selected if:
    * * it is specified in a [`Discrete l] range
    * * it exists in the given [`Continuous r] range *)
  val get_slice_values :
    keyspace -> table ->
    ?max_keys:int ->
    string key_range -> column_name list ->
    (key option * (key * string option list) list) Lwt.t

  (** Similar to [get_slice_values], but returning the data and the
    * timestamp in microseconds since the beginning of the Unix epoch. *)
  val get_slice_values_with_timestamps :
    keyspace -> table ->
    ?max_keys:int ->
    string key_range -> column_name list ->
    (key option * (key * (string * Int64.t) option list) list) Lwt.t

  (** @return [Some last_column_name, column_list] if any column exists for the
    * selected key, [None] otherwise. *)
  val get_columns :
    keyspace -> table ->
    ?max_columns:int -> ?decode_timestamps:bool ->
    key -> column_range ->
    (column_name * (string column list)) option Lwt.t

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
    keyspace -> table -> key -> string column list ->
    unit Lwt.t

  val put_multi_columns :
    keyspace -> table -> (key * string column list) list -> unit Lwt.t

  val delete_columns :
    keyspace -> table -> key -> column_name list -> unit Lwt.t

  val delete_key : keyspace -> table -> key -> unit Lwt.t

  val delete_keys : keyspace -> table -> string key_range -> unit Lwt.t

  (** {3} Asynchronous notifications *)

  (** [listen ks topìc] allows to receive notifications sent to the specified
    * [topic] in the keyspace [ks]. Note that [listen] is not affected by
    * surrounding transactions, i.e., the subscription is performed even if
    * the surrounding transaction is canceled.
    * Note that subscriptions are per [keyspace], not per keyspace name: it is
    * possible to subscribe to different topics in two different [keyspace]
    * objects which operate on the same DB keyspace.
    * *)
  val listen : keyspace -> string -> unit Lwt.t

  (** [listen_prefix ks prefix] allows to receive notifications sent to topics
    * which are (possibly improper) suffixes of [prefix] in the keyspace [ks].
    * Note that [listen_prefix] is not affected by surrounding transactions,
    * i.e., the subscription is performed even if the surrounding transaction
    * is canceled.
    *
    * If a notification would match several regular topics and prefixes, only
    * one notification is returned.
    *
    * Note that subscriptions are per [keyspace], not per keyspace name: it is
    * possible to subscribe to different topics in two different [keyspace]
    * objects which operate on the same DB keyspace.
    * *)
  val listen_prefix : keyspace -> string -> unit Lwt.t

  (** [unlisten ks topìc] signals that further notifications sent to the [topic]
    * in the keyspace [ks] will not be received. Notifications that were
    * already queued in the server will not be discarded, however.
    * Note that [unlisten] is not affected by surrounding transactions, i.e.,
    * the unsubscription is performed even if the surrounding transaction is
    * canceled. *)
  val unlisten : keyspace -> string -> unit Lwt.t

  (** [unlisten_prefix ks prefix] signals that further notifications sent to
    * topics which are (possibly improper) suffixes of [prefix] are not to be
    * received anymore. Notifications that were already queued in the server
    * will not be discarded, however.
    *
    * Note that [unlisten_prefix] is not affected by surrounding transactions,
    * i.e., the unsubscription is performed even if the surrounding
    * transaction is canceled. *)
  val unlisten_prefix : keyspace -> string -> unit Lwt.t

  (** [notify ks topic] sends a notification associated to the given [topic]
    * in keyspace [ks], which will be received by all the connections that
    * performed [listen] on the same [ks]/[topic]. [notify] honors surrounding
    * transactions, i.e., the notification will be actually performed only
    * when/if the outermost surrounding transaction is committed, and no
    * notification is sent if any of the surrounding transactions is aborted.
    *
    * Multiple notifications for the same topic within a transaction might be
    * coalesced, and no assumption should be made about the order in which the
    * notifications are reported to the listeners.
    * *)
  val notify : keyspace -> string -> unit Lwt.t

  (** Return queued notifications, blocking if there is none yet.
    * An empty list will be returned when there are no more queued
    * notifications and the underlying connection is closed.
    * *)
  val await_notifications : keyspace -> string list Lwt.t

  (** {3} Obs_backup *)
  type backup_cursor

  val dump :
    keyspace ->
    ?format:backup_format ->
    ?only_tables:table list ->
    ?offset:backup_cursor -> unit ->
    (string * backup_cursor option) option Lwt.t

  (** [load tx data] returns [false] if the data is in an unknown format. *)
  val load : keyspace -> string -> bool Lwt.t

  module Raw_dump : RAW_DUMP with type db := db

  (** {3 Administration} *)

  (** Load statistics  *)
  val load_stats : keyspace -> Obs_load_stats.stats Lwt.t

  (** [dump_info property] returns information about the state of the DB if
    * [property] is understood by the DB implementation.  *)
  val get_property : db -> string -> string option Lwt.t

  (** Trigger whole keyspace compaction. *)
  val compact : keyspace -> unit Lwt.t

  (** [compact_table ks table ?from_key ?to_key ()] compacts the table between
    * keys [from_key] and [to_key] (inclusive, defaulting to the beginning/end
    * of the table if not suppplied). *)
  val compact_table :
    keyspace -> table -> ?from_key:string -> ?to_key:string -> unit -> unit Lwt.t
  (** List currently executing transactions. *)
  val list_transactions : keyspace -> tx_info list Lwt.t

  (** List keys of tables modified so far in the specified transaction.  *)
  val changed_tables : keyspace -> tx_id -> string list Lwt.t
end

(** DB operations with BSON-encoded columns: columns whose name begins with
  * '@' are BSON-encoded. All the extra functions in {!S} are similar to those
  * in {!RAW_S} but decode/encode properly such columns. *)
module type S =
sig
  include RAW_S

  (** {3 Read operations} *)

  (** Similar to {!get_slice}, but decodes the BSON-encoded records in columns
    * whose name begins with [@]. *)
  val get_bson_slice :
    keyspace -> table ->
    ?max_keys:int -> ?max_columns:int -> ?decode_timestamps:bool ->
    string key_range -> ?predicate:row_predicate -> column_range ->
    (string, decoded_data) slice Lwt.t

  (** Refer to {!get_slice_values}. *)
  val get_bson_slice_values :
    keyspace -> table ->
    ?max_keys:int ->
    string key_range -> column_name list ->
    (key option * (key * decoded_data option list) list) Lwt.t

  (** Refer to {!get_slice_values_with_timestamps}. *)
  val get_bson_slice_values_with_timestamps :
    keyspace -> table ->
    ?max_keys:int ->
    string key_range -> column_name list ->
    (key option * (key * (decoded_data * Int64.t) option list) list) Lwt.t

  (** Refer to {!get_columns}. *)
  val get_bson_columns :
    keyspace -> table ->
    ?max_columns:int -> ?decode_timestamps:bool ->
    key -> column_range ->
    (column_name * (decoded_data column list)) option Lwt.t

  (** Refer to {!get_column_values}. *)
  val get_bson_column_values :
    keyspace -> table ->
    key -> column_name list ->
    decoded_data option list Lwt.t

  (** Refer to {!get_column}. *)
  val get_bson_column :
    keyspace -> table ->
    key -> column_name -> (decoded_data * timestamp) option Lwt.t

  (** {3 Write operations} *)

  (** Refer to {!put_columns}.
    * @raise Invalid_BSON_column if the data for any @column is not [BSON x]. *)
  val put_bson_columns :
    keyspace -> table -> key -> decoded_data column list ->
    unit Lwt.t

  (** Refer to {!put_multi_columns}.
    * @raise Invalid_BSON_column if the data for any @column is not [BSON x]. *)
  val put_multi_bson_columns :
    keyspace -> table -> (key * decoded_data column list) list -> unit Lwt.t
end

module type BACKUP_SUPPORT =
sig
  type backup_cursor
  val string_of_cursor : backup_cursor -> string
  val cursor_of_string : string -> backup_cursor option
end
