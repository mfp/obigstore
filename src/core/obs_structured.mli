(** Structured data de/encoding in [@columns]. *)

open Obs_data_model

module type RAW =
sig
  type keyspace

  val get_slice :
    keyspace -> table ->
    ?max_keys:int -> ?max_columns:int -> ?decode_timestamps:bool ->
    string key_range -> ?predicate:row_predicate -> column_range ->
    (string, string) slice Lwt.t

  val get_slice_values :
    keyspace -> table ->
    ?max_keys:int ->
    string key_range -> column_name list ->
    (key option * (key * string option list) list) Lwt.t

  val get_slice_values_with_timestamps :
    keyspace -> table ->
    ?max_keys:int ->
    string key_range -> column_name list ->
    (key option * (key * (string * Int64.t) option list) list) Lwt.t

  val get_columns :
    keyspace -> table ->
    ?max_columns:int -> ?decode_timestamps:bool ->
    key -> column_range ->
    (column_name * (string column list)) option Lwt.t

  val get_column_values :
    keyspace -> table ->
    key -> column_name list ->
    string option list Lwt.t

  val get_column :
    keyspace -> table ->
    key -> column_name -> (string * timestamp) option Lwt.t

  val put_columns :
    keyspace -> table -> key -> string column list ->
    unit Lwt.t

  val put_multi_columns :
    keyspace -> table -> (key * string column list) list -> unit Lwt.t
end

module type STRUCTURED =
sig
  type keyspace

  val get_bson_slice :
    keyspace -> table ->
    ?max_keys:int -> ?max_columns:int -> ?decode_timestamps:bool ->
    string key_range -> ?predicate:row_predicate -> column_range ->
    (string, decoded_data) slice Lwt.t

  val get_bson_slice_values :
    keyspace -> table ->
    ?max_keys:int ->
    string key_range -> column_name list ->
    (key option * (key * decoded_data option list) list) Lwt.t

  val get_bson_slice_values_with_timestamps :
    keyspace -> table ->
    ?max_keys:int ->
    string key_range -> column_name list ->
    (key option * (key * (decoded_data * Int64.t) option list) list) Lwt.t

  val get_bson_columns :
    keyspace -> table ->
    ?max_columns:int -> ?decode_timestamps:bool ->
    key -> column_range ->
    (column_name * (decoded_data column list)) option Lwt.t

  val get_bson_column_values :
    keyspace -> table ->
    key -> column_name list ->
    decoded_data option list Lwt.t

  val get_bson_column :
    keyspace -> table ->
    key -> column_name -> (decoded_data * timestamp) option Lwt.t

  val put_bson_columns :
    keyspace -> table -> key -> decoded_data column list ->
    unit Lwt.t

  val put_multi_bson_columns :
    keyspace -> table -> (key * decoded_data column list) list -> unit Lwt.t
end

module Make : functor(M : RAW) ->
  STRUCTURED with type keyspace = M.keyspace
