(*
 * Copyright (M.Codec) 2011-2012 Mauricio Fernandez <mfp@acm.org>
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

open Obs_data_model

module type TABLE_CONFIG =
sig
  type 'a row

  module Codec : Obs_key_encoding.CODEC_OPS

  val name : string
  val row_of_key_data : Codec.key key_data -> Codec.key row option
  val row_needs_timestamps : bool
end

module Trivial_row :
sig
  type 'a row = 'a key_data list
  val row_of_key_data : 'a -> 'a option
end

val key_range_with_prefix :
  ('a, 'b, 'c) Obs_key_encoding.codec ->
  ?starting_with:'b option ->
  ((('d, _, _) Obs_key_encoding.codec -> 'd) ->
   ('a, 'b, 'c) Obs_key_encoding.codec -> 'a) -> 'b key_range

val rev_key_range_with_prefix :
  ('a, 'b, 'c) Obs_key_encoding.codec ->
  ?starting_with:'b ->
  ((('d, _, _) Obs_key_encoding.codec -> 'd) ->
   ('a, 'b, 'c) Obs_key_encoding.codec -> 'a) -> 'b key_range

module Make :
  functor (M : TABLE_CONFIG) ->
  functor (OP : Obs_data_model.S) ->
sig
  open M.Codec

  module Codec : Obs_key_encoding.CODEC_OPS
    with type key = M.Codec.key
     and type internal_key = M.Codec.internal_key
     and type tail = M.Codec.tail

  type keyspace = OP.keyspace
  type key = M.Codec.key
  type key_range = [`Continuous of key range | `Discrete of key list]

  val table : table

  val key_range_with_prefix :
    ?starting_with:key option ->
    ((('a, 'b, 'c) Obs_key_encoding.codec -> 'a) ->
     (internal_key, key, M.Codec.tail) Obs_key_encoding.codec ->
     internal_key) -> key_range

  val rev_key_range_with_prefix :
    ?starting_with:key ->
    ((('a, 'b, 'c) Obs_key_encoding.codec -> 'a) ->
     (internal_key, key, tail) Obs_key_encoding.codec -> internal_key) ->
    key_range

  val size_on_disk : keyspace -> Int64.t Lwt.t

  val key_range_size_on_disk :
    keyspace ->
    ?first:key -> ?up_to:key -> unit -> Int64.t Lwt.t

  val read_committed_transaction :
    keyspace -> (keyspace -> 'a Lwt.t) -> 'a Lwt.t

  val repeatable_read_transaction :
    keyspace -> (keyspace -> 'a Lwt.t) -> 'a Lwt.t

  val lock : keyspace -> shared:bool -> string list -> unit Lwt.t

  val get_keys :
    keyspace -> ?max_keys:int -> key_range -> key list Lwt.t

  val exists_key : keyspace -> key -> bool Lwt.t
  val exists_keys : keyspace -> key list -> bool list Lwt.t

  val count_keys : keyspace -> key_range -> Int64.t Lwt.t

  val get_slice :
    keyspace ->
    ?max_keys:int ->
    ?max_columns:int ->
    ?decode_timestamps:bool ->
    key_range ->
    ?predicate:row_predicate ->
    column_range ->
    (key option * key key_data list) Lwt.t

  val get_row : keyspace -> key -> key M.row option Lwt.t

  val get_rows : keyspace -> key_range -> (key option * key M.row list) Lwt.t

  val get_slice_values :
    keyspace -> ?max_keys:int -> key_range -> column_name list ->
    (key option * (key * string option list) list) Lwt.t

  val get_slice_values_with_timestamps :
    keyspace -> ?max_keys:int -> key_range -> column_name list ->
    (key option * (key * (string * Int64.t) option list) list)
    Lwt.t

  val get_columns :
    keyspace -> ?max_columns:int -> ?decode_timestamps:bool ->
    key -> column_range -> (column_name * column list) option
    Lwt.t

  val get_column_values :
    keyspace -> key -> column_name list -> string option list Lwt.t

  val get_column :
    keyspace -> key -> column_name -> (string * timestamp) option Lwt.t

  val put_columns : keyspace -> key -> column list -> unit Lwt.t

  val put_multi_columns :
    keyspace -> (key * column list) list -> unit Lwt.t

  val delete_columns :
    keyspace -> key -> column_name list -> unit Lwt.t

  val delete_key : keyspace -> key -> unit Lwt.t
end
