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

open Lwt

module DM = Obs_data_model
module Option = BatOption
module List = struct include List include BatList end
open DM

module type TABLE_CONFIG =
sig
  type 'key row

  module Codec : Obs_key_encoding.CODEC_OPS
  val name : string

  val row_of_key_data : Codec.key key_data -> Codec.key row option
end

module Trivial_row =
struct
  type 'key row = 'key key_data list
  let row_of_key_data d = Some d
end

let key_range_with_prefix c ?starting_with p =
  let open Obs_key_encoding in
  let first = match starting_with with
      None -> Some (expand (p min_suffix) c);
    | Some key -> key
  in
    `Continuous
      { first; reverse = false;
        up_to = Some (succ_value c (expand (p max_suffix) c));
      }

let rev_key_range_with_prefix c ?starting_with p =
  let open Obs_key_encoding in
  let first = match starting_with with
      None -> Some (succ_value c (expand (p max_suffix) c));
    | Some key -> Some (succ_value c key)
  in
    `Continuous
      { first; reverse = true;
        up_to = Some (expand (p min_suffix) c);
      }

module Make
  (M : TABLE_CONFIG)
  (OP : Obs_data_model.S) =
struct
  open OP
  module C = M.Codec
  module Codec = M.Codec

  type keyspace = OP.keyspace
  type key = M.Codec.key
  type key_range = [`Continuous of key range | `Discrete of key list]

  let key_range_with_prefix ?starting_with p =
    key_range_with_prefix C.codec ?starting_with p

  let rev_key_range_with_prefix ?starting_with p =
    rev_key_range_with_prefix C.codec ?starting_with p

  let table = table_of_string M.name

  let size_on_disk ks = table_size_on_disk ks table

  let key_range_size_on_disk ks ?first ?up_to () =
    key_range_size_on_disk ks
      ?first:(Option.map C.encode_to_string first)
      ?up_to:(Option.map C.encode_to_string up_to)
      table

  let read_committed_transaction = OP.read_committed_transaction
  let repeatable_read_transaction = OP.repeatable_read_transaction
  let lock = OP.lock

  let inject_range = function
      `Discrete l -> `Discrete (List.map C.encode_to_string l)
    | `Continuous kr ->
      `Continuous
        { kr with first = Option.map C.encode_to_string kr.first;
                  up_to = Option.map C.encode_to_string kr.up_to; }

  let get_keys ks ?max_keys range =
    get_keys ks table ?max_keys (inject_range range) >|=
    List.map C.decode_string

  let exists_key ks k = exists_key ks table (C.encode_to_string k)
  let exists_keys ks l = exist_keys ks table (List.map C.encode_to_string l)
  let count_keys ks range = count_keys ks table (inject_range range)

  let get_slice ks ?max_keys ?max_columns ?decode_timestamps
        key_range ?predicate col_range =
    lwt k, data =
      get_slice ks table ?max_keys ?max_columns ?decode_timestamps
        (inject_range key_range) ?predicate col_range
    in
      return (Option.map C.decode_string k,
              List.map
                (fun kd -> { kd with key = C.decode_string kd.key })
                data)

  let get_row ks ?decode_timestamps key =
    match_lwt get_slice ks ?decode_timestamps (`Discrete [key]) `All with
        _, kd :: _ -> return (M.row_of_key_data kd)
      | _ -> return None

  let get_rows ks ?decode_timestamps key_range =
    lwt k, l = get_slice ks key_range `All in
      return (k, List.filter_map M.row_of_key_data l)

  let get_slice_values ks ?max_keys key_range cols =
    lwt k, l =
      get_slice_values ks table ?max_keys (inject_range key_range) cols
    in
      return (Option.map C.decode_string k,
              List.map (fun (k, cols) -> C.decode_string k, cols) l)

  let get_slice_values_with_timestamps ks ?max_keys key_range cols =
    lwt k, l =
      get_slice_values_with_timestamps ks table ?max_keys
        (inject_range key_range) cols
    in
      return (Option.map C.decode_string k,
              List.map (fun (k, cols) -> C.decode_string k, cols) l)

  let get_columns ks ?max_columns ?decode_timestamps key crange =
    get_columns ks table ?max_columns ?decode_timestamps
      (C.encode_to_string key) crange

  let get_column_values ks key cols =
    get_column_values ks table (C.encode_to_string key) cols

  let get_column ks key col = get_column ks table (C.encode_to_string key) col

  let put_columns ks key cols = put_columns ks table (C.encode_to_string key) cols

  let put_multi_columns ks l =
    put_multi_columns ks table
      (List.map (fun (k, cols) -> (C.encode_to_string k, cols)) l)

  let delete_columns ks key cols =
    delete_columns ks table (C.encode_to_string key) cols

  let delete_key ks key = delete_key ks table (C.encode_to_string key)
end
