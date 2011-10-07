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

(* Order-preserving key encoding. *)

(* This module provides functions that encode/decode values into/from
 * byte sequences while preserving the ordering of the original values, i.e.,
 * given two values [x] and [y] and noting the result of the encoding process
 * [enc x] and [enc y] respectively, then, without loss of generality:
 * * [x = y] implies [enc x = enc y]
 * * [x < y] implies [enc x < enc y]
 *)

open Obigstore_core

type error =
    Unsatisfied_constraint of string
  | Incomplete_fragment of string
  | Bad_encoding of string

exception Error of error * string

type ('a, 'prop) codec

type property = [ `Codec ]
type self_delimited = [ property | `Self_delimited ]

(** [encode codec b x] appends to the buffer [b] a byte sequence representing
  * [x] according to the [codec].
  * @raise Error(Unsatisfied_constraint _, _) if [x] doesn't satisfy a
  * constraint imposed by [codec]. *)
val encode : ('a, _) codec -> Bytea.t -> 'a -> unit

(** Similar to {!encode}, but directly returning a string. *)
val encode_to_string : ('a, _) codec -> 'a -> string

(** [decode codec s off len] returns the value corresponding to the byte
  * sequence in [s] starting at [off] and whose length is at most [len].
  * @raise Invalid_arg if [off], [len] don't represent a valid substring of
  * [s].
  * @raise Error((Incomplete_fragment _ | Bad_encoding _), _) if the byte
  * sequence cannot be decoded correctly. *)

val decode : ('a, _) codec -> string -> int -> int -> 'a

(** Similar to {!decode}. *)
val decode_string : ('a, _) codec -> string -> 'a

val pp : ('a, _) codec -> 'a -> string

val string : (string, property) codec
val self_delimited_string : (string, self_delimited) codec

val stringz : (string, self_delimited) codec
val stringz_unsafe : (string, self_delimited) codec

val positive_int64 : (Int64.t, self_delimited) codec

val bool : (bool, self_delimited) codec

(** Similar to {!positive_int64}, but with inverted order relative to the
  * natural order of [Int64.t] values, i.e.,
  * given [f = encode_to_string positive_int64_complement], if [x < y] then
  * [f x > f y]. *)
val positive_int64_complement : (Int64.t, self_delimited) codec

val tuple2 :
  ('a, self_delimited) codec -> ('b, 'p) codec -> ('a * 'b, 'p) codec

val tuple3 :
  ('a, self_delimited) codec -> ('b, self_delimited) codec ->
  ('c, 'p) codec ->
  ('a * 'b * 'c, 'p) codec

val tuple4 :
  ('a, self_delimited) codec -> ('b, self_delimited) codec ->
  ('c, self_delimited) codec -> ('d, 'p) codec ->
  ('a * 'b * 'c * 'd, 'p) codec

val tuple5 :
  ('a, self_delimited) codec -> ('b, self_delimited) codec ->
  ('c, self_delimited) codec -> ('d, self_delimited) codec ->
  ('e, 'p) codec -> ('a * 'b * 'c * 'd * 'e, 'p) codec

val custom :
  ('a, 'p) codec ->
  encode:('custom -> 'a) -> decode:('a -> 'custom) -> pp:('custom -> string) ->
  ('custom, 'p) codec
