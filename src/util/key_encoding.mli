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

open Obigstore_core

type error = Unsatisfied_constraint of string

exception Error of error

type ('a, 'prop) codec

type property = [ `Codec ]
type non_null = [ property | `Non_null ]

val encode : ('a, _) codec -> Bytea.t -> 'a -> unit
val encode_to_string : ('a, _) codec -> 'a -> string
val decode : ('a, _) codec -> string -> int -> int -> 'a
val decode_string : ('a, _) codec -> string -> 'a
val pp : ('a, _) codec -> 'a -> string

val non_null_string : (string, non_null) codec
val non_null_string_unsafe : (string, non_null) codec

val stringz_tuple2 :
  ('a, non_null) codec -> ('b, non_null) codec -> ('a * 'b, property) codec

val stringz_tuple3 :
  ('a, non_null) codec -> ('b, non_null) codec ->
  ('c, non_null) codec ->
  ('a * 'b * 'c, property) codec

val stringz_tuple4 :
  ('a, non_null) codec -> ('b, non_null) codec ->
  ('c, non_null) codec -> ('d, non_null) codec ->
  ('a * 'b * 'c * 'd, property) codec
