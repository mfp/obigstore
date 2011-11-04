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

(** Computation of the CRC32C cyclic redundancy check using the Castagnoli
  * polynomial 0x1EDC6F41. *)

type t

(** Create and initialize. *)
val create : unit -> t

(** Reset so that the CRC32C of another sequence can be computed. *)
val reset : t -> unit

(** Compute the CRC32C of a string: the result is returned as a 4-byte string
  * corresponding to an unsigned 32-bit integer in lsb order. *)
val string : string -> string

(** [substring s off len] computes the CRC32C of the substring of [s] starting
  * at [off] of length [len]. The result is returned as a 4-byte
  * string corresponding to an unsigned 32-bit integer in lsb order. *)
val substring : string -> int -> int -> string

val update : t -> string -> int -> int -> unit
val update_unsafe : t -> string -> int -> int -> unit

(** [result t] returns a 4-byte string corresponding to an unsigned 32-bit
  * integer in lsb order. *)
val result : t -> string

(** [unsafe_result t] returns a 4-byte string corresponding to an unsigned
  * 32-bit integer in lsb order.  [t] cannot be used for any further
  * calculations (neither in the [update] nor the [result] families) until it
  * is {!reset}. *)
val unsafe_result : t -> string

(** [xor a b] updates [a] so that it reflects the result of XORing [a] and
  * [b]. *)
val xor : string -> string -> unit
