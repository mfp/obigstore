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

(** Finite map over strings using ternary search trees (TSTs). *)

type 'a t
val empty : 'a t
val length : 'a t -> int
val is_empty : 'a t -> bool
val find : string -> 'a t -> 'a

(** [find_prefixes k t] returns all the values whose keys are a prefix of [k]
  * (including [k] itself), in longest to shortest order (i.e., the value for
  * [k] would come first). *)
val find_prefixes : string -> 'a t -> 'a list
val mem : string -> 'a t -> bool
val add : string -> 'a -> 'a t -> 'a t
val remove : string -> 'a t -> 'a t

val iter : (string -> 'a -> unit) -> 'a t -> unit

val fold : (string -> 'a -> 'b -> 'b) -> 'a t -> 'b -> 'b
