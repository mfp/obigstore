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

(** Utility functions for pretty-printing. *)

(** [escape_string s] returns a new string where non-printable characters have
  * been replaced by a ["\xXX"] escaping sequence. *)
val escape_string : string -> string

val pp_escaped : Format.formatter -> string -> unit

(** Pretty-print a datum. If [strict], write JSON-compliant output by
  * base64-encoding the data. *)
val pp_datum : strict:bool -> Format.formatter -> string -> unit

(** Pretty-print a key.
  * If [strict], write JSON-compliant output by base64-encoding the data. *)
val pp_key : strict:bool -> Format.formatter -> string -> unit

(** [pp_list ?delim pp fmt l] pretty-prints a list using [delim]
  * (default [format_of_string ",@ "]) as the delimiter. *)
val pp_list :
  ?delim:(unit, Format.formatter, unit) format ->
  (Format.formatter -> 'a -> unit) -> Format.formatter -> 'a list -> unit
