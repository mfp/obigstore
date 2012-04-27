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

(** BSON (de)serialization. *)

(** {2 Exceptions and types} *)

(** Possible element types in a document. *)
type allowed_elm =
  [ `Double | `UTF8 | `Document | `Array | `Binary | `ObjectId
  | `Boolean | `DateTime | `Null | `Regexp | `JavaScript | `Symbol
  | `JavaScriptScoped | `Int32 | `Timestamp | `Int64 | `Minkey | `Maxkey ]

type error =
    Invalid_delimiter
  | Invalid_bool of int
  | Unknown_type of int
  | Unknown_binary_type of int
  | Truncated
  | Rejected of allowed_elm

exception Malformed of error

type document = (e_name * element) list

and element =
    Double of float
  | UTF8 of string
  | Document of document
  | Array of element list
  | Binary of binary_type * string
  | ObjectId of object_id
  | Boolean of bool
  | DateTime of millis_since_unix_epoch
  | Null
  | Regexp of cstring * cstring
  | JavaScript of string
  | Symbol of string
  | JavaScriptScoped of string * document
  | Int32 of int
  | Timestamp of Int64.t
  | Int64 of Int64.t
  | Minkey
  | Maxkey

and binary_type = Generic | Function | Old | UUID | MD5 | UserDefined

and cstring = string

and e_name = cstring

and object_id = string

and millis_since_unix_epoch = Int64.t

(** {2 Simple interface} *)

(** [pp_bson_elm ~strict fmt] pretty-prints a BSON element to the [fmt]
  * formatter. If [strict] is true, JSON-compliant syntax is used
  * whenever possible. *)
val pp_bson_elm : strict:bool -> Format.formatter -> element -> unit

(** [pp_bson ~strict fmt] pretty-prints a BSON document to the [fmt]
  * formatter. If [strict] is true, JSON-compliant syntax is used whenever
  * possible. *)
val pp_bson : strict:bool -> Format.formatter -> document -> unit

val string_of_document : document -> string

(** @raise Malformed if the string does not represent a valid BSON message. *)
val document_of_string : string -> document

(** {2 Low-level (de)serialization. } *)
val dump_document : Obs_bytea.t -> document -> unit

(** [read_document s off len] deserializes and returns the BSON document held
  * in [s] starting at [!off] and whose extension is at most [len], and
  * increments [!off] until it points to the first byte after the BSON
  * document.
  *
  * @raise Malformed if the string does not represent a valid BSON message. *)
val read_document : string -> int ref -> int -> document

(** {2 Validation} *)

(** Type representing elements allowed in a document. *)
type allowed_elements

(** Return a description of the allowed element. *)
val string_of_allowed_elm : allowed_elm -> string

(** Value that allows all known element types. *)
val all_allowed : allowed_elements

(** [allow l x] returns a new value such that it allows the elements allowed
  * by [x] or included in [l]. *)
val allow : allowed_elm list -> allowed_elements -> allowed_elements

(** [disallow l x] returns a new value such that it only allows the elements
  * allowed by [x] and not included in [l]. *)
val disallow : allowed_elm list -> allowed_elements -> allowed_elements

(** [validate_document ?allowed s off len] validates the document
  * held in [s], starting at [!off] and of length at most [len]. It will raise
  * a [Malformed] exception indicating the cause if the document does not
  * validate.
  *
  * @param allowed specify the element types allowed ([all_allowed] by
  * default)
  * *)
val validate_document :
  ?allowed:allowed_elements -> string -> int ref -> int -> unit

(** [validate_string ?allowed s] validates the document held in [s], raising
  * and exception if it doesn't validate. Refer to {!validate_document}. *)
val validate_string : ?allowed:allowed_elements -> string -> unit

(** Convenience functions to extract values. All these functions raise
  * [Invalid_arg _] if the type is incorrect. *)
module Browse :
sig
  val element : document -> string -> element
  val opt_element : document -> string -> element option

  val int : element -> int
  val int64 : element -> Int64.t
  val bytea : element -> string
  val boolean : element -> bool
  val timestamp : element -> Int64.t
  val document : element -> document
end

module Build :
sig
  val generic_binary : string -> element

  (** Synonym for {!generic_binary}. *)
  val bytea : string -> element
end
