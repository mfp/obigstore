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

module List = struct include List include BatList end
module String = struct include String include BatString end

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

let malformed error = raise (Malformed error)

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

and cstring = string   (* nul-terminated *)
and e_name = cstring
and object_id = string (* 12 bytes *)
and millis_since_unix_epoch = Int64.t
and binary_type = Generic | Function | Old | UUID | MD5 | UserDefined

let dump_cstring b s =
  Obs_bytea.add_string b s;
  Obs_bytea.add_byte b 0

let dump_string b s =
  Obs_bytea.add_int32_le b (String.length s);
  Obs_bytea.add_string b s;
  Obs_bytea.add_byte b 0

let dump_ty_name b c s =
  Obs_bytea.add_byte b c;
  dump_cstring b s

let byte_of_binary_type = function
      Generic     -> 0x00
    | Function    -> 0x01
    | Old         -> 0x02
    | UUID        -> 0x03
    | MD5         -> 0x05
    | UserDefined -> 0x80

let binary_type_of_byte = function
    0x00 -> Generic
  | 0x01 -> Function
  | 0x02 -> Old
  | 0x03 -> UUID
  | 0x05 -> MD5
  | 0x80 -> UserDefined
  | n -> malformed (Unknown_binary_type n)

let dump_binary b ty s =
  Obs_bytea.add_int32_le b (String.length s);
  Obs_bytea.add_byte b (byte_of_binary_type ty);
  Obs_bytea.add_string b s

let rec dump_document b doc =
  let off = Obs_bytea.length b in
    Obs_bytea.add_int32_le b 0;
    List.iter (dump_element b) doc;
    Obs_bytea.add_byte b 0;
    let len = Obs_bytea.length b - off in
      Obs_bytea.blit_int32_le_at b ~off len

and dump_element b (name, elm) = match elm with
    Double x -> dump_ty_name b 0x01 name; Obs_bytea.add_ieee754_float b x
  | UTF8 s -> dump_ty_name b 0x02 name; dump_string b s
  | Document doc -> dump_ty_name b 0x03 name; dump_document b doc
  | Array l ->
      let doc = List.mapi (fun i elm -> (string_of_int i, elm)) l in
        dump_ty_name b 0x04 name;
        dump_document b doc
  | Binary (ty, s) -> dump_ty_name b 0x05 name; dump_binary b ty s
  | ObjectId s -> dump_ty_name b 0x07 name; Obs_bytea.add_string b s
  | Boolean false -> dump_ty_name b 0x08 name; Obs_bytea.add_byte b 0
  | Boolean true -> dump_ty_name b 0x08 name; Obs_bytea.add_byte b 1
  | DateTime n -> dump_ty_name b 0x09 name; Obs_bytea.add_int64_le b n
  | Null -> dump_ty_name b 0x0A name
  | Regexp (pattern, options) -> dump_ty_name b 0x0B name;
                                 dump_cstring b pattern;
                                 dump_cstring b options
  | JavaScript s -> dump_ty_name b 0x0D name; dump_string b s
  | Symbol s -> dump_ty_name b 0x0E name; dump_string b s
  | JavaScriptScoped (s, doc) ->
      dump_ty_name b 0x0F name;
      let off = Obs_bytea.length b in
        Obs_bytea.add_int32_le b 0;
        dump_string b s;
        dump_document b doc;
        let len = Obs_bytea.length b in
          Obs_bytea.blit_int32_le_at b ~off (len - off)
  | Int32 n -> dump_ty_name b 0x10 name; Obs_bytea.add_int32_le b n
  | Timestamp n -> dump_ty_name b 0x11 name; Obs_bytea.add_int64_le b n
  | Int64 n -> dump_ty_name b 0x12 name; Obs_bytea.add_int64_le b n
  | Minkey -> dump_ty_name b 0xFF name
  | Maxkey -> dump_ty_name b 0x7F name

let read_cstring s off max =
  try
    let upto = String.index_from s !off '\x00' in
      if upto >= max then malformed Truncated;
      let start = !off in
        off := upto + 1;
        String.sub s start (upto - start)
  with Not_found -> malformed Truncated

let skip_cstring s off max =
  try
    let upto = String.index_from s !off '\x00' in
      if upto >= max then malformed Truncated;
      off := upto + 1
  with Not_found -> malformed Truncated

let byte_at s off = Char.code (String.unsafe_get s off)

let read_int32 s off max =
  if max < !off + 4 then malformed Truncated;
  let a = byte_at s !off
  and b = byte_at s (!off + 1)
  and c = byte_at s (!off + 2)
  and d = byte_at s (!off + 3) in
    off := !off + 4;
    a lor b lsl 8 lor c lsl 16 lor d lsl 24

let read_string s off max =
  let len = read_int32 s off max in
  let start = !off in
    if max < start + len then malformed Truncated;
    if s.[start + len] <> '\x00' then malformed Invalid_delimiter;
    off := start + len + 1;
    String.sub s start len

let skip_string s off max =
  let len = read_int32 s off max in
  let start = !off in
    if max < start + len then malformed Truncated;
    off := start + len + 1

let read_ty s off max =
  if !off >= max then malformed Truncated;
  let r = Char.code (String.unsafe_get s !off) in
    incr off;
    r

let read_e_name s off max = read_cstring s off max
let skip_e_name s off max = skip_cstring s off max

let skip_n off n max =
  if max < !off + n then malformed Truncated;
  off := !off + n

let read_int64 s off max =
  let open Int64 in
  if max < !off + 8 then malformed Truncated;
  let n = !off in
  let a = byte_at s n and
      b = byte_at s (n + 1) and
      c = byte_at s (n + 2) and
      d = byte_at s (n + 3) and
      e = byte_at s (n + 4) and
      f = byte_at s (n + 5) and
      g = byte_at s (n + 6) and
      h = byte_at s (n + 7)
  in
    off := !off + 8;
    logor
      (of_int (a lor b lsl 8 lor c lsl 16))
      (logor
         (shift_left (of_int (d lor e lsl 8 lor f lsl 16)) 24)
         (shift_left (of_int (g lor h lsl 8)) 48))

let read_double s off max = Int64.float_of_bits (read_int64 s off max)

let rec read_element s off max = match read_ty s off max with
    0x01 -> let name = read_e_name s off max in
            let d = read_double s off max in
              (name, Double d)
  | 0x02 -> let name = read_e_name s off max in
            let s = read_string s off max in
              (name, UTF8 s)
  | 0x03 -> let name = read_e_name s off max in
            let doc = read_document s off max in
              (name, Document doc)
  | 0x04 -> let name = read_e_name s off max in
            let doc = read_document s off max in
              (name, Array (List.map snd doc))
  | 0x05 ->
      let name = read_e_name s off max in
      let len = read_int32 s off max in
        if max < !off + 1 + len then malformed Truncated;
        let subtype = Char.code s.[!off] in
        let s = String.sub s (!off + 1) len in
          off := !off + 1 + len;
          (name, Binary (binary_type_of_byte subtype, s))
  | 0x07 ->
      let name = read_e_name s off max in
        if max < !off + 12 then malformed Truncated;
        let s = String.sub s !off 12 in
          off := !off + 12;
          (name, ObjectId s)
  | 0x08 -> let name = read_e_name s off max in
            if max < !off + 1 then malformed Truncated;
            let v = match Char.code s.[!off] with
                0 -> false
              | 1 -> true
              | n -> malformed (Invalid_bool n)
            in
              incr off;
              (name, Boolean v)
  | 0x09 ->
      let name = read_e_name s off max in
      let v = read_int64 s off max in
        (name, DateTime v)
  | 0x0A -> let name = read_e_name s off max in
              (name, Null)
  | 0x0B ->
      let name = read_e_name s off max in
      let pattern = read_cstring s off max in
      let options = read_cstring s off max in
        (name, Regexp (pattern, options))
  | 0x0D -> let name = read_e_name s off max in
            let code = read_string s off max in
              (name, JavaScript code)
  | 0x0E -> let name = read_e_name s off max in
            let s = read_string s off max in
              (name, Symbol s)
  | 0x0F -> let name = read_e_name s off max in
            let len = read_int32 s off max in
            let max' = !off + len - 4 in
              if max < max' then malformed Truncated;
              let code = read_string s off max' in
              let doc = read_document s off max' in
                (name, JavaScriptScoped (code, doc))
  | 0x10 -> let name = read_e_name s off max in
            let n = read_int32 s off max in
              (name, Int32 n)
  | 0x11 -> let name = read_e_name s off max in
            let n = read_int64 s off max in
              (name, Timestamp n)
  | 0x12 -> let name = read_e_name s off max in
            let n = read_int64 s off max in
              (name, Int64 n)
  | 0xFF -> let name = read_e_name s off max in
              (name, Minkey)
  | 0x7F -> let name = read_e_name s off max in
              (name, Maxkey)
  | n -> malformed (Unknown_type n)

and read_document s off max =
  let len = read_int32 s off max in
    if max < !off + len - 4 then malformed Truncated;
    let l = read_elms [] s off (!off + len - 4 - 1) in
      if max < !off + 1 || s.[!off] <> '\x00' then malformed Invalid_delimiter;
      incr off;
      List.rev l

and read_elms acc s off max =
  if !off >= max then acc
  else
    let elm = read_element s off max in
      read_elms (elm :: acc) s off max

let string_of_document d =
  let b = Obs_bytea.create 13 in
     dump_document b d;
     Obs_bytea.contents b

let document_of_string s =
  read_document s (ref 0) (String.length s)

let read_document s off len = read_document s off (!off + len)

let allowed_elm_of_byte = function
    0x01 -> `Double
  | 0x02 -> `UTF8
  | 0x03 -> `Document
  | 0x04 -> `Array
  | 0x05 -> `Binary
  | 0x07 -> `ObjectId
  | 0x08 -> `Boolean
  | 0x09 -> `DateTime
  | 0x0A -> `Null
  | 0x0B -> `Regexp
  | 0x0D -> `JavaScript
  | 0x0E -> `Symbol
  | 0x0F -> `JavaScriptScoped
  | 0x10 -> `Int32
  | 0x11 -> `Timestamp
  | 0x12 -> `Int64
  | 0xFF -> `Minkey
  | 0x7F -> `Maxkey
  | n -> malformed (Unknown_type n)

let byte_of_allowed_elm =  function
  | `Double -> 0x01
  | `UTF8 -> 0x02
  | `Document -> 0x03
  | `Array -> 0x04
  | `Binary -> 0x05
  | `ObjectId -> 0x07
  | `Boolean -> 0x08
  | `DateTime -> 0x09
  | `Null -> 0x0A
  | `Regexp -> 0x0B
  | `JavaScript -> 0x0D
  | `Symbol -> 0x0E
  | `JavaScriptScoped -> 0x0F
  | `Int32 -> 0x10
  | `Timestamp -> 0x11
  | `Int64 -> 0x12
  | `Minkey -> 0xFF
  | `Maxkey -> 0x7F

let string_of_allowed_elm = function
  | `Double -> "Double"
  | `UTF8 -> "UTF8"
  | `Document -> "Document"
  | `Array -> "Array"
  | `Binary -> "Binary"
  | `ObjectId -> "ObjectId"
  | `Boolean -> "Boolean"
  | `DateTime -> "DateTime"
  | `Null -> "Null"
  | `Regexp -> "Regexp"
  | `JavaScript -> "JavaScript"
  | `Symbol -> "Symbol"
  | `JavaScriptScoped -> "JavaScriptScoped"
  | `Int32 -> "Int32"
  | `Timestamp -> "Timestamp"
  | `Int64 -> "Int64"
  | `Minkey -> "Minkey"
  | `Maxkey  -> "Maxkey"

let read_and_check_ty allowed s off max =
  let ty = read_ty s off max in
    if not (Array.unsafe_get allowed ty) then
      malformed (Rejected (allowed_elm_of_byte ty));
    ty

let rec skip_element allowed s off max = match read_and_check_ty allowed s off max with
    0x01 | 0x09 | 0x11 | 0x12 -> skip_e_name s off max; skip_n off 8 max
  | 0x02 | 0x0D | 0x0E -> skip_e_name s off max; skip_string s off max
  | 0x03 | 0x04 -> skip_e_name s off max; skip_document allowed s off max
  | 0x05 -> skip_e_name s off max;
            let len = read_int32 s off max in
              skip_n off (len + 1) max
  | 0x07 -> skip_e_name s off max; skip_n off 12 max
  | 0x08 -> skip_e_name s off max; skip_n off 1 max
  | 0x0A | 0xFF | 0x7F -> skip_e_name s off max
  | 0x0B -> skip_e_name s off max; skip_cstring s off max; skip_cstring s off max
  | 0x0F -> skip_e_name s off max;
            let len = read_int32 s off max in
            let max' = !off + len - 4 in
              if max < max' then malformed Truncated;
              skip_string s off max';
              skip_document allowed s off max';
  | 0x10 -> skip_e_name s off max; skip_n off 4 max
  | n -> malformed (Unknown_type n)

and skip_document allowed s off max =
  let len = read_int32 s off max in
    if max < !off + len - 4 then malformed Truncated;
      skip_elms allowed s off (!off + len - 4 - 1);
      if max < !off + 1 || s.[!off] <> '\x00' then malformed Invalid_delimiter;
      incr off

and skip_elms allowed s off max =
  if !off < max then begin
    skip_element allowed s off max;
    skip_elms allowed s off max
  end

type allowed_elements = bool array

let all_allowed  = Array.make 256 true

let disallow l a =
  let a = Array.copy a in
    List.iter (fun ty -> a.(byte_of_allowed_elm ty) <- false) l;
    a

let allow l a =
  let a = Array.copy a in
    List.iter (fun ty -> a.(byte_of_allowed_elm ty) <- true) l;
    a

let validate_document ?(allowed = all_allowed) s off len =
  skip_document allowed s off (!off + len)

let validate_string ?allowed s =
  validate_document ?allowed s (ref 0) (String.length s)

module Browse =
struct
  let invalid_arg x = invalid_arg ("Obs_bson.Browse." ^ x)

  let element doc elm = List.assoc elm doc

  let opt_element t x = try Some (element t x) with Not_found -> None

  let int = function
      Int32 n -> n
    | _ -> invalid_arg "int"

  let int64 = function
      Int64 x -> x
    | _ -> invalid_arg "int64"

  let bytea = function
      Binary (_, s) -> s
    | _ -> invalid_arg "bytea"

  let boolean = function
      Boolean x -> x
    | _ -> invalid_arg "boolean"

  let timestamp = function
      DateTime x | Timestamp x -> x
    | _ -> invalid_arg "timestamp"

  let document = function
      Document x -> x
    | _ -> invalid_arg "document"
end

module Build =
struct
  let generic_binary x = Binary (Generic, x)
  let bytea = generic_binary
end
