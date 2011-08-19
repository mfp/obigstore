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

type t = string

let create () = String.copy "\000\000\000\000"

external update_unsafe : t -> string -> int -> int -> unit = "obigstore_crc32c_update" "noalloc"
external string : string -> string = "obigstore_crc32c_string"
external fix_endianness : t -> unit = "obigstore_crc32c_ensure_lsb" "noalloc"

let reset t =
  String.unsafe_set t 0 '\000';
  String.unsafe_set t 1 '\000';
  String.unsafe_set t 2 '\000';
  String.unsafe_set t 3 '\000'

let update t s off len =
  if off < 0 || len < 0 || off + len > String.length s then
    invalid_arg "Crc32c.update";
  update_unsafe t s off len

let result t =
  let s = String.copy t in
    fix_endianness s;
    s

let unsafe_result t = fix_endianness t; t

let substring s off len =
  let crc = create () in
    update crc s off len;
    unsafe_result crc

let gb s n = Char.code (String.unsafe_get s n)
let sb s n b = String.unsafe_set s n (Char.unsafe_chr b)

let xor a b =
  for i = 0 to min (String.length a) (String.length b) - 1 do
    sb a i (gb a i lxor gb b i)
  done
