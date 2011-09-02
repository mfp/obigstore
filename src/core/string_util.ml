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

let chr n =
  let s = String.create 1 in
    String.unsafe_set s 0 (Char.unsafe_chr n);
    s

let rec strneq s1 o1 s2 o2 = function
    0 -> true
  | len ->
      if String.unsafe_get s1 o1 = String.unsafe_get s2 o2 then
        strneq s1 (o1 + 1) s2 (o2 + 1) (len - 1)
      else
        false

let cmp_substrings s1 o1 l1 s2 o2 l2 =
  if o1 < 0 || l1 < 0 || o1 + l1 > String.length s1 ||
     o2 < 0 || l2 < 0 || o2 + l2 > String.length s2 then
    invalid_arg "String_util.cmp_substrings";
  let rec loop_cmp s1 p1 l1 s2 p2 l2 = function
      0 -> l1 - l2
    | n ->
        let c1 = Char.code (String.unsafe_get s1 p1) in
        let c2 = Char.code (String.unsafe_get s2 p2) in
          match c1 - c2 with
              0 -> loop_cmp s1 (p1 + 1) l1 s2 (p2 + 1) l2 (n - 1)
            | n -> n
  in loop_cmp s1 o1 l1 s2 o2 l2 (if l1 < l2 then l1 else l2)

let compare s1 s2 =
  cmp_substrings s1 0 (String.length s1) s2 0 (String.length s2)
