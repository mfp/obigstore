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

open Printf
open Obigstore_core

type error = Unsatisfied_constraint of string

exception Error of error

type ('a, 'prop) encoder = Bytea.t -> 'a -> unit
type ('a, 'prop) decoder = string -> int -> int -> int ref -> 'a

type ('a, 'prop) codec =
    {
      encode : ('a, 'prop) encoder;
      decode : ('a, 'prop) decoder;
      pp : 'a -> string;
    }

type property = [ `Codec ]
type non_null = [ property | `Non_null ]

let encode c b x = c.encode b x

let encode_to_string c x =
  let b = Bytea.create 13 in
    encode c b x;
    Bytea.contents b

let dummy_ref = ref 0
let decode c s off len = c.decode s off len dummy_ref
let decode_string c s = decode c s 0 (String.length s)

let pp c x = c.pp x

let error e = raise (Error e)

let non_null_string =
  let encode b s =
    try
      ignore (String.index s '\000');
      error (Unsatisfied_constraint "non null")
    with Not_found -> Bytea.add_string b s in
  let decode s off len n =
    n := off;
    (* FIXME: check off and len? *)
    let max = min (String.length s) (off + len) in
    let finished = ref false in
      while !n < max && not !finished do
        if s.[!n] <> '\000' then incr n
        else finished := true
      done;
      let s = String.sub s off (!n - off) in
        if !n < max then incr n;
        s in
  let pp = sprintf "%S" in
    { encode; decode; pp; }

let non_null_string_unsafe =
  { non_null_string with encode = Bytea.add_string; }

let stringz_tuple2 c1 c2 =
  let encode b (x, y) =
    c1.encode b x;
    Bytea.add_byte b 0;
    c2.encode b y in

  let decode s off len n =
    let x = c1.decode s off len n in
    let dn = !n - off in
    let y = c2.decode s !n (len - dn) n in
      (x, y) in

  let pp (x, y) = sprintf "(%s, %s)" (c1.pp x) (c2.pp y)
  in
    { encode; decode; pp; }

let stringz_tuple3 c1 c2 c3 =

  let encode b (x, y, z) =
    c1.encode b x;
    Bytea.add_byte b 0;
    c2.encode b y;
    Bytea.add_byte b 0;
    c3.encode b z in

  let decode s off len n =
    let x = c1.decode s off len n in
    let dn = !n - off in
    let y = c2.decode s !n (len - dn) n in
    let dn = !n - off in
    let z = c3.decode s !n (len - dn) n in
      (x, y, z) in

  let pp (x, y, z) = sprintf "(%s, %s, %s)" (c1.pp x) (c2.pp y) (c3.pp z)
  in
    { encode; decode; pp; }

let stringz_tuple4 c1 c2 c3 c4 =

  let encode b (x, y, z, zz) =
    c1.encode b x;
    Bytea.add_byte b 0;
    c2.encode b y;
    Bytea.add_byte b 0;
    c3.encode b z;
    Bytea.add_byte b 0;
    c4.encode b zz in

  let decode s off len n =
    let x = c1.decode s off len n in
    let dn = !n - off in
    let y = c2.decode s !n (len - dn) n in
    let dn = !n - off in
    let z = c3.decode s !n (len - dn) n in
    let dn = !n - off in
    let zz = c4.decode s !n (len - dn) n in
      (x, y, z, zz) in

  let pp (x, y, z, zz) =
    sprintf "(%s, %s, %s, %s)" (c1.pp x) (c2.pp y) (c3.pp z) (c4.pp zz)

  in { encode; decode; pp; }
