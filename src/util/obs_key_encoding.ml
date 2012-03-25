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

external decode_int64_be :
  string -> int -> Int64.t = "obigstore_decode_int64_be"

external decode_int64_complement_be :
  string -> int -> Int64.t = "obigstore_decode_int64_complement_be"

type error =
    Unsatisfied_constraint of string
  | Incomplete_fragment of string
  | Bad_encoding of string
  | Unknown_tag of int

exception Error of error * string

let string_of_error = function
    Unsatisfied_constraint s -> sprintf "Unsatisfied_constraint %S" s
  | Incomplete_fragment s -> sprintf "Incomplete_fragment %S" s
  | Bad_encoding s -> sprintf "Bad_encoding %S" s
  | Unknown_tag n -> sprintf "Unknown_tag %d" n

let () =
  Printexc.register_printer
    (function
       | Error (x, where) -> Some (sprintf "Error (%s, %S)"
                                     (string_of_error x) where)
       | _ -> None)

type ('a, 'prop) encoder = Obs_bytea.t -> 'a -> unit

(* [decode s off len scratch new_offset] *)
type ('a, 'prop) decoder = string -> int -> int -> Obs_bytea.t -> int ref -> 'a

type ('a, 'prop) codec =
    {
      encode : ('a, 'prop) encoder;
      decode : ('a, 'prop) decoder;
      pp : 'a -> string;
    }

type property = [ `Codec ]
type self_delimited = [ property | `Self_delimited ]

let encode c b x = c.encode b x

let encode_to_string c x =
  let b = Obs_bytea.create 13 in
    encode c b x;
    Obs_bytea.contents b

let dummy_ref = ref 0
let dummy_scratch = Obs_bytea.create 13
let decode c s off len = c.decode s off len dummy_scratch dummy_ref
let decode_string c s = decode c s 0 (String.length s)

let pp c x = c.pp x

let error where e = raise (Error (e, "Obs_key_encoding." ^ where))

let invalid_off_len ~fname s off len =
  raise
    (Invalid_argument
       (sprintf "Obs_key_encoding.%s decoding: \
                 invalid offset (%d) or length (%d) in string of length %d"
          fname off len (String.length s)))

let check_off_len fname s off len =
  if off < 0 || len < 0 || off + len > String.length s then
    invalid_off_len ~fname s off len

let string =
  let encode = Obs_bytea.add_string in
  let decode s off len scratch n =
    check_off_len "string" s off len;
    n := off + len;
    String.sub s off len in
  let pp = sprintf "%S" in
    { encode; decode; pp; }

type delim_string_dec_state = Normal | Got_zero

let self_delimited_string =
  let encode b s =
    for i = 0 to String.length s - 1 do
      match String.unsafe_get s i with
          c when c <> '\000' -> Obs_bytea.add_char b c
        | _ (* \000 *) -> Obs_bytea.add_string b "\000\001"
    done;
    Obs_bytea.add_string b "\000\000" in

  let decode s off len scratch n =
    check_off_len "self_delimited_string" s off len;
    let max = off + len in
    let finished = ref false in
    let state = ref Normal in
    let b = scratch in
      Obs_bytea.clear b;
      n := off;
      while !n < max && not !finished do
        begin match !state with
            Normal -> begin match String.unsafe_get s !n with
                c when c <> '\000' -> Obs_bytea.add_char b c
              | _ (* \000 *) -> state := Got_zero
            end
          | Got_zero -> match String.unsafe_get s !n with
                '\000' -> finished := true; state := Normal
              | '\001' -> Obs_bytea.add_char b '\000'; state := Normal;
              | _ -> error "self_delimited_string.decode"
                       (Bad_encoding "self_delimited_string")
        end;
        incr n
      done;
      if !state <> Normal then
        error "self_delimited_string.decode"
          (Bad_encoding "self_delimited_string");
      Obs_bytea.contents b in

  let pp = sprintf "%S"
  in
    { encode; decode; pp; }

let stringz =

  let encode b s =
    try
      ignore (String.index s '\000');
      error "stringz.encode" (Unsatisfied_constraint "non null")
    with Not_found ->
      Obs_bytea.add_string b s;
      Obs_bytea.add_char b '\000' in

  let decode s off len scratch n =
    check_off_len "stringz" s off len;
    n := off;
    let max = off + len in
    let finished = ref false in
      while !n <= max && not !finished do
        if s.[!n] <> '\000' then incr n
        else finished := true
      done;
      if not !finished then error "stringz.decode" (Incomplete_fragment "stringz");
      let s = String.sub s off (!n - off) in
        incr n;
        s in

  let pp = sprintf "%S"
  in
    { encode; decode; pp; }

let byte =
  let encode b x = Obs_bytea.add_byte b x in
  let decode s off len scratch n =
    check_off_len "bool" s off len;
    if len < 1 then invalid_off_len ~fname:"byte" s off len;
    n := off + 1;
    Char.code s.[off]
  in
    { encode; decode; pp = string_of_int; }

let bool =
  let encode b x = Obs_bytea.add_byte b (if x then 1 else 0) in
  let decode s off len scratch n =
    check_off_len "bool" s off len;
    if len < 1 then invalid_off_len ~fname:"bool" s off len;
    n := off + 1;
    not (s.[off] = '\000')
  in
    {encode; decode; pp = string_of_bool; }

let stringz_unsafe =
  let encode b s = Obs_bytea.add_string b s; Obs_bytea.add_byte b 0 in
    { stringz with encode; }

let positive_int64 =
  let encode b n =
    if Int64.compare n 0L < 0 then
      error "positive_int64.encode" (Unsatisfied_constraint "negative Int64.t");
    Obs_bytea.add_int64_be b n in

  let decode s off len scratch n =
    check_off_len "int64" s off len;
    if len < 8 then invalid_off_len ~fname:"int64" s off len;
    n := off + 8;
    decode_int64_be s off
  in
    { encode; decode; pp = Int64.to_string; }

let positive_int64_complement =
  let encode b n =
    if Int64.compare n 0L < 0 then
      error "positive_int64_complement.encode"
        (Unsatisfied_constraint "negative Int64.t");
    Obs_bytea.add_int64_complement_be b n in

  let decode s off len scratch n =
    check_off_len "int64" s off len;
    if len < 8 then invalid_off_len ~fname:"int64_complement" s off len;
    n := off + 8;
    decode_int64_complement_be s off
  in
    { encode; decode; pp = Int64.to_string; }

let tuple2 c1 c2 =
  let encode b (x, y) =
    c1.encode b x;
    c2.encode b y in

  let decode s off len scratch n =
    let x = c1.decode s off len scratch n in
    let dn = !n - off in
    let y = c2.decode s !n (len - dn) scratch n in
      (x, y) in

  let pp (x, y) = sprintf "(%s, %s)" (c1.pp x) (c2.pp y)
  in
    { encode; decode; pp; }

let tuple3 c1 c2 c3 =

  let encode b (x, y, z) =
    c1.encode b x;
    c2.encode b y;
    c3.encode b z in

  let decode s off len scratch n =
    let x = c1.decode s off len scratch n in
    let len = len - (!n - off) and old_n = !n in
    let y = c2.decode s !n len scratch n in
    let len = len - (!n - old_n) in
    let z = c3.decode s !n len scratch n in
      (x, y, z) in

  let pp (x, y, z) = sprintf "(%s, %s, %s)" (c1.pp x) (c2.pp y) (c3.pp z)
  in
    { encode; decode; pp; }

let tuple4 c1 c2 c3 c4 =

  let encode b (x, y, z, zz) =
    c1.encode b x;
    c2.encode b y;
    c3.encode b z;
    c4.encode b zz in

  let decode s off len scratch n =
    let x = c1.decode s off len scratch n in
    let len = len - (!n - off) and old_n = !n in
    let y = c2.decode s !n len scratch n in
    let len = len - (!n - old_n) and old_n = !n in
    let z = c3.decode s !n len scratch n in
    let len = len - (!n - old_n) in
    let zz = c4.decode s !n len scratch n in
      (x, y, z, zz) in

  let pp (x, y, z, zz) =
    sprintf "(%s, %s, %s, %s)" (c1.pp x) (c2.pp y) (c3.pp z) (c4.pp zz)

  in { encode; decode; pp; }

let tuple5 c1 c2 c3 c4 c5 =

  let encode b (x, y, z, zx, zy) =
    c1.encode b x;
    c2.encode b y;
    c3.encode b z;
    c4.encode b zx;
    c5.encode b zy;
  in

  let decode s off len scratch n =
    let x = c1.decode s off len scratch n in
    let len = len - (!n - off) and old_n = !n in
    let y = c2.decode s !n len scratch n in
    let len = len - (!n - old_n) and old_n = !n in
    let z = c3.decode s !n len scratch n in
    let len = len - (!n - old_n) and old_n = !n in
    let zx = c4.decode s !n len scratch n in
    let len = len - (!n - old_n) in
    let zy = c5.decode s !n len scratch n in
      (x, y, z, zx, zy) in

  let pp (x, y, z, zx, zy) =
    sprintf "(%s, %s, %s, %s, %s)" (c1.pp x) (c2.pp y) (c3.pp z) (c4.pp zx) (c5.pp zy)

  in { encode; decode; pp; }

let decode_choice choices s off len scratch n =
  let tag = byte.decode s off len scratch n in
  let len = len - (!n - off) in
    if tag < 0 || tag >= Array.length choices then
      error (sprintf "choice%d.decode" (Array.length choices)) (Unknown_tag tag);
    choices.(tag) s !n len scratch n

let choice2 lbl1 c1 lbl2 c2 =
  let encode b = function
      `A x -> byte.encode b 0; c1.encode b x
    | `B x -> byte.encode b 1; c2.encode b x in

  let decode =
    decode_choice
      [|
        (fun s off len scratch n -> let x = c1.decode s off len scratch n in `A x);
        (fun s off len scratch n -> let x = c2.decode s off len scratch n in `B x);
      |] in

  let pp = function
      `A x -> sprintf "%s:%s" lbl1 (c1.pp x)
    | `B x -> sprintf "%s:%s" lbl2 (c2.pp x)

  in { encode; decode; pp; }

let choice3 lbl1 c1 lbl2 c2 lbl3 c3 =
  let encode b = function
      `A x -> byte.encode b 0; c1.encode b x
    | `B x -> byte.encode b 1; c2.encode b x
    | `C x -> byte.encode b 2; c3.encode b x in

  let decode =
    decode_choice
      [|
        (fun s off len scratch n -> let x = c1.decode s off len scratch n in `A x);
        (fun s off len scratch n -> let x = c2.decode s off len scratch n in `B x);
        (fun s off len scratch n -> let x = c3.decode s off len scratch n in `C x);
      |] in

  let pp = function
      `A x -> sprintf "%s:%s" lbl1 (c1.pp x)
    | `B x -> sprintf "%s:%s" lbl2 (c2.pp x)
    | `C x -> sprintf "%s:%s" lbl3 (c3.pp x)

  in { encode; decode; pp; }

let choice4 lbl1 c1 lbl2 c2 lbl3 c3 lbl4 c4 =
  let encode b = function
      `A x -> byte.encode b 0; c1.encode b x
    | `B x -> byte.encode b 1; c2.encode b x
    | `C x -> byte.encode b 2; c3.encode b x
    | `D x -> byte.encode b 3; c4.encode b x in

  let decode =
    decode_choice
      [|
        (fun s off len scratch n -> let x = c1.decode s off len scratch n in `A x);
        (fun s off len scratch n -> let x = c2.decode s off len scratch n in `B x);
        (fun s off len scratch n -> let x = c3.decode s off len scratch n in `C x);
        (fun s off len scratch n -> let x = c4.decode s off len scratch n in `D x);
      |] in

  let pp = function
      `A x -> sprintf "%s:%s" lbl1 (c1.pp x)
    | `B x -> sprintf "%s:%s" lbl2 (c2.pp x)
    | `C x -> sprintf "%s:%s" lbl3 (c3.pp x)
    | `D x -> sprintf "%s:%s" lbl4 (c4.pp x)

  in { encode; decode; pp; }

let choice5 lbl1 c1 lbl2 c2 lbl3 c3 lbl4 c4 lbl5 c5 =
  let encode b = function
      `A x -> byte.encode b 0; c1.encode b x
    | `B x -> byte.encode b 1; c2.encode b x
    | `C x -> byte.encode b 2; c3.encode b x
    | `D x -> byte.encode b 3; c4.encode b x
    | `E x -> byte.encode b 4; c5.encode b x in

  let decode =
    decode_choice
      [|
        (fun s off len scratch n -> let x = c1.decode s off len scratch n in `A x);
        (fun s off len scratch n -> let x = c2.decode s off len scratch n in `B x);
        (fun s off len scratch n -> let x = c3.decode s off len scratch n in `C x);
        (fun s off len scratch n -> let x = c4.decode s off len scratch n in `D x);
        (fun s off len scratch n -> let x = c5.decode s off len scratch n in `E x);
      |] in

  let pp = function
      `A x -> sprintf "%s:%s" lbl1 (c1.pp x)
    | `B x -> sprintf "%s:%s" lbl2 (c2.pp x)
    | `C x -> sprintf "%s:%s" lbl3 (c3.pp x)
    | `D x -> sprintf "%s:%s" lbl4 (c4.pp x)
    | `E x -> sprintf "%s:%s" lbl5 (c5.pp x)

  in { encode; decode; pp; }

let custom c ~encode ~decode ~pp =
  let encode b x = c.encode b (encode x) in
  let decode s off len scratch n = decode (c.decode s off len scratch n) in
    { encode; decode; pp }
