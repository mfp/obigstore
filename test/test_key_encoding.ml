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
open Test_00util
open OUnit
open Obigstore_core
open Obigstore_util

module K = Key_encoding

let check_roundtrip codec x =
  let b = Bytea.create 13 in
  let printer x =
    sprintf "%s (encoded as %S)" (K.pp codec x) (K.encode_to_string codec x)
  in
    K.encode codec b x;
    aeq printer x (K.decode_string codec (Bytea.contents b))

let comparison = function
    n when n < 0 -> `LT
  | n when n > 0 -> `GT
  | _ -> `EQ

let check_order_preservation ?(cmp = Pervasives.compare) codec l =
  List.iter
    (fun x ->
       List.iter
         (fun y ->
            let x' = K.encode_to_string codec x in
            let y' = K.encode_to_string codec y in
              match comparison (cmp x y) with
                  `LT when x' >= y' ->
                      assert_failure_fmt "%s < %s but encoded forms %S >= %S"
                        (K.pp codec x) (K.pp codec y) x' y'
                | `EQ when x' <> y' ->
                      assert_failure_fmt "%s = %s but encoded forms %S <> %S"
                        (K.pp codec x) (K.pp codec y) x' y'
                | `GT when x' <= y' ->
                      assert_failure_fmt "%s > %s but encoded forms %S <= %S"
                        (K.pp codec x) (K.pp codec y) x' y'
                | _ -> ())
         l)
    l

let test_stringz () =
  let v =
    [""; "a"; "ab"; "abcd"; "\001ab"; String.make 200 'x' ]
  in
    List.iter (check_roundtrip K.stringz) v;
    List.iter (check_roundtrip K.stringz_unsafe) v;
    check_order_preservation K.stringz v;
    check_order_preservation K.stringz_unsafe v;
    try
      check_roundtrip K.stringz "a\000";
      assert_failure "Expected Unsatisfied_constraint error"
    with K.Error (K.Unsatisfied_constraint _, "Key_encoding.stringz.encode") -> ()

let test_self_delimited_string () =
  let codec = K.self_delimited_string in
  let l =
    [
      ""; "\000"; "a"; "\000\001"; "\001"; "\000\000";
      "\000\001\000\001\000\000"; "\001\000\001\001\000";
    ]
  in
    List.iter (check_roundtrip codec) l;
    check_order_preservation codec l

let positive_int64_vector =
    [ 0L; 1L; Int64.max_int; 42L; ]

let test_positive_int64 () =
  List.iter (check_roundtrip K.positive_int64) positive_int64_vector;
  check_order_preservation K.positive_int64 positive_int64_vector

let test_positive_int64_complement () =
  List.iter (check_roundtrip K.positive_int64_complement) positive_int64_vector;
  check_order_preservation
    ~cmp:(fun x y -> compare y x) K.positive_int64_complement positive_int64_vector

let test_tuple2 () =
  let codec = K.tuple2 K.stringz K.stringz in
  let l =
      [ "", ""; "a", ""; "", "a"; "abc", "def"; "asdsdsa", "sdfdsfssdf" ]
  in
    List.iter (check_roundtrip codec) l;
    check_order_preservation codec l

let test_tuple3 () =
  let codec = K.tuple3 K.stringz K.stringz K.stringz in
  let l =
    [ "", "", ""; "a", "", ""; "", "a", ""; "abc", "def", "";
      "asdsdsa", "", "sdfdsfssdf" ]
  in
    List.iter (check_roundtrip codec) l;
    check_order_preservation codec l

let test_tuple4 () =
  let codec = K.tuple4 K.stringz K.stringz K.stringz K.stringz in
  let l =
    [ "", "", "", ""; "a", "", "", ""; "", "a", "", ""; "abc", "def", "", "";
      "asdsdsa", "", "x", "sdfdsfssdf" ]
  in
    List.iter (check_roundtrip codec) l;
    check_order_preservation codec l

let test_custom () =
  let encode s = Scanf.sscanf s "%Ld-%Ld-%s" (fun a b c -> (a, b, c)) in
  let decode (a, b, c) = Printf.sprintf "%Ld-%Ld-%s" a b c in
  let pp = sprintf "%S" in
  let codec = K.custom
                (K.tuple3 K.positive_int64 K.positive_int64_complement K.stringz)
                ~encode ~decode ~pp in
  let l =
    [
      "0-0-x"; "0-1-"; "32-43-"; "1-1-"; "1-1-bar"; "42-42-foobar";
    ]
  in
    List.iter (check_roundtrip codec) l;
    check_order_preservation
      ~cmp:(fun x y ->
              let (a1, b1, c1) = encode x in
              let (a2, b2, c2) = encode y in
                compare (a1, Int64.neg b1, c1) (a2, Int64.neg b2, c2))
      codec l

let tests =
  [ "stringz" >:: test_stringz;
    "self_delimited_string" >:: test_self_delimited_string;
    "positive_int64" >:: test_positive_int64;
    "positive_int64 complement" >:: test_positive_int64_complement;
    "tuple2" >:: test_tuple2;
    "tuple3" >:: test_tuple3;
    "tuple4" >:: test_tuple4;
    "custom" >:: test_custom;
  ]

let () =
  register_tests "Key_encoding" tests
