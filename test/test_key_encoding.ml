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

let test_roundtrip codec x =
  let b = Bytea.create 13 in
  let printer x =
    sprintf "%s (encoded as %S)" (K.pp codec x) (K.encode_to_string codec x)
  in
    K.encode codec b x;
    aeq printer x (K.decode_string codec (Bytea.contents b))

let test_stringz () =
  let v =
    [""; "a"; "ab"; "abcd"; "\001ab"; String.make 200 'x' ]
  in
    List.iter (test_roundtrip K.stringz) v;
    List.iter (test_roundtrip K.stringz_unsafe) v;
    try
      test_roundtrip K.stringz "a\000";
      assert_failure "Expected Unsatisfied_constraint error"
    with K.Error (K.Unsatisfied_constraint _) -> ()

let int64_vector =
    [ 0L; -1L; Int64.max_int; Int64.min_int; 42L; ]

let test_int64 () = List.iter (test_roundtrip K.int64) int64_vector

let test_int64_complement () =
  List.iter (test_roundtrip K.int64) int64_vector

let test_tuple2 () =
  List.iter
    (test_roundtrip (K.tuple2 K.stringz K.stringz))
    [ "", ""; "a", ""; "", "a"; "abc", "def"; "asdsdsa", "sdfdsfssdf" ]

let test_tuple3 () =
  List.iter
    (test_roundtrip (K.tuple3 K.stringz K.stringz K.stringz))
    [ "", "", ""; "a", "", ""; "", "a", ""; "abc", "def", "";
      "asdsdsa", "", "sdfdsfssdf" ]

let test_tuple4 () =
  List.iter
    (test_roundtrip (K.tuple4 K.stringz K.stringz K.stringz K.stringz))
    [ "", "", "", ""; "a", "", "", ""; "", "a", "", ""; "abc", "def", "", "";
      "asdsdsa", "", "x", "sdfdsfssdf" ]

let test_custom () =
  let encode s = Scanf.sscanf s "%Ld-%Ld-%s" (fun a b c -> (a, b, c)) in
  let decode (a, b, c) = Printf.sprintf "%Ld-%Ld-%s" a b c in
  let pp x = x in
  let c = K.custom
            (K.tuple3 K.int64 K.int64_complement K.stringz)
            ~encode ~decode ~pp
  in
    List.iter (test_roundtrip c)
      [
        "32-43-"; "-1--1-"; "-1--1-bar"; "42-42-foobar";
      ]

let tests =
  [ "stringz" >:: test_stringz;
    "int64" >:: test_int64;
    "int64 complement" >:: test_int64_complement;
    "tuple2" >:: test_tuple2;
    "tuple3" >:: test_tuple3;
    "tuple4" >:: test_tuple4;
    "custom" >:: test_custom;
  ]

let () =
  register_tests "Key_encoding" tests
