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

module List = BatList

module K = Obs_key_encoding

let check_roundtrip codec x =
  let b = Obs_bytea.create 13 in
  let printer x =
    sprintf "%s (encoded as %S)" (K.pp codec x) (K.encode_to_string codec x)
  in
    K.encode codec b x;
    aeq printer x (K.decode_string codec (Obs_bytea.contents b))

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
    with K.Error (K.Unsatisfied_constraint _, "Obs_key_encoding.stringz.encode") -> ()

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

let test_bool () =
  let v = [ true; false ] in
    List.iter (check_roundtrip K.bool) v;
    check_order_preservation K.bool v

let test_byte () =
  let v = List.init 256 (fun n -> n) in
    List.iter (check_roundtrip K.byte) v;
    check_order_preservation K.byte v

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

let test_tuple5 () =
  let codec = K.tuple5 K.stringz K.stringz K.stringz K.stringz K.stringz in
  let l1 =
    [ "", "", "", ""; "a", "", "", ""; "", "a", "", ""; "abc", "def", "", "";
      "asdsdsa", "", "x", "sdfdsfssdf" ] in
  let suffix = [ ""; "a"; "x"; "abc" ] in
  let l =
    List.concat
      (List.map
         (fun (a, b, c, d) -> List.map (fun e -> (a, b, c, d, e)) suffix) l1)
  in
    List.iter (check_roundtrip codec) l;
    check_order_preservation codec l

let test_choice2 () =
  let codec = K.choice2 "a" K.stringz "b" K.stringz in
  let l = [`A ""; `B ""; `A "\001"; `B "\001"; `A "foo"; ] in
    List.iter (check_roundtrip codec) l;
    check_order_preservation codec l

let test_choice3 () =
  let codec = K.choice3 "a" K.stringz "b" K.stringz "c" K.byte in
  let l = [`A ""; `B ""; `A "\001"; `C 0; `B "\001"; `A "foo"; `C 1] in
    List.iter (check_roundtrip codec) l;
    check_order_preservation codec l

let test_choice4 () =
  let codec = K.choice4 "a" K.stringz "b" K.stringz "c" K.byte "d" K.byte in
  let l = [`A ""; `B ""; `A "\001"; `C 0; `B "\001"; `A "foo"; `C 1; `D 1] in
    List.iter (check_roundtrip codec) l;
    check_order_preservation codec l

let test_choice5 () =
  let codec = K.choice5 "a" K.stringz "b" K.stringz "c" K.byte "d" K.byte "e" K.byte in
  let l = [`A ""; `B ""; `A "\001"; `C 0; `B "\001"; `A "foo"; `C 1; `D 1;
           `E 0; `E 1; `C 0 ]
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

open K

let test_min () =
  let check c expected =
    assert_equal ~printer:(K.pp c) expected (min c) in
  let check_ints c min =
    check c min;
    check (c *** c) (min, min);
    check (c *** c *** c) (min, (min, min));
    check (tuple3 c c c) (min, min, min) in
  let check_strings c =
    check c "";
    check (c *** c) ("", "");
    check (c *** c *** c) ("", ("", ""));
    check (tuple3 c c c) ("", "", "")
  in
    check bool false;
    check bool false;
    check_ints byte 0;
    check_ints positive_int64 Int64.zero;
    check_ints positive_int64_complement Int64.zero;
    check_strings self_delimited_string;
    check_strings stringz;
    check_strings stringz_unsafe

let test_max () =
  let check c expected =
    assert_equal ~printer:(pp c) expected (max c) in
  let check_ints c m =
    check c m;
    check (c *** c) (m, m);
    check (c *** c *** c) (m, (m, m));
    check (tuple3 c c c) (m, m, m) in
  let check_strings c m =
    check c m;
    check (c *** c) (m, m);
    check (c *** c *** c) (m, (m, m));
    check (tuple3 c c c) (m, m, m);
  in
    check bool true;
    check bool true;
    check_ints byte 255;
    check_ints positive_int64 Int64.max_int;
    check_ints positive_int64_complement Int64.max_int;
    check_strings self_delimited_string (max stringz);
    check_strings stringz (max stringz);
    check_strings stringz_unsafe (max stringz)

let test_lower () =
  let check c l =
    List.iter
      (fun (f, expected, orig) ->
         assert_equal ~printer:(K.pp c) expected (f c orig))
      l in
  let check_ints c (!!) =
    check (c *** c)
      [
        lower1, (!!0, !!0), (!!0, !!1);
        lower1, (!!0, !!0), (!!0, !!255);
        lower1, (!!42, !!0), (!!42, !!255);
      ];
    check (tuple3 c c c)
      [
        lower1, (!!1, !!0, !!0), (!!1, !!2, !!3);
        lower1, (!!1, !!0, !!0), (!!1, !!255, !!0);
        lower2, (!!1, !!2, !!0), (!!1, !!2, !!3);
        lower2, (!!1, !!255, !!0), (!!1, !!255, !!3);
      ];
    check (tuple4 c c c c)
      [
        lower1, (!!1, !!0, !!0, !!0), (!!1, !!2, !!3, !!4);
        lower1, (!!1, !!0, !!0, !!0), (!!1, !!255, !!3, !!4);
        lower2, (!!1, !!2, !!0, !!0), (!!1, !!2, !!3, !!4);
        lower2, (!!1, !!255, !!0, !!0), (!!1, !!255, !!3, !!4);
        lower3, (!!1, !!2, !!3, !!0), (!!1, !!2, !!3, !!4);
        lower3, (!!1, !!255, !!3, !!0), (!!1, !!255, !!3, !!4);
      ];
    check (tuple5 c c c c c)
      [
        lower1, (!!1, !!0, !!0, !!0, !!0), (!!1, !!2, !!3, !!4, !!5);
        lower1, (!!1, !!0, !!0, !!0, !!0), (!!1, !!255, !!3, !!4, !!5);
        lower2, (!!1, !!2, !!0, !!0, !!0), (!!1, !!2, !!3, !!4, !!5);
        lower2, (!!1, !!255, !!0, !!0, !!0), (!!1, !!255, !!3, !!4, !!5);
        lower3, (!!1, !!2, !!3, !!0, !!0), (!!1, !!2, !!3, !!4, !!5);
        lower3, (!!1, !!255, !!3, !!0, !!0), (!!1, !!255, !!3, !!4, !!5);
        lower4, (!!1, !!2, !!3, !!4, !!0), (!!1, !!2, !!3, !!4, !!5);
        lower4, (!!1, !!255, !!3, !!4, !!0), (!!1, !!255, !!3, !!4, !!5);
      ] in
  let check_strings c =
    check (c *** c) [ lower1, ("1", ""), ("1", "2"); ];
    check (tuple3 c c c)
      [
        lower1, ("1", "", ""), ("1", "2", "3");
        lower2, ("1", "2", ""), ("1", "2", "3");
      ]
  in
    check (bool *** bool) [ lower1, (true, false), (true, true) ];
    check_ints byte (fun x -> x);
    check_ints positive_int64 Int64.of_int;
    check_ints positive_int64_complement Int64.of_int;
    check_strings self_delimited_string;
    check_strings stringz;
    check_strings stringz_unsafe

let test_upper () =
  let check c l =
    List.iter
      (fun (f, expected, orig) ->
         assert_equal ~printer:(K.pp c) expected (f c orig))
      l in
  let check_ints c (!!) =
    let m = K.max c in
      check (c *** c)
        [
          upper1, (!!0, m), (!!0, !!1);
          upper1, (!!0, m), (!!0, !!255);
          upper1, (!!42, m), (!!42, !!255);
        ];
      check (tuple3 c c c)
        [
          upper1, (!!1, m, m), (!!1, !!2, !!3);
          upper1, (!!1, m, m), (!!1, !!255, !!0);
          upper2, (!!1, !!2, m), (!!1, !!2, !!3);
          upper2, (!!1, !!255, m), (!!1, !!255, !!3);
        ];
      check (tuple4 c c c c)
        [
          upper1, (!!1, m, m, m), (!!1, !!2, !!3, !!4);
          upper1, (!!1, m, m, m), (!!1, !!255, !!3, !!4);
          upper2, (!!1, !!2, m, m), (!!1, !!2, !!3, !!4);
          upper2, (!!1, !!255, m, m), (!!1, !!255, !!3, !!4);
          upper3, (!!1, !!2, !!3, m), (!!1, !!2, !!3, !!4);
          upper3, (!!1, !!255, !!3, m), (!!1, !!255, !!3, !!4);
        ];
      check (tuple5 c c c c c)
        [
          upper1, (!!1, m, m, m, m), (!!1, !!2, !!3, !!4, !!5);
          upper1, (!!1, m, m, m, m), (!!1, !!255, !!3, !!4, !!5);
          upper2, (!!1, !!2, m, m, m), (!!1, !!2, !!3, !!4, !!5);
          upper2, (!!1, !!255, m, m, m), (!!1, !!255, !!3, !!4, !!5);
          upper3, (!!1, !!2, !!3, m, m), (!!1, !!2, !!3, !!4, !!5);
          upper3, (!!1, !!255, !!3, m, m), (!!1, !!255, !!3, !!4, !!5);
          upper4, (!!1, !!2, !!3, !!4, m), (!!1, !!2, !!3, !!4, !!5);
          upper4, (!!1, !!255, !!3, !!4, m), (!!1, !!255, !!3, !!4, !!5);
        ] in
  let check_strings c =
    let m = K.max c in
      check (c *** c) [ upper1, ("1", m), ("1", "2"); ];
      check (tuple3 c c c)
        [
          upper1, ("1", m, m), ("1", "2", "3");
          upper2, ("1", "2", m), ("1", "2", "3");
        ]
  in
    check (bool *** bool) [ upper1, (false, true), (false, false) ];
    check_ints byte (fun x -> x);
    check_ints positive_int64 Int64.of_int;
    check_ints positive_int64_complement Int64.of_int;
    check_strings self_delimited_string;
    check_strings stringz;
    check_strings stringz_unsafe

let test_expand_max () =
  let check f c l =
    List.iter
      (fun (expected, t) ->
         assert_equal ~printer:(K.pp c) expected (f c t))
      l in
  let check_ints c (!!) =
    let m = K.max c in
      check expand_max1 (c *** c)
        [ (!!0, m), !!0; (!!1, m), !!1; ];
      check expand_max1 (tuple3 c c c)
        [ (!!1, m, m), !!1; (!!2, m, m), !!2; ];
      check expand_max2 (tuple4 c c c c)
        [
          (!!1, !!2, m, m), (!!1, !!2);
          (!!1, !!255, m, m), (!!1, !!255);
        ];
      check expand_max3 (tuple5 c c c c c)
        [
          (!!1, !!2, !!3, m, m), (!!1, !!2, !!3);
          (!!1, !!255, !!3, m, m), (!!1, !!255, !!3);
        ];
      let c5 = tuple5 c c c c c in
        assert_equal ~printer:(K.pp c5)
          (!!1, !!2, m, m, m)
          (expand (part !!1 @@ part !!2 @@ max_suffix) c5) in
  let check_strings c =
    let m = K.max c in
      check expand_max1 (c *** c)
        [ ("1", m), "1"; ("2", m), "2"; ];
      check expand_max2 (tuple3 c c c)
        [
          ("1", "2", m), ("1", "2");
          ("1", "", m), ("1", "");
        ]
  in
    check expand_max1 (bool *** bool)
      [ (false, true), false; (true, true), true; ];
    check_ints byte (fun x -> x);
    check_ints positive_int64 Int64.of_int;
    check_ints positive_int64_complement Int64.of_int;
    check_strings self_delimited_string;
    check_strings stringz;
    check_strings stringz_unsafe

let test_expand_min () =
  let check f c l =
    List.iter
      (fun (expected, t) ->
         assert_equal ~printer:(K.pp c) expected (f c t))
      l in
  let check_ints c (!!) =
    let m = K.min c in
      check expand_min1 (c *** c)
        [ (!!0, m), !!0; (!!1, m), !!1; ];
      check expand_min1 (tuple3 c c c)
        [ (!!1, m, m), !!1; (!!2, m, m), !!2; ];
      check expand_min2 (tuple4 c c c c)
        [
          (!!1, !!2, m, m), (!!1, !!2);
          (!!1, !!255, m, m), (!!1, !!255);
        ];
      check expand_min3 (tuple5 c c c c c)
        [
          (!!1, !!2, !!3, m, m), (!!1, !!2, !!3);
          (!!1, !!255, !!3, m, m), (!!1, !!255, !!3);
        ];
      let c5 = tuple5 c c c c c in
        assert_equal ~printer:(K.pp c5)
          (!!1, !!2, !!0, !!0, !!0)
          (expand (part !!1 @@ part !!2 @@ min_suffix) c5)
  in
  let check_strings c =
    let m = K.min c in
      check expand_min1 (c *** c)
        [ ("1", m), "1"; ("2", m), "2"; ];
      check expand_min2 (tuple3 c c c)
        [
          ("1", "2", m), ("1", "2");
          ("1", "", m), ("1", "");
        ]
  in
    check expand_min1 (bool *** bool)
      [ (false, false), false; (true, false), true; ];
    check_ints byte (fun x -> x);
    check_ints positive_int64 Int64.of_int;
    check_ints positive_int64_complement Int64.of_int;
    check_strings self_delimited_string;
    check_strings stringz;
    check_strings stringz_unsafe

let tests =
  [ "stringz" >:: test_stringz;
    "byte" >:: test_byte;
    "bool" >:: test_bool;
    "self_delimited_string" >:: test_self_delimited_string;
    "positive_int64" >:: test_positive_int64;
    "positive_int64 complement" >:: test_positive_int64_complement;
    "tuple2" >:: test_tuple2;
    "tuple3" >:: test_tuple3;
    "tuple4" >:: test_tuple4;
    "tuple5" >:: test_tuple5;
    "custom" >:: test_custom;
    "choice2" >:: test_choice2;
    "choice3" >:: test_choice3;
    "choice4" >:: test_choice4;
    "choice5" >:: test_choice5;
    "min" >:: test_min;
    "max" >:: test_max;
    (* "succ" >:: test_succ; *)
    (* "pred" >:: test_pred; *)
    "lower" >:: test_lower;
    "upper" >:: test_upper;
    "expand_max" >:: test_expand_max;
    "expand_min" >:: test_expand_min;
  ]

let () =
  register_tests "Obs_key_encoding" tests
