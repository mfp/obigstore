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

open Printf
open Test_00util
open OUnit

module R = Random.State

let vector =
  let open Obs_bson in
  [
    "empty", [];
    "double", ["x", Double 1.0];
    "utf8", ["x", UTF8 ""];
    "utf8", ["x", UTF8 "bar"];
    "doc", ["x", Document []];
    "doc", ["x", Document ["x", Double 1.0]];
    "doc", ["x", Document ["x", Double 1.0; "y", UTF8 "x"]];
    "array", ["x", Array [Double 1.0; UTF8 ""]];
    "bin", ["x", Binary (Generic, "a")];
    "bin", ["x", Binary (Function, "a")];
    "bin", ["x", Binary (Old, "a")];
    "bin", ["x", Binary (UUID, "a")];
    "bin", ["x", Binary (MD5, "a")];
    "bin", ["x", Binary (UserDefined, "a")];
    "bool", ["x", Boolean true];
    "bool", ["x", Boolean false];
    "datetime", ["x", DateTime 14543555L];
    "null", ["x", Null];
    "regexp", ["x", Regexp ("a*b", "")];
    "js", ["x", JavaScript "1.0"];
    "symbol", ["x", Symbol "fff"];
    "jsscoped", ["x", JavaScriptScoped ("1.0", [])];
    "int32", ["x", Int32 42];
    "int32", ["x", Int32 154543];
    "timestamp", ["x", Timestamp 434343223L];
    "minkey", ["x", Minkey];
    "maxkey", ["x", Maxkey];
  ]

let rand_string rng = String.create (R.int rng 20)

let rand_cstring rng =
  Cryptokit.(transform_string (Hexa.encode ()) (rand_string rng))

let choose rng fs = fs.(R.int rng (Array.length fs)) rng

let random_double rng _ = Obs_bson.Double (R.float rng 1e6)

let random_utf8 rng _ = (* FIXME: ensure proper utf-8 *)
  Obs_bson.UTF8 (rand_string rng)

let random_binary rng _ =
  let open Obs_bson in
  Binary ([| Generic; Function; Old; UUID; MD5; UserDefined; |].(R.int rng 6),
          rand_string rng)

let random_objectid rng _ = Obs_bson.ObjectId (String.create 12)
let random_bool rng _ = Obs_bson.Boolean (R.bool rng)
let random_datetime rng _ = Obs_bson.DateTime (R.int64 rng Int64.max_int)
let random_regexp rng _ = Obs_bson.Regexp (rand_cstring rng, rand_cstring rng)
let random_javascript rng _ = Obs_bson.JavaScript (rand_string rng)
let random_symbol rng _ = Obs_bson.Symbol (rand_string rng)
let random_int32 rng _ = Obs_bson.Int32 (R.int rng 1_000_000)
let random_timestamp rng _ = Obs_bson.Timestamp (R.int64 rng Int64.max_int)
let random_int64 rng _ = Obs_bson.Int64 (R.int64 rng Int64.max_int)

let rec random_document_elm rng depth =
  if depth >= 4 then random_int32 rng depth
  else
    Obs_bson.Document (random_document rng depth)

and random_document rng depth =
  List.init (R.int rng 10) (fun i -> string_of_int i, random_element rng depth)

and random_array rng depth =
  if depth >= 4 then random_int32 rng depth else
    Obs_bson.Array (List.map snd (random_document rng depth))

and random_element rng depth =
  let fs =
    [| random_double; random_utf8; random_binary; random_objectid;
       random_bool; random_datetime; random_regexp; random_javascript;
       random_symbol; random_int32; random_timestamp; random_int64;
       random_document_elm; random_array; random_javascript_scoped;
    |]
  in choose rng fs (depth + 1)

and random_javascript_scoped rng depth =
  let context =
    if depth >= 4 then [] else random_document rng depth
  in
    Obs_bson.JavaScriptScoped (rand_string rng, context)

let hexdump s =
  let b = Buffer.create 13 in
  let fmt = Format.formatter_of_buffer b in
    for i = 0 to String.length s - 1 do
      let c = Char.code s.[i] in
        Format.fprintf fmt "%x%x@ " (c lsr 4) (c land 0xF);
    done;
    Format.fprintf fmt "@.";
    Buffer.contents b

let test_roundtrip (msg, doc) () =
  try
    assert_equal ~msg doc Obs_bson.(document_of_string (string_of_document doc))
  with exn ->
    assert_failure_fmt "Failed to deserialize %s\n%s"
      msg (hexdump (Obs_bson.string_of_document doc))

let test_roundtrip_randomized iterations () =
  let rng = Random.State.make [|1; 2; 3|] in
    for i = 1 to iterations do
      let doc = random_document rng 0 in
      let desc = sprintf "doc%d" i in
        test_roundtrip (desc, doc) ()
    done

let test_validation () =
  let open Obs_bson in

  let assert_does_not_validate ?truncate disallowed doc =
    try
      let s = string_of_document doc in
      let s = String.slice ?last:truncate s in
      let allowed = disallow [disallowed] all_allowed in
        validate_string ~allowed s;
        assert_failure_fmt "Should have disallowed %s"
          (string_of_allowed_elm disallowed)
    with
        Obs_bson.Malformed (Rejected s) when s = disallowed -> ()
      | Obs_bson.Malformed Truncated when truncate <> None -> () in

  let assert_does_not_validate disallowed doc =
    let len = String.length (string_of_document doc) in
      assert_does_not_validate disallowed doc;
      for i = 1 to len - 1 do
        assert_does_not_validate ~truncate:i disallowed doc
      done in

  let check (disallowed, elm) =
    let doc = ["x", elm] in
      begin try
        validate_string (string_of_document doc);
      with exn ->
        assert_failure_fmt "validation error %s" (Printexc.to_string exn)
      end;
      assert_does_not_validate disallowed doc;
      assert_does_not_validate disallowed ["nested", Document doc];
  in
    List.iter check
      [
        `Double, Double 42.0;
        `Double, Document ["foo", Double 42.0];
        `UTF8, UTF8 "foo";
        `Document, Document ["foo", Double 42.0];
        `Array, Document ["foo", Array [ Double 1.0 ]];
        `Binary, Binary (Generic, "");
        `Binary, Binary (Function, "");
        `Binary, Binary (Old, "");
        `Binary, Binary (UUID, "");
        `Binary, Binary (MD5, "");
        `Binary, Binary (UserDefined, "");
        `ObjectId, ObjectId "123456789012";
        `Boolean, Boolean true;
        `Boolean, Boolean false;
        `DateTime, DateTime 1L;
        `Null, Null;
        `Regexp, Regexp ("a*b", "");
        `JavaScript, JavaScript "foo";
        `Symbol, Symbol "bar";
        `JavaScriptScoped, JavaScriptScoped ("xx", []);
        `Int32, Int32 334324234;
        `Timestamp, Timestamp 1434252353L;
        `Int64, Int64 334324232334L;
        `Minkey, Minkey;
        `Maxkey, Maxkey;
      ]

let tests =
  [
    "roundtrip" >:::
      List.map (fun ((msg, doc) as x) -> msg >:: test_roundtrip x) vector;
    "validation" >:: test_validation;
    "randomized roundtrip" >:: test_roundtrip_randomized 10000;
  ]

let () =
  register_tests "Obs_bson" tests
