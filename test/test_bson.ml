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

let test_roundtrip (msg, doc) () =
  assert_equal ~msg doc Obs_bson.(document_of_string (string_of_document doc))

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
  ]

let () =
  register_tests "Obs_bson" tests
