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

let aeq expected b =
  assert_equal ~printer:to_hex expected (Obs_bytea.contents b)

let test_add_int32_le b =
  Obs_bytea.add_int32_le b (-1);
  aeq (string_of_bytes [ 0xFF; 0xFF; 0xFF; 0xFF ]) b;
  Obs_bytea.add_int32_le b 0xabcd;
  aeq (string_of_bytes [ 0xFF; 0xFF; 0xFF; 0xFF; 0xcd; 0xab; 0x00; 0x00; ]) b;
  Obs_bytea.clear b;
  Obs_bytea.add_int32_le b 0x3bcd0102;
  aeq (string_of_bytes [ 0x02; 0x01; 0xcd; 0x3b; ]) b

let test_add_int64_le b =
  Obs_bytea.add_int64_le b (-1L);
  aeq (String.make 8 '\255') b;
  Obs_bytea.add_int64_le b 0x1bcd010203040506L;
  aeq (String.make 8 '\255' ^
       string_of_bytes [ 0x06; 0x05; 0x04; 0x03; 0x02; 0x01; 0xcd; 0x1b ]) b

let test_add_int32_be b =
  Obs_bytea.add_int32_be b (-1);
  aeq (string_of_bytes [ 0xFF; 0xFF; 0xFF; 0xFF ]) b;
  Obs_bytea.add_int32_be b 0xabcd;
  aeq (string_of_bytes [ 0xFF; 0xFF; 0xFF; 0xFF; 0x00; 0x00; 0xab; 0xcd; ]) b;
  Obs_bytea.clear b;
  Obs_bytea.add_int32_be b 0x3bcd0102;
  aeq (string_of_bytes [ 0x3b; 0xcd; 0x01; 0x02; ]) b

let test_add_int64_be b =
  Obs_bytea.add_int64_be b (-1L);
  aeq (String.make 8 '\255') b;
  Obs_bytea.add_int64_be b 0x1bcd010203040506L;
  aeq (String.make 8 '\255' ^
       string_of_bytes [ 0x1b; 0xcd; 0x01; 0x02; 0x03; 0x04; 0x05; 0x06; ]) b

let with_buf f () = f (Obs_bytea.create 10)

let tests =
  List.map (fun (n, f) -> n >:: with_buf f)
    [
      "add_int32_le", test_add_int32_le;
      "add_int64_le", test_add_int64_le;
      "add_int32_be", test_add_int32_be;
      "add_int64_be", test_add_int64_be;
    ]

let () =
  register_tests "Obs_bytea" tests
