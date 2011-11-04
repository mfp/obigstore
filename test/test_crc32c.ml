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

let test_vectors =
  [
    range 0x01 0x28, [0x7F; 0x15; 0x2C; 0x0E];
    range 0x29 0x50, [0xF6; 0xEB; 0x80; 0xE9];
    range 0x51 0x78, [0xED; 0xBD; 0x74; 0xDE];
    range 0x79 0xA0, [0x62; 0xC8; 0x79; 0xD5];
    range 0xA1 0xC8, [0xD0; 0x9A; 0x97; 0xBA];
    range 0xC9 0xF0, [0x13; 0xD9; 0x29; 0x2B];
    range 0x01 0xF0, [0x75; 0xD3; 0xC5; 0x24];
  ]

let aeq_crc expected input =
  let check = assert_equal ~printer:to_hex in
    check ~msg:"Obs_crc32c.string" expected (Obs_crc32c.string input);
    check ~msg:"Obs_crc32c.substring" expected
      (Obs_crc32c.substring input 0 (String.length input));
    let crc = Obs_crc32c.create () in
    let check_with_update () =
      for i = 0 to String.length input - 1 do
        Obs_crc32c.update crc input i 1;
      done;
      check ~msg:"Obs_crc32c.update followed by result"
        expected (Obs_crc32c.result crc);
      check ~msg:"Obs_crc32c.update followed by unsafe_result"
        expected (Obs_crc32c.unsafe_result crc)
    in
      check_with_update ();
      Obs_crc32c.reset crc;
      check_with_update ()

let test_crc32c () =
  let test (input, output) =
    let input = string_of_bytes input in
    let output = string_of_bytes output in
      aeq_crc output input
  in List.iter test test_vectors

let () =
  register_tests "Obs_crc32c" ["test vectors" >:: test_crc32c]
