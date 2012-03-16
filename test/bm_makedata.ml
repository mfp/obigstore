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
open Lwt

module C = Cryptokit

let num_pairs = ref None
let key_size = ref 16
let value_size = ref 32
let random = ref true

let usage_message = "Usage: bm_makedata N [options]"

let params = Arg.align
  [
    "-value-size", Arg.Set_int value_size, "N Value size (default: 32).";
    "-sequential", Arg.Clear random,
      " Generate output with keys in lexicographic order.";
  ]

let to_hex s = C.(transform_string (Hexa.encode ()) s)

let zero_pad n s =
  let len = String.length s in
    if len >= n then s
    else String.make (n - len) '0' ^ s

let output_string s =
  Lwt_io.LE.write_int Lwt_io.stdout (String.length s) >>
  Lwt_io.write Lwt_io.stdout s

let () =
  Arg.parse params
    (fun s -> match !num_pairs with
         None ->
           begin try
             num_pairs := Some (int_of_string s)
           with _ -> raise (Arg.Bad s)
           end
       | Some _ -> raise (Arg.Bad s))
    usage_message;
  Lwt_unix.run begin
    let rng = C.Random.pseudo_rng "0123456789deadbeef" in
    let num_pairs = match !num_pairs with
        None -> Arg.usage params usage_message; exit 1
      | Some n -> n
    in
      for_lwt i = 1 to num_pairs do
        let k = if !random then to_hex (C.Random.string rng (!key_size / 2))
                else zero_pad !value_size (string_of_int i) in
        let v = to_hex (C.Random.string rng (!value_size / 2)) in
          output_string k >>
          output_string v
      done >>
      Lwt_io.flush Lwt_io.stdout
  end

