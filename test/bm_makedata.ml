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

let hex_tbl = "0123456789abcdef"

let to_hex s =
  let r = String.create (String.length s * 2) in
  let off = ref 0 in
    for i = 0 to String.length s - 1 do
      let c = Char.code (String.unsafe_get s i) in
        String.unsafe_set r !off (String.unsafe_get hex_tbl (c lsr 4));
        String.unsafe_set r (!off + 1) (String.unsafe_get hex_tbl (c land 0xF));
        off := !off + 2
    done;
    r

let zero_pad n s =
  let len = String.length s in
    if len >= n then s
    else String.make (n - len) '0' ^ s

let output_string s =
  Lwt_io.LE.write_int Lwt_io.stdout (String.length s) >>
  Lwt_io.write Lwt_io.stdout s

type rng = { mutable seed : string; }

let cast_digest (x : Digest.t) : string = Obj.magic x

let random_string rng n =
  if n <= 16 then begin
    let x = cast_digest (Digest.string rng.seed) in
      rng.seed <- x;
      if n = 16 then x else String.sub x 0 n
  end else begin
    let s = String.create n in
    let rec fill_buf = function
        m when m <= 0 -> s
      | m -> let x = cast_digest (Digest.string rng.seed) in
               rng.seed <- x;
               String.blit x 0 s (n - m) (if m < 16 then m else 16);
               fill_buf (m - 16)
    in fill_buf n
  end

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
    let rng = { seed = "0123456789deadbeef" } in
    let num_pairs = match !num_pairs with
        None -> Arg.usage params usage_message; exit 1
      | Some n -> n
    in
      for_lwt i = 1 to num_pairs do
        let k = if !random then to_hex (random_string rng (!key_size / 2))
                else zero_pad !key_size (string_of_int i) in
        let v = to_hex (random_string rng (!value_size / 2)) in
          output_string k >>
          output_string v
      done >>
      Lwt_io.flush Lwt_io.stdout
  end

