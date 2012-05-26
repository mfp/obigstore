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
open Bm_util

let compressibility_ratio = 0.5

let num_pairs = ref None
let key_size = ref 16
let value_size = ref 32
let mode = ref `Random
let hotspots = ref 12

let usage_message = "Usage: bm_makedata N [options]"

let set_mixed_mode s =
  try
    let p, h = Scanf.sscanf s "%f:%d" (fun p h -> (p, h)) in
      mode := `Mixed p;
      hotspots := h;
  with
    | End_of_file ->
      let p = Scanf.sscanf s "%f" (fun p -> p) in
        mode := `Mixed p
    | Scanf.Scan_failure _ ->
        raise (Arg.Bad "mixed expects an argument like '0.95:8'")

let params = Arg.align
  [
    "-value-size", Arg.Set_int value_size, "N Value size (default: 32).";
    "-sequential", Arg.Unit (fun () -> mode := `Sequential),
      " Generate output with keys in lexicographic order.";
    "-mixed", Arg.String set_mixed_mode,
      "P:N Generate mixed load with 2^N hotspots and P(seq) = P.";
  ]

let prng = Cheapo_prng.make ~seed:(Random.int 0xFFFFFF)

let `Staged random_string = random_string_maker ~compressibility_ratio prng

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
    let num_pairs = match !num_pairs with
        None -> Arg.usage params usage_message; exit 1
      | Some n -> n in
    let gen = match !mode with
        `Mixed p_seq ->
          mixed_load_gen
            ~hotspots:!hotspots ~compressibility_ratio
            ~key_size:!key_size ~value_size:!value_size ~p_seq prng
      | `Random -> (fun _ -> (zero_pad !key_size (string_of_int (Cheapo_prng.next prng)),
                              random_string !value_size))
      | `Sequential -> (fun i -> (zero_pad !key_size (string_of_int i),
                                  random_string !value_size))
    in
      for_lwt i = 1 to num_pairs do
        let k, v = gen i in
          output_string k >>
          output_string v
      done >>
      Lwt_io.flush Lwt_io.stdout
  end

