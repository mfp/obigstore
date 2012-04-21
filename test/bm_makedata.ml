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


let rng = Cryptokit.Random.pseudo_rng
            (Digest.to_hex (Digest.string ""))

let make_rand_string size =
  Cryptokit.(transform_string (Hexa.encode ()) (Random.string rng 500_000))

let random_string_maker () =
  let buf = make_rand_string 500_000 in
  let ptr = ref 0 in
  let off = ref 0 in
    `Staged
      (fun n ->
         if n > String.length buf - !ptr then begin
           (* we shift the stream so that we don't get the same strings given
            * the same sequence of lengths *)
           incr off;
           ptr := !off;
         end;
         let s = String.sub buf !ptr n in
           ptr := !ptr + n;
           s)

let `Staged random_string = random_string_maker ()

let zero_pad n s =
  let len = String.length s in
    if len >= n then s
    else String.make (n - len) '0' ^ s

let incr_hex_char s off =
  match s.[off] with
      '0'..'8' as c -> s.[off] <- Char.chr (Char.code c + 1)
    | '9' -> s.[off] <- 'a'
    | 'a'..'e' as c -> s.[off] <- Char.chr (Char.code c + 1)
    | 'f' -> s.[off] <- '0'
    | _ -> ()

let rec incr_str off s =
  if off >= 0 then begin
    incr_hex_char s off;
    match s.[off] with
        '0' -> incr_str (off - 1) s
      | _ -> ()
  end

let incr_str s = incr_str (String.length s - 1) s

let cheapo_die p =
  let a = Array.init (1 lsl 13) (fun _ -> Random.float 1.0 < p) in
  let n = ref 0 in
    (fun () ->
       incr n;
       n := !n land (Array.length a - 1);
       a.(!n))

let mixed_load_gen ~key_size ~value_size ~p_seq =
  let `Staged random_string = random_string_maker () in
  let remembered = Array.init (1 lsl !hotspots) (fun _ -> random_string key_size) in
  let idx = ref 0 in
  let die = cheapo_die p_seq in
    (fun i ->
       incr idx;
       idx := !idx land (Array.length remembered - 1);
       let key =
         if die () then begin
           let s = remembered.(!idx) in
             incr_str s;
             s
         end else begin
           let s = random_string key_size in
             remembered.(!idx) <- s;
             incr idx;
             s
         end in
       let v = random_string value_size in
         (key, v))

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
          mixed_load_gen ~key_size:!key_size ~value_size:!value_size ~p_seq
      | `Random -> (fun _ -> (random_string !key_size, random_string !value_size))
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

