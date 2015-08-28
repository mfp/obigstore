
(*  Copyright 1999 Institut National de Recherche en Informatique et   *)
(*  en Automatique, 2011-2012 Mauricio Fernandez                            *)

(* the original carried this copyright notice: *)

(***********************************************************************)
(*                                                                     *)
(*                           Objective Caml                            *)
(*                                                                     *)
(*   Pierre Weis and Xavier Leroy, projet Cristal, INRIA Rocquencourt  *)
(*                                                                     *)
(*  Copyright 1999 Institut National de Recherche en Informatique et   *)
(*  en Automatique.  All rights reserved.  This file is distributed    *)
(*  under the terms of the GNU Library General Public License, with    *)
(*  the special exception on linking described in file ../LICENSE.     *)
(*                                                                     *)
(***********************************************************************)

include Extprot.Msg_buffer

let create = make

let copy b =
  {
    position = b.position;
    length   = b.position;
    buffer   = String.sub b.buffer 0 b.position;
    release  = (fun () -> ());
  }

let sub b ofs len =
  if ofs < 0 || len < 0 || ofs > b.position - len
  then invalid_arg "Obs_bytea.sub"
  else begin
    let r = String.create len in
    String.blit b.buffer ofs r 0 len;
    r
  end

let blit src srcoff dst dstoff len =
  if len < 0 || srcoff < 0 || srcoff > src.position - len
             || dstoff < 0 || dstoff > (String.length dst) - len
  then invalid_arg "Obs_bytea.blit"
  else
    String.blit src.buffer srcoff dst dstoff len

let nth b ofs =
  if ofs < 0 || ofs >= b.position then
   invalid_arg "Obs_bytea.nth"
  else String.get b.buffer ofs

let add_bytea b bs =
  add_substring b bs.buffer 0 bs.position

let add_vint b n =
  let n = ref n in
    while !n land -128 <> 0 do
      add_byte b (128 lor (!n land 0x7f));
      n := !n lsr 7
    done;
    add_byte b !n

let rec vint_len = function
    n when n < 128 -> 1
  | n -> 1 + vint_len (n lsr 7)

let add_rev_vint b n =
  let len = vint_len n in
  let new_position = b.position + len in
  if new_position > b.length then resize b len;
  let n = ref n in
  let pos = ref (new_position - 1) in
    while !n <> 0 do
      if !n < 128 then
        b.buffer.[!pos] <- Char.unsafe_chr (!n land 0x7F)
      else
        b.buffer.[!pos] <- Char.unsafe_chr ((!n land 0x7F) lor 0x80);
      n := !n lsr 7;
      decr pos
    done;
    b.position <- new_position

external obigstore_bytea_blit_int64_complement_le : string -> int -> Int64.t -> unit =
  "obigstore_bytea_blit_int64_complement_le" "noalloc"

external obigstore_bytea_blit_int64_le : string -> int -> Int64.t -> unit =
  "obigstore_bytea_blit_int64_le" "noalloc"

external obigstore_bytea_blit_int32_le : string -> int -> int -> unit =
  "obigstore_bytea_blit_int_as_i32_le" "noalloc"

external obigstore_bytea_blit_int64_complement_be : string -> int -> Int64.t -> unit =
  "obigstore_bytea_blit_int64_complement_be" "noalloc"

external obigstore_bytea_blit_int64_be : string -> int -> Int64.t -> unit =
  "obigstore_bytea_blit_int64_be" "noalloc"

external obigstore_bytea_blit_int32_be : string -> int -> int -> unit =
  "obigstore_bytea_blit_int_as_i32_be" "noalloc"

let add_int64_complement_le b n =
  let new_position = b.position + 8 in
  if new_position > b.length then resize b 8;
  obigstore_bytea_blit_int64_complement_le b.buffer b.position n;
  b.position <- new_position

let add_int64_le b n =
  let new_position = b.position + 8 in
  if new_position > b.length then resize b 8;
  obigstore_bytea_blit_int64_le b.buffer b.position n;
  b.position <- new_position

let add_int32_le b n =
  let new_position = b.position + 4 in
    if new_position > b.length then resize b 4;
    obigstore_bytea_blit_int32_le b.buffer b.position n;
    b.position <- new_position

let add_int64_complement_be b n =
  let new_position = b.position + 8 in
  if new_position > b.length then resize b 8;
  obigstore_bytea_blit_int64_complement_be b.buffer b.position n;
  b.position <- new_position

let add_int64_be b n =
  let new_position = b.position + 8 in
  if new_position > b.length then resize b 8;
  obigstore_bytea_blit_int64_be b.buffer b.position n;
  b.position <- new_position

let add_int32_be b n =
  let new_position = b.position + 4 in
    if new_position > b.length then resize b 4;
    obigstore_bytea_blit_int32_be b.buffer b.position n;
    b.position <- new_position

let add_ieee754_float b f = add_int64_le b (Int64.bits_of_float f)

let unsafe_blit_int32_le_at b ~off n =
  obigstore_bytea_blit_int32_le b.buffer off n

let blit_int32_le_at b ~off n =
  if off < 0 || off > b.position - 4 then
    invalid_arg "Obs_bytea.blit_int32_le_at: invalid off";
  unsafe_blit_int32_le_at b ~off n

let unsafe_string b = b.buffer
