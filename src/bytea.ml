
(*  Copyright 1999 Institut National de Recherche en Informatique et   *)
(*  en Automatique, 2011 Mauricio Fernandez                            *)

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

type t = Extprot.Msg_buffer.t =
 {mutable buffer : string;
  mutable position : int;
  mutable length : int;
  initial_buffer : string}

let create n =
 let n = if n < 1 then 1 else n in
 let n = if n > Sys.max_string_length then Sys.max_string_length else n in
 let s = String.create n in
 {buffer = s; position = 0; length = n; initial_buffer = s}

let contents b = String.sub b.buffer 0 b.position

let sub b ofs len =
  if ofs < 0 || len < 0 || ofs > b.position - len
  then invalid_arg "Bytea.sub"
  else begin
    let r = String.create len in
    String.blit b.buffer ofs r 0 len;
    r
  end

let blit src srcoff dst dstoff len =
  if len < 0 || srcoff < 0 || srcoff > src.position - len
             || dstoff < 0 || dstoff > (String.length dst) - len
  then invalid_arg "Bytea.blit"
  else
    String.blit src.buffer srcoff dst dstoff len

let nth b ofs =
  if ofs < 0 || ofs >= b.position then
   invalid_arg "Bytea.nth"
  else String.get b.buffer ofs

let length b = b.position

let clear b = b.position <- 0

let reset b =
  b.position <- 0; b.buffer <- b.initial_buffer;
  b.length <- String.length b.buffer

let resize b more =
  let len = b.length in
  let new_len = ref len in
  while b.position + more > !new_len do new_len := 2 * !new_len done;
  if !new_len > Sys.max_string_length then begin
    if b.position + more <= Sys.max_string_length
    then new_len := Sys.max_string_length
    else failwith "Bytea.add: cannot grow buffer"
  end;
  let new_buffer = String.create !new_len in
  String.blit b.buffer 0 new_buffer 0 b.position;
  b.buffer <- new_buffer;
  b.length <- !new_len

let add_char b c =
  let pos = b.position in
  if pos >= b.length then resize b 1;
  b.buffer.[pos] <- c;
  b.position <- pos + 1

let add_substring b s offset len =
  if offset < 0 || len < 0 || offset > String.length s - len
  then invalid_arg "Bytea.add_substring";
  let new_position = b.position + len in
  if new_position > b.length then resize b len;
  String.blit s offset b.buffer b.position len;
  b.position <- new_position

let add_string b s =
  let len = String.length s in
  let new_position = b.position + len in
  if new_position > b.length then resize b len;
  String.blit s 0 b.buffer b.position len;
  b.position <- new_position

let add_bytea b bs =
  add_substring b bs.buffer 0 bs.position

let add_byte b n = add_char b (Char.unsafe_chr n)

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

external ostore_bytea_blit_int64_complement_le : string -> int -> Int64.t -> unit =
  "ostore_bytea_blit_int64_complement_le" "noalloc"

external ostore_bytea_blit_int64_le : string -> int -> Int64.t -> unit =
  "ostore_bytea_blit_int64_le" "noalloc"

external ostore_bytea_blit_int32_le : string -> int -> int -> unit =
  "ostore_bytea_blit_int_as_i32_le" "noalloc"

let add_int64_complement_le b n =
  let new_position = b.position + 8 in
  if new_position > b.length then resize b 8;
  ostore_bytea_blit_int64_complement_le b.buffer b.position n;
  b.position <- new_position

let add_int64_le b n =
  let new_position = b.position + 8 in
  if new_position > b.length then resize b 8;
  ostore_bytea_blit_int64_le b.buffer b.position n;
  b.position <- new_position

let add_int32_le b n =
  let new_position = b.position + 4 in
    if new_position > b.length then resize b 4;
    ostore_bytea_blit_int32_le b.buffer b.position n;
    b.position <- new_position

let unsafe_string b = b.buffer
