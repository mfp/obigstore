(*
 * Copyright (M.Codec) 2011-2012 Mauricio Fernandez <mfp@acm.org>
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

let is_id_char = function
    'A'..'Z' | 'a'..'z' | '0'..'9' | '_' -> true
    | _ -> false

let char_is_id = function
    'A'..'Z' | 'a'..'z' | '0'..'9' | '_' -> true
  | _ -> false

let char_is_num = function
    '0'..'9' -> true
  | _ -> false

let all_chars_satisfy pred s =
  let ok_so_far = ref true in
  let i = ref 0 in
  let len = String.length s in
    while !ok_so_far && !i < len do
      ok_so_far := pred (String.unsafe_get s !i);
      incr i
    done;
    !ok_so_far

external is_printable: char -> bool = "caml_is_printable"

let hex_table =
  Array.init 16 (fun n -> let s = Printf.sprintf "%X" n in s.[0])

let escape_string s =
  let len =
    let n = ref 0 in
      for i = 0 to String.length s - 1 do
        n := !n + (match s.[i] with
                       '\\' | '"' -> 2
                     | c when is_printable c -> 1
                     | _ -> 4);
      done;
      !n in
  let ret = String.create len in
  let n = ref 0 in
    for i = 0 to String.length s - 1 do
      match s.[i] with
          '\\' | '"' as c -> ret.[!n] <- '\\';
                             ret.[!n+1] <- c;
                             n := !n + 2;
        | c when is_printable c -> ret.[!n] <- c;
                                   incr n
        | c -> ret.[!n] <- '\\';
               ret.[!n+1] <- 'x';
               ret.[!n+2] <- hex_table.(Char.code c / 16);
               ret.[!n+3] <- hex_table.(Char.code c land 0xF);
               n := !n + 4
    done;
    ret

let pp_escaped fmt s =
  Format.fprintf fmt "\"%s\"" (escape_string s)

let pp_base64 fmt s =
  Format.fprintf fmt "\"%s\""
    Cryptokit.(transform_string (Base64.encode_compact ()) s)

let pp_datum ~strict fmt = function
    s when strict -> pp_base64 fmt s
  | "" -> Format.fprintf fmt "\"\""
  | s when not strict && all_chars_satisfy char_is_num s -> Format.fprintf fmt "%s" s
  | s -> pp_escaped fmt s

let pp_key ~strict fmt s =
  if strict then pp_base64 fmt s
  else
    match s with
      "" -> Format.fprintf fmt "\"\""
    | s ->
        match s.[0] with
            'A'..'Z' | 'a'..'z' | '_' when not strict ->
                if all_chars_satisfy char_is_id s then Format.fprintf fmt "%s" s
                else pp_escaped fmt s
            | _ -> pp_escaped fmt s

let rec pp_list pp fmt = function
    [] -> ()
  | x :: [] -> pp fmt x
  | x :: tl -> pp fmt x;
               Format.fprintf fmt ",@ ";
               pp_list pp fmt tl
