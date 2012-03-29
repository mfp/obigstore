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

external decode_int64_be :
  string -> int -> Int64.t = "obigstore_decode_int64_be"

external decode_int64_complement_be :
  string -> int -> Int64.t = "obigstore_decode_int64_complement_be"

type error =
    Unsatisfied_constraint of string
  | Incomplete_fragment of string
  | Bad_encoding of string
  | Unknown_tag of int

exception Error of error * string

let string_of_error = function
    Unsatisfied_constraint s -> sprintf "Unsatisfied_constraint %S" s
  | Incomplete_fragment s -> sprintf "Incomplete_fragment %S" s
  | Bad_encoding s -> sprintf "Bad_encoding %S" s
  | Unknown_tag n -> sprintf "Unknown_tag %d" n

let () =
  Printexc.register_printer
    (function
       | Error (x, where) -> Some (sprintf "Error (%s, %S)"
                                     (string_of_error x) where)
       | _ -> None)

type 'a encoder = Obs_bytea.t -> 'a -> unit

(* [decode s off len scratch] *)
type 'a decoder = string -> off:int ref -> len:int -> Obs_bytea.t -> 'a

type ('a, 'b, 'parts) codec =
    {
      min : 'a;
      max : 'a;
      encode : 'a encoder;
      decode : 'a decoder;
      succ : 'a -> 'a;
      pred : 'a -> 'a;
      inject : 'b -> 'a;
      extract : 'a -> 'b;
      pp : 'b -> string;
      parts : 'parts;
    }

type 'a primitive_codec = ('a, 'a, unit) codec

type ('a, 'b, 'ma, 'mb, 'ta, 'tb) cons =
    ('a, 'ma, 'ta) codec * ('b, 'mb, 'tb) codec

let encode c b x = c.encode b (c.inject x)

let encode_to_string c x =
  let b = Obs_bytea.create 13 in
    encode c b x;
    Obs_bytea.contents b

let decode c s ~off ~len scratch =
  c.extract (c.decode s ~off:(ref off) ~len scratch)

let decode_string c s =
  decode c s ~off:0 ~len:(String.length s) (Obs_bytea.create 8)

let split
      (c : (_, _, ('a, 'b, 'ma, 'mb, 'ta, 'tb) cons) codec) :
      ('a, 'ma, 'ta) codec * ('b, 'mb, 'tb) codec =
  c.parts

let skip f (c : ('a * 'b, _, _) codec) ((x : 'a), y) =
  let _, c2 = split c in
    (x, f c2 y)

let lower1 (c : ('a * 'b, _, _) codec) ((x : 'a), (y : 'b)) =
  let c1, c2 = split c in
    (x, c2.min)

let upper1 (c : ('a * 'b, _, _) codec) ((x : 'a), (y : 'b)) =
  let c1, c2 = split c in
    (x, c2.max)

let pp c x = c.pp x
let min c = c.extract c.min
let max c = c.extract c.max
let succ c x = c.extract (c.succ (c.inject x))
let pred c x = c.extract (c.pred (c.inject x))

let lift f c x = c.extract (f c (c.inject x))

let lower5 c x = lift (skip (skip (skip (skip lower1)))) c x
let lower4 c x = lift (skip (skip (skip lower1))) c x
let lower3 c x = lift (skip (skip lower1)) c x
let lower2 c x = lift (skip lower1) c x
let lower1 c x = lift lower1 c x

let upper5 c x = lift (skip (skip (skip (skip upper1)))) c x
let upper4 c x = lift (skip (skip (skip upper1))) c x
let upper3 c x = lift (skip (skip upper1)) c x
let upper2 c x = lift (skip upper1) c x
let upper1 c x = lift upper1 c x

let error where e = raise (Error (e, "Obs_key_encoding." ^ where))

let invalid_off_len ~fname s off len =
  raise
    (Invalid_argument
       (sprintf "Obs_key_encoding.%s decoding: \
                 invalid offset (%d) or length (%d) in string of length %d"
          fname off len (String.length s)))

let check_off_len fname s off len =
  if off < 0 || len < 0 || off + len > String.length s then
    invalid_off_len ~fname s off len

type delim_string_dec_state = Normal | Got_zero

let id x = x

let max_string = String.make 256 '\xFF'

let string_succ s = s ^ "\000"

let string_pred = function
    "" -> ""
  | s ->
      let s = String.copy s in
      let n = ref (String.length s - 1) in
      let finished = ref false in
        while !n >= 0 && not !finished do
          begin
            match String.unsafe_get s !n with
                '\x00' -> ()
              | c -> s.[!n] <- Char.chr (Char.code c - 1);
                     finished := true
          end;
          decr n;
        done;
        s

let self_delimited_string =
  let encode b s =
    for i = 0 to String.length s - 1 do
      match String.unsafe_get s i with
          c when c <> '\000' -> Obs_bytea.add_char b c
        | _ (* \000 *) -> Obs_bytea.add_string b "\000\001"
    done;
    Obs_bytea.add_string b "\000\000" in

  let decode s ~off ~len scratch =
    check_off_len "self_delimited_string" s !off len;
    let max = !off + len in
    let finished = ref false in
    let state = ref Normal in
    let b = scratch in
      Obs_bytea.clear b;
      while !off < max && not !finished do
        begin match !state with
            Normal -> begin match String.unsafe_get s !off with
                c when c <> '\000' -> Obs_bytea.add_char b c
              | _ (* \000 *) -> state := Got_zero
            end
          | Got_zero -> match String.unsafe_get s !off with
                '\000' -> finished := true; state := Normal
              | '\001' -> Obs_bytea.add_char b '\000'; state := Normal;
              | _ -> error "self_delimited_string.decode"
                       (Bad_encoding "self_delimited_string")
        end;
        incr off
      done;
      if not !finished then
        error "self_delimited_string.decode"
          (Incomplete_fragment "self_delimited_string");
      if !state <> Normal then
        error "self_delimited_string.decode"
          (Bad_encoding "self_delimited_string");
      Obs_bytea.contents b in

  let pp = sprintf "%S" in
  let inject = id in
  let extract = id in
    { min = ""; max = max_string; encode; decode;
      succ = string_succ; pred = string_pred; inject; extract;
      pp; parts = ();
    }

let stringz =
  let encode b s =
    try
      ignore (String.index s '\000');
      error "stringz.encode" (Unsatisfied_constraint "non null")
    with Not_found ->
      Obs_bytea.add_string b s;
      Obs_bytea.add_char b '\000' in

  let decode s ~off ~len scratch =
    check_off_len "stringz" s !off len;
    let off0 = !off in
    let max = !off + len in
    let finished = ref false in
      while !off <= max && not !finished do
        if s.[!off] <> '\000' then incr off
        else finished := true
      done;
      if not !finished then error "stringz.decode" (Incomplete_fragment "stringz");
      let s = String.sub s off0 (!off - off0) in
        incr off;
        s

  in { self_delimited_string with encode; decode; }

let byte =
  let encode b x = Obs_bytea.add_byte b x in
  let decode s ~off ~len scratch =
    check_off_len "bool" s !off len;
    if len < 1 then invalid_off_len ~fname:"byte" s !off len;
    let n = Char.code s.[!off] in
      incr off;
      n in
  let succ n = if n < 255 then n + 1 else 255 in
  let pred n = if n > 0 then n - 1 else 0 in
  let pp = sprintf "0x%02X" in
    { min = 0; max = 255; encode; decode; succ; pred; pp;
      inject = id; extract = id; parts = ();
    }

let bool =
  let encode b x = Obs_bytea.add_byte b (if x then 1 else 0) in
  let decode s ~off ~len scratch =
    check_off_len "bool" s !off len;
    if len < 1 then invalid_off_len ~fname:"bool" s !off len;
    let b = not (s.[!off] = '\000') in
      incr off;
      b in
  let succ b = true in
  let pred b = false in
    { min = false; max = true; encode; decode; pp = string_of_bool;
      succ; pred; inject = id; extract = id; parts = ();
    }

let stringz_unsafe =
  let encode b s = Obs_bytea.add_string b s; Obs_bytea.add_byte b 0 in
    { stringz with encode; }

let positive_int64 =
  let encode b n =
    if Int64.compare n 0L < 0 then
      error "positive_int64.encode" (Unsatisfied_constraint "negative Int64.t");
    Obs_bytea.add_int64_be b n in

  let decode s ~off ~len scratch =
    check_off_len "positive_int64" s !off len;
    if len < 8 then invalid_off_len ~fname:"positive_int64" s !off len;
    let r = decode_int64_be s !off in
      off := !off + 8;
      r in

  let succ n = if n = Int64.max_int then n else Int64.add n 1L in
  let pred n = if n = 0L then n else Int64.sub n 1L in

    { min = 0L; max = Int64.max_int; encode; decode; succ; pred;
      inject = id; extract = id; pp = Int64.to_string; parts = ();
    }

let positive_int64_complement =
  let encode b n =
    if Int64.compare n 0L < 0 then
      error "positive_int64_complement.encode"
        (Unsatisfied_constraint "negative Int64.t");
    Obs_bytea.add_int64_complement_be b n in

  let decode s ~off ~len scratch =
    check_off_len "positive_int64" s !off len;
    if len < 8 then invalid_off_len ~fname:"positive_int64_complement" s !off len;
    let r = decode_int64_complement_be s !off in
      off := !off + 8;
      r
  in
    { positive_int64 with encode; decode }

let tuple2 c1 c2 : (_, _, (_, _, _, _, _, _) cons) codec =
  {
    min = (c1.min, c2.min);
    max = (c1.max, c2.max);
    encode = (fun b (x, y) -> c1.encode b x; c2.encode b y);
    decode = (fun s ~off ~len scratch ->
                let off0 = !off in
                let x = c1.decode s ~off ~len scratch in
                let y = c2.decode s ~off ~len:(len - (!off - off0)) scratch in
                  (x, y));
    succ = (fun (x, y) ->
              let y' = c2.succ y in
                if y = y' then (c1.succ x, y') else (x, y'));
    pred = (fun (x, y) ->
              let y' = c2.pred y in
                if y = y' then (c1.pred x, y') else (x, y'));
    inject = (fun (x, y) -> (c1.inject x, c2.inject y));
    extract = (fun (x, y) -> (c1.extract x, c2.extract y));
    pp = (fun (x, y) -> sprintf "(%s, %s)" (c1.pp x) (c2.pp y));
    parts = (c1, c2);
  }

let ( *** ) = tuple2

let custom ~encode:inject ~decode:extract ~pp c =
  { c with inject = (fun x -> c.inject (inject x));
           extract = (fun x -> extract (c.extract x));
           pp; }

let tuple3 c1 c2 c3 =
  let c = c1 *** c2 *** c3 in
  let encode (x, y, z) = (x, (y, z)) in
  let decode (x, (y, z)) = (x, y, z) in
  let pp (x, y, z) = sprintf "(%s, %s, %s)" (c1.pp x) (c2.pp y) (c3.pp z) in
    custom ~encode ~decode ~pp c

let tuple4 c1 c2 c3 c4 =
  let c = c1 *** c2 *** c3 *** c4 in
  let encode (x, y, z, a) = (x, (y, (z, a))) in
  let decode (x, (y, (z, a))) = (x, y, z, a) in
  let pp (x, y, z, a) =
    sprintf "(%s, %s, %s, %s)" (c1.pp x) (c2.pp y) (c3.pp z) (c4.pp a)
  in
    custom ~encode ~decode ~pp c

let tuple5 c1 c2 c3 c4 c5 =
  let c = c1 *** c2 *** c3 *** c4 *** c5 in
  let encode (x, y, z, a, b) = (x, (y, (z, (a, b)))) in
  let decode (x, (y, (z, (a, b)))) = (x, y, z, a, b) in
  let pp (x, y, z, a, b) =
    sprintf "(%s, %s, %s, %s, %s)"
      (c1.pp x) (c2.pp y) (c3.pp z) (c4.pp a) (c5.pp b)
  in custom ~encode ~decode ~pp c

let decode_choice choices s ~off ~len scratch =
  let off0 = !off in
  let tag = byte.decode s ~off ~len scratch in
  let len = len - (!off - off0) in
    if tag < 0 || tag >= Array.length choices then
      error (sprintf "choice%d.decode" (Array.length choices)) (Unknown_tag tag);
    choices.(tag) s ~off ~len scratch

type ('t1, 't2) choice2 = [`A of 't1 | `B of 't2]
type ('t1, 't2, 't3) choice3 = [('t1, 't2) choice2 | `C of 't3]
type ('t1, 't2, 't3, 't4) choice4 = [('t1, 't2, 't3) choice3 | `D of 't4]
type ('t1, 't2, 't3, 't4, 't5) choice5 = [('t1, 't2, 't3, 't4) choice4 | `E of 't5]

let _encode2 c1 c2 b = function
    `A x -> byte.encode b 0; c1.encode b x
  | `B x -> byte.encode b 1; c2.encode b x

let _pp2 lbl1 c1 lbl2 c2 = function
    `A x -> sprintf "%s:%s" lbl1 (c1.pp x)
  | `B x -> sprintf "%s:%s" lbl2 (c2.pp x)

type inj_func = { injected : 'a 'b 'c. ('a, 'b, 'c) codec -> 'b -> 'b }

let _succ = { injected = succ }
let _pred = { injected = succ }

let _lift2 f c1 c2 = function
    `A x -> `A (f.injected c1 x)
  | `B x -> `B (f.injected c2 x)

let wrapA c s ~off ~len scratch = `A (c.decode s ~off ~len scratch)
let wrapB c s ~off ~len scratch = `B (c.decode s ~off ~len scratch)
let wrapC c s ~off ~len scratch = `C (c.decode s ~off ~len scratch)
let wrapD c s ~off ~len scratch = `D (c.decode s ~off ~len scratch)
let wrapE c s ~off ~len scratch = `E (c.decode s ~off ~len scratch)

(* TODO: allow choice2 composition of types with nontrivial inject/extract *)
let choice2 lbl1 c1 lbl2 c2 =
  { min = `A c1.min; max = `B c2.max; parts = ();
    encode = _encode2 c1 c2;
    decode = decode_choice [| wrapA c1; wrapB c2 |];
    (* in order to support nontrivial inject/extract, we'd need a new _lift2'
     * supporting funcs of type
     * 'a 'b 'c. ('a, 'b, 'c) codec -> 'a -> 'b
     * and
     * 'a 'b 'c. ('a, 'b, 'c) codec -> 'b -> 'a
     * which leads to lots of duplication = PITA.
     * We can (and most often will, to change the tags) apply
     *   [custom ~encode ~decode ~pp]
     * anyway.
     * *)
    (* inject = _lift2 _inject c1 c2; extract = _lift2 _extract c1 c2; *)
    inject = id; extract = id;
    pp = _pp2 lbl1 c1 lbl2 c2;
    succ = _lift2 _succ c1 c2; pred = _lift2 _pred c1 c2;
  }

let _encode3 c1 c2 c3 b = function
    #choice2 as x -> _encode2 c1 c2 b x
  | `C x -> byte.encode b 2; c3.encode b x

let _pp3 lbl1 c1 lbl2 c2 lbl3 c3 = function
  | #choice2 as x -> _pp2 lbl1 c1 lbl2 c2 x
  | `C x -> sprintf "%s:%s" lbl3 (c3.pp x)

let _lift3 f c1 c2 c3 = function
    `C x -> `C (f.injected c3 x)
  | #choice2 as x -> _lift2 f c1 c2 x

let choice3 lbl1 c1 lbl2 c2 lbl3 c3 =
  { min = `A c1.min; max = `C c3.max; parts = ();
    encode = _encode3 c1 c2 c3;
    decode = decode_choice [| wrapA c1; wrapB c2; wrapC c3; |];
    (* inject = _lift3 _inject c1 c2 c3; extract = _lift3 _extract c1 c2 c3; *)
    inject = id; extract = id;
    pp = _pp3 lbl1 c1 lbl2 c2 lbl3 c3;
    succ = _lift3 _succ c1 c2 c3; pred = _lift3 _pred c1 c2 c3;
  }

let _encode4 c1 c2 c3 c4 b = function
    #choice3 as x -> _encode3 c1 c2 c3 b x
  | `D x -> byte.encode b 3; c4.encode b x

let _pp4 lbl1 c1 lbl2 c2 lbl3 c3 lbl4 c4 = function
  | #choice3 as x -> _pp3 lbl1 c1 lbl2 c2 lbl3 c3 x
  | `D x -> sprintf "%s:%s" lbl4 (c4.pp x)

let _lift4 f c1 c2 c3 c4 = function
    `D x -> `D (f.injected c4 x)
  | #choice3 as x -> _lift3 f c1 c2 c3 x

let choice4 lbl1 c1 lbl2 c2 lbl3 c3 lbl4 c4 =
  { min = `A c1.min; max = `D c4.max; parts = ();
    encode = _encode4 c1 c2 c3 c4;
    decode = decode_choice [| wrapA c1; wrapB c2; wrapC c3; wrapD c4|];
    (* inject = _lift4 _inject c1 c2 c3 c4; extract = _lift4 _extract c1 c2 c3 c4; *)
    inject = id; extract = id;
    pp = _pp4 lbl1 c1 lbl2 c2 lbl3 c3 lbl4 c4;
    succ = _lift4 _succ c1 c2 c3 c4; pred = _lift4 _pred c1 c2 c3 c4;
  }

let _encode5 c1 c2 c3 c4 c5 b = function
    #choice4 as x -> _encode4 c1 c2 c3 c4 b x
  | `E x -> byte.encode b 4; c5.encode b x

let _pp5 lbl1 c1 lbl2 c2 lbl3 c3 lbl4 c4 lbl5 c5 = function
  | #choice4 as x -> _pp4 lbl1 c1 lbl2 c2 lbl3 c3 lbl4 c4 x
  | `E x -> sprintf "%s:%s" lbl5 (c5.pp x)

let _lift5 f c1 c2 c3 c4 c5 = function
    `E x -> `E (f.injected c5 x)
  | #choice4 as x -> _lift4 f c1 c2 c3 c4 x

let choice5 lbl1 c1 lbl2 c2 lbl3 c3 lbl4 c4 lbl5 c5 =
  { min = `A c1.min; max = `E c5.max; parts = ();
    encode = _encode5 c1 c2 c3 c4 c5;
    decode = decode_choice [| wrapA c1; wrapB c2; wrapC c3; wrapD c4; wrapE c5|];
    (* inject = _lift5 _inject c1 c2 c3 c4 c5; extract = _lift5 _extract c1 c2 c3 c4 c5; *)
    inject = id; extract = id;
    pp = _pp5 lbl1 c1 lbl2 c2 lbl3 c3 lbl4 c4 lbl5 c5;
    succ = _lift5 _succ c1 c2 c3 c4 c5; pred = _lift5 _pred c1 c2 c3 c4 c5;
  }
