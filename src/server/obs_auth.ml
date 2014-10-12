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

type perms    = [ `Full_access | `Replication ]
type username = string
and password  = string

type t =
    Accept_all
  | Password of (string, password * perms) Hashtbl.t

let rng =
  try Cryptokit.Random.device_rng "/dev/urandom"
  with Unix.Unix_error _ ->
    let r64 () = Random.int64 Int64.max_int in
      Random.self_init ();
      Cryptokit.Random.pseudo_rng
        (sprintf "%Ld-%Ld-%Ld-%Ld" (r64 ()) (r64 ()) (r64 ()) (r64 ()))

let make l =
  let h = Hashtbl.create (List.length l) in
    List.iter (fun (role, pwd, perms) -> Hashtbl.add h role (pwd, perms)) l;
    (Password h)

let accept_all = Accept_all

let to_hex s = Cryptokit.(transform_string (Hexa.encode ()) s)

let challenge t =
  let s = String.create 16 in
    rng#random_bytes s 0 16;
    to_hex s

let cmp_constant_time s1 s2 =
  let len = String.length s1 in
    if String.length s2 <> len then `Different
    else
      let rec loop acc = function
          off when off >= 0 ->
            let c1 = Char.code s1.[off] in
            let c2 = Char.code s2.[off] in
              loop (acc + abs (c1 - c2)) (off - 1)
        | _ -> acc
      in match loop 0 (len - 1) with
          0 -> `Equal
        | _ -> `Different

(* we leak (via timing) info about whether the role exists, but are careful
 * enough not to leak info about the password! *)
let check_response auth ~role ~challenge ~response =
  try
    let pwd, perms = Hashtbl.find auth role in
    let expected   = to_hex Cryptokit.(hash_string (MAC.hmac_sha1 pwd) challenge) in
      match cmp_constant_time expected response with
        | `Equal -> perms
        | `Different -> failwith (sprintf "Incorrect password")
  with Not_found -> failwith (sprintf "Unknown role %S" role)

let check_response auth ~role ~challenge ~response =
  match auth with
    | Accept_all -> `Full_access
    | Password t -> check_response t ~role ~challenge ~response
