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

module S = Server.Make(Storage)(Protocol_payload.Version_0_0_0)

let port = ref 12050
let db_dir = ref None
let debug = ref false

let params =
  Arg.align
    [
      "-port", Arg.Set_int port, "PORT Port to listen at (default: 12050)";
      "-debug", Arg.Set debug, " Dump debug info to stderr.";
    ]

let usage_message = "Usage: obigstore [options] [database dir]"

let _ = Sys.set_signal Sys.sigpipe Sys.Signal_ignore
let _ = Sys.set_signal Sys.sigint (Sys.Signal_handle (fun _ -> exit 0))

let () =
  Arg.parse
    params
    (function
       | s when !db_dir = None && s <> "" & s.[0] <> '-' -> db_dir := Some s
       | s -> eprintf "Unknown argument: %S\n%!" s;
              Arg.usage params usage_message;
              exit 1)
    usage_message;
  let addr = Unix.ADDR_INET (Unix.inet_addr_any, !port) in
    match !db_dir with
        None -> Arg.usage params usage_message;
                exit 1
      | Some dir ->
          let db = Storage.open_db dir in
            Lwt_unix.run (S.run_server ~debug:!debug db addr !port)
