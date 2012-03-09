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

module S = Obs_server.Make(Obs_storage)(Obs_protocol_payload.Version_0_0_0)

let port = ref 12050
let db_dir = ref None
let debug = ref false
let gcommit_period = ref 0.010
let write_buffer_size = ref (4 * 1024 * 1024)
let block_size = ref 4096
let max_open_files = ref 1000

let params =
  Arg.align
    [
      "-port", Arg.Set_int port, "PORT Port to listen at (default: 12050)";
      "-debug", Arg.Set debug, " Dump debug info to stderr.";
      "-group-commit-period", Arg.Set_float gcommit_period,
        "DT Group commit period (default: 0.010s)";
      "-write-buffer-size", Arg.Set_int write_buffer_size, "N Write buffer size (default: 4MB)";
      "-block_size", Arg.Set_int block_size, "N Block size (default: 4KB)";
      "-max-open-files", Arg.Set_int max_open_files, "N Max open files (default: 1000)";
    ]

let usage_message = "Usage: obigstore [options] [database dir]"

let _ = Sys.set_signal Sys.sigpipe Sys.Signal_ignore
let _ = Sys.set_signal Sys.sigint (Sys.Signal_handle (fun _ -> exit 0))
let _ = Sys.set_signal Sys.sighup (Sys.Signal_handle (fun _ -> Gc.compact ()))

let () =
  Arg.parse
    params
    (function
       | s when !db_dir = None && s <> "" & s.[0] <> '-' -> db_dir := Some s
       | s -> eprintf "Unknown argument: %S\n%!" s;
              Arg.usage params usage_message;
              exit 1)
    usage_message;
  let address = Unix.ADDR_INET (Unix.inet_addr_any, !port) in
  let data_address = Unix.ADDR_INET (Unix.inet_addr_any, !port + 1) in
    match !db_dir with
        None -> Arg.usage params usage_message;
                exit 1
      | Some dir ->
          let db = Obs_storage.open_db
                     ~group_commit_period:!gcommit_period dir
                     ~write_buffer_size:!write_buffer_size
                     ~block_size:!block_size
                     ~max_open_files:!max_open_files
          in Lwt_unix.run (S.run_plain_server ~debug:!debug db
                             ~address ~data_address)
