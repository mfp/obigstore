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

module String = struct include String include BatString end
module S = Obs_server.Make(Obs_storage)(Obs_protocol_payload.Version_0_0_0)

let port = ref 12050
let db_dir = ref None
let debug = ref false
let gcommit_period = ref 0.010
let write_buffer_size = ref (4 * 1024 * 1024)
let block_size = ref 4096
let max_open_files = ref 1000
let master = ref None

let params =
  Arg.align
    [
      "-port", Arg.Set_int port, "PORT Port to listen at (default: 12050)";
      "-master", Arg.String (fun s -> master := Some s),
        "HOST:PORT Replicate database reachable on HOST:PORT.";
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

let open_db dir =
  Obs_storage.open_db
    ~group_commit_period:!gcommit_period dir
    ~write_buffer_size:!write_buffer_size
    ~block_size:!block_size
    ~max_open_files:!max_open_files

let run_slave ~dir ~address ~data_address host port =
  let module C =
    Obs_protocol_client.Make(Obs_protocol_payload.Version_0_0_0) in
  let module DUMP =
    Obs_dump.Make(struct include C include C.Raw_dump end) in
  let master_addr = Unix.ADDR_INET (host, port) in
  let master_data_address = Unix.ADDR_INET (host, port + 1) in
    Lwt_unix.run begin
      lwt ich, och = Lwt_io.open_connection master_addr in
      let db = C.make ~data_address:master_data_address ich och in
      lwt raw_dump = C.Raw_dump.dump db in
        DUMP.dump_local ~verbose:true ~destdir:dir raw_dump >>
        let db = open_db dir in
          ignore begin try_lwt
            if !debug then eprintf "Getting replication stream\n%!";
            lwt stream = C.Replication.get_update_stream raw_dump in
            if !debug then eprintf "Got replication stream\n%!";
            let rec get_updates () =
              match_lwt C.Replication.get_update stream with
                  None ->
                    return ()
                | Some update ->
                    lwt s, off, len = C.Replication.get_update_data update in
                    let () =
                      if !debug then eprintf "Got update (%d bytes).\n%!" len in
                    let update' =
                      Obs_storage.Replication.update_of_string s off len
                    in
                      match update' with
                        None ->
                          (* FIXME: signal dropped update to master *)
                          get_updates ()
                      | Some update' ->
                          Obs_storage.Replication.apply_update db update' >>
                          C.Replication.ack_update update >>
                          get_updates ()
            in get_updates ()
          with exn ->
            (* FIXME: better logging *)
            let bt = Printexc.get_backtrace () in
              eprintf "Exception in replication thread: %s\n%s\n%!"
                (Printexc.to_string exn) bt;
              return ()
          end;
          S.run_plain_server ~debug:!debug db
            ~address ~data_address
    end

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
          match !master with
              None ->
                let db = open_db dir in
                  Lwt_unix.run (S.run_plain_server ~debug:!debug db
                                  ~address ~data_address)
            | Some master ->
                let host, port =
                  begin try
                    let h, p = String.split master ":" in
                      h, int_of_string p
                  with Not_found | Failure _ ->
                    eprintf "-master needs argument of the form HOST:PORT \
                             (e.g.: 127.0.0.1:15000)\n%!";
                    exit 1
                  end in
                let host =
                  try
                    (Unix.gethostbyname host).Unix.h_addr_list.(0)
                  with Not_found ->
                    eprintf "Couldn't find master %S\n%!" host;
                    exit 1
                in run_slave ~dir ~address ~data_address host port
