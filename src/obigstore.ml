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
open Lwt

module String = BatString
module S = Obs_server.Make(Obs_storage)

let host = ref "0.0.0.0"
let port = ref "12050"
let db_dir = ref None
let debug = ref false
let write_buffer_size = ref (4 * 1024 * 1024)
let block_size = ref 4096
let max_open_files = ref 1000
let master = ref None
let assume_page_fault = ref false
let unsafe_mode = ref false
let replication_wait = ref Obs_protocol_server.Await_commit
let max_concurrency = ref 5000
let engine = ref "default"

let set_await_recv () =
  replication_wait := Obs_protocol_server.Await_commit

let params =
  Arg.align
    [
      "-host", Arg.Set_string host, "HOST IP or hostname to listen to (default: 0.0.0.0)";
      "-port", Arg.Set_string port, "PORT Port or service name to listen to (default: 12050)";
      "-master", Arg.String (fun s -> master := Some s),
        "HOST:PORT Replicate database reachable on HOST:PORT.";
      "-debug", Arg.Set debug, " Dump debug info to stderr.";
      "-write-buffer-size", Arg.Set_int write_buffer_size, "N Write buffer size (default: 4MB)";
      "-block-size", Arg.Set_int block_size, "N Block size (default: 4KB)";
      "-max-open-files", Arg.Set_int max_open_files, "N Max open files (default: 1000)";
      "-assume-page-fault", Arg.Set assume_page_fault,
        " Assume working set doesn't fit in RAM and avoid blocking.";
      "-no-fsync", Arg.Set unsafe_mode,
        " Don't fsync after writes (may loss data on system crash).";
      "-await-recv", Arg.Unit set_await_recv,
        " Await mere reception (not commit) from replicas.";
      "-max-concurrency", Arg.Set_int max_concurrency,
        "N Allow at most N simultaneous requests (default: 5000)";
      "-engine", Arg.Set_string engine, "select|ev Use specified event loop engine."
    ]

let usage_message = "Usage: obigstore [options] [database dir]"

let _ = Sys.set_signal Sys.sigpipe Sys.Signal_ignore
let _ = Sys.set_signal Sys.sigint (Sys.Signal_handle (fun _ -> exit 0))
let _ = Sys.set_signal Sys.sighup (Sys.Signal_handle (fun _ -> Gc.compact ()))

let open_db dir =
  Obs_storage.open_db
    ~write_buffer_size:!write_buffer_size
    ~block_size:!block_size
    ~max_open_files:!max_open_files
    ~assume_page_fault:!assume_page_fault
    ~unsafe_mode:!unsafe_mode
    dir

let get_synced_db (ich,och) ~dir ~address ~data_address ~master_data_addr
    auth protos ~role ~password =
  let module C =
    Obs_protocol_client.Make(Obs_protocol_bin.Version_0_0_0) in
  let module DUMP =
    Obs_dump.Make(struct include C include C.Raw_dump end) in
  lwt db = C.make ~data_address:master_data_addr ich och ~role ~password in
  lwt raw_dump = C.Raw_dump.dump db in
  DUMP.dump_local ~verbose:true ~destdir:dir raw_dump >>
  let db = open_db dir in
  if !debug then eprintf "Getting replication stream\n%!";
  lwt stream = C.Replication.get_update_stream raw_dump in
  let lwt_stream = C.Replication.get_updates stream in
  if !debug then eprintf "Got replication stream\n%!";
  let iter_f update =
    try_lwt
      lwt s, off, len = C.Replication.get_update_data update in
      let () =
        if !debug then eprintf "Got update (%d bytes).\n%!" len in
      let update' =
        Obs_storage.Replication.update_of_string s off len
      in
      match update' with
      | None ->
        (* FIXME: signal dropped update to master *)
        return_unit
      | Some update' ->
        Obs_storage.Replication.apply_update db update' >>
        C.Replication.ack_update update
    with exn ->
      (* FIXME: better logging *)
      let bt = Printexc.get_backtrace () in
      eprintf "Exception in replication thread: %s\n%s\n%!"
        (Printexc.to_string exn) bt;
      raise exn

  in async (fun () -> Lwt_stream.iter_s iter_f lwt_stream);
  return db

let bin_protos =
  [
    (0, 0, 0), (module Obs_protocol_bin.Version_0_0_0 : Obs_protocol.SERVER_FUNCTIONALITY);
  ]

let text_protos =
  [
    (0, 0, 0), (module Obs_protocol_textual.Version_0_0_0 : Obs_protocol.SERVER_FUNCTIONALITY);
  ]

let protos = (List.(rev (sort compare bin_protos)), List.(rev (sort compare text_protos)))

let () =
  Arg.parse
    params
    (function
       | s when !db_dir = None && s <> "" && s.[0] <> '-' -> db_dir := Some s
       | s -> eprintf "Unknown argument: %S\n%!" s;
              Arg.usage params usage_message;
              exit 1)
    usage_message;
  Lwt_log.default := Lwt_log.channel
                       ~template:"$(date).$(milliseconds) [$(pid)] $(message)"
                       ~close_mode:`Keep ~channel:Lwt_io.stderr ();
  let get_address_pairs host port =
    let open Unix in
    let ais = getaddrinfo host port [] in
    try
      List.map
        (fun ai -> match ai.ai_addr with
           | ADDR_INET (h, p) -> ADDR_INET (h, p), ADDR_INET (h, p + 1)
           | _ -> raise Not_found
        )
        ais
    with Not_found -> []
  in
  let address_pairs = get_address_pairs !host !port in
  if address_pairs = [] then
     raise (Invalid_argument
                     (Printf.sprintf "Impossible to obtain a socket address from %s/%s" !host !port));
  let address, data_address = List.hd address_pairs in
  let auth = Obs_auth.accept_all in
  match !db_dir with
  | None -> Arg.usage params usage_message;
    exit 1
  | Some dir ->
    begin match !engine with
        "default" -> ()
      | "ev" -> Lwt_engine.set (new Lwt_engine.libev)
      | "select" -> Lwt_engine.set (new Lwt_engine.select)
      | _ -> Arg.usage params usage_message; exit 1
    end;
    Lwt_main.run begin
      lwt db =
        match !master with
        | None ->
          Lwt.return (open_db dir)
        | Some master ->
          let host, port =
            begin try
                String.rsplit master ":"
              with Not_found | Failure _ ->
                eprintf "-master needs argument of the form HOST:PORT \
                         (e.g.: 127.0.0.1:15000)\n%!";
                exit 1
            end in
          let remote_pairs = get_address_pairs host port in
          let rec try_connect pairs = match pairs with
            | [] -> raise_lwt (Failure "No master available")
            | (a, da)::pairs ->
              try_lwt Lwt_io.open_connection a >|= fun (ic, oc) -> ic,oc,da with
              | Unix.Unix_error _ as exn ->
                if pairs = [] then raise_lwt exn
                else try_connect pairs
          in
          lwt mic, moc, master_data_addr = try_connect remote_pairs in
          get_synced_db (mic, moc) ~dir ~address ~data_address ~master_data_addr
            auth protos ~role:"guest" ~password:"guest"
      in
      S.run_server db
        ~max_async_reqs:!max_concurrency
        ~replication_wait:!replication_wait
        ~address ~data_address auth protos
    end
