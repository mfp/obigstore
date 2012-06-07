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

open Lwt
open Printf
open Obs_util

module D = Obs_protocol_client.Make(Obs_protocol_bin.Version_0_0_0)

let keyspace = ref ""
let server = ref "127.0.0.1"
let port = ref 12050
let file = ref "-"

let usage_message = "Usage: ob_load -keyspace NAME [options]"

let params =
  Arg.align
    [
      "-keyspace", Arg.Set_string keyspace, "NAME Dump tables in keyspace NAME.";
      "-server", Arg.Set_string server, "ADDR Connect to server at ADDR.";
      "-port", Arg.Set_int port, "N Connect to server port N (default: 12050)";
      "-i", Arg.Set_string file, "FILE Load from FILE (default: -)";
    ]

let load db ~keyspace ?size ich =
  let region = Lwt_util.make_region 10 in (* max parallel reqs *)
  lwt ks = D.register_keyspace db keyspace in
  let error = ref None in
    D.repeatable_read_transaction ks
      (fun tx ->
         let rec loop_load progress =
           match !error with
             | None ->
                 Progress_report.update progress (Lwt_io.position ich) >>
                 lwt len = Lwt_io.read_int ich in
                 let buf = String.create len in
                 lwt () = Lwt_io.read_into_exactly ich buf 0 len in
                   ignore begin
                     try_lwt
                       Lwt_util.run_in_region region 1
                         (fun () -> D.load tx buf >|= ignore)
                     with e ->
                       error := Some e;
                       return ()
                   end;
                   (* wait until one of the reqs is done *)
                   Lwt_util.run_in_region region 1 (fun () -> return ()) >>
                   loop_load progress
             | Some error -> raise_lwt error
         in try_lwt
              Progress_report.with_progress_report
                ?max:size Lwt_io.stderr loop_load
            with End_of_file -> return ())

let role = "guest"
let password = "guest"

let () =
  Printexc.record_backtrace true;
  Arg.parse params ignore usage_message;
  if !keyspace = "" then begin
    Arg.usage params usage_message;
    exit 1
  end;
  in Lwt_unix.run begin
    lwt input, size = match !file with
        "-" -> return (Lwt_io.stdin, None)
      | "" -> Arg.usage params usage_message; exit 1
      | f ->
          lwt ich = Lwt_io.open_file ~mode:Lwt_io.input f in
          lwt size = Lwt_io.file_length f in
            return (ich, Some size) in
    let addr = Unix.ADDR_INET (Unix.inet_addr_of_string !server, !port) in
    let data_address = Unix.ADDR_INET (Unix.inet_addr_of_string !server, !port + 1) in
    lwt ich, och = Lwt_io.open_connection addr in
    lwt db = D.make ~data_address ich och ~role ~password in
      load db ~keyspace:!keyspace ?size input
  end
