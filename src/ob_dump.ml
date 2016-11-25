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

module D      = Obs_protocol_client.Make(Obs_protocol_bin.Version_0_0_0)
module Option = BatOption
module Set    = BatSet
module List   = BatList

module S = Set.Make(String)

let keyspace        = ref ""
let tables          = ref []
let server          = ref "127.0.0.1"
let port            = ref 12050
let output          = ref "-"
let raw_dump_dstdir = ref None
let verbose         = ref false
let serverside      = ref true
let role            = ref (try Sys.getenv "OBIGSTORE_ROLE" with _ -> "guest")
let password        = ref (try Sys.getenv "OBIGSTORE_PASSWORD" with _ -> "guest")

let usage_message = "Usage: ob_dump [-serverside | -keyspace NAME | -full DIR] [options]"

let params =
  Arg.align
    [
      "-keyspace", Arg.Set_string keyspace, "NAME Dump tables in keyspace NAME.";
      "-table", Arg.String (fun s -> tables := s :: !tables),
        "NAME Dump table NAME (default: all)";
      "-full", Arg.String (fun s -> raw_dump_dstdir := Some s),
        "DIRNAME Perform raw dump of whole database to given directory.";
      "-serverside", Arg.Set serverside,
        " Trigger server-side dump and return directory.";
      "-o", Arg.Set_string output, "FILE Dump to file FILE (default: '-')";
      "-server", Arg.Set_string server, "ADDR Connect to server at ADDR.";
      "-port", Arg.Set_int port, "N Connect to server port N (default: 12050)";
      "-role", Arg.Set_string role, "ROLE Authentify as ROLE";
      "-password", Arg.Set_string password, "PASS Authentify using password PASS";
      "-v", Arg.Set verbose, " Verbose mode.";
    ]

let dump db ~keyspace ~only_tables och =
  match_lwt D.get_keyspace db keyspace with
      None -> return_unit
    | Some ks ->
        lwt all_tables = match only_tables with
            Some l -> return l
          | None -> D.list_tables ks in
        lwt approx_size =
          Lwt_list.fold_left_s
            (fun s table -> D.table_size_on_disk ks table >|= Int64.add s)
            0L all_tables
        in
          D.repeatable_read_transaction ks
            (fun tx ->
               let rec loop_dump offset progress =
                 Progress_report.update progress (Lwt_io.position och) >>
                 match_lwt D.dump tx ?only_tables ?offset () with
                     None -> return_unit
                   | Some (data, cursor) ->
                       Lwt_io.LE.write_int och (String.length data) >>
                       Lwt_io.write och data >>
                       match cursor with
                         | Some _ -> loop_dump cursor progress
                         | None -> return_unit
               in Progress_report.with_progress_report ~max:approx_size
                    Lwt_io.stderr (loop_dump None))

let () =
  Printexc.record_backtrace true;
  Arg.parse params ignore usage_message;
  begin match !serverside, !keyspace, !raw_dump_dstdir with
    | false, "", None -> Arg.usage params usage_message; exit 1
    | _ -> ()
  end;
  let only_tables = match List.rev_map Obs_data_model.table_of_string !tables with
      [] -> None
    | l -> Some l
  in Lwt_unix.run begin
    let addr = Unix.ADDR_INET (Unix.inet_addr_of_string !server, !port) in
    let data_address = Unix.ADDR_INET (Unix.inet_addr_of_string !server, !port + 1) in
    lwt ich, och = Lwt_io.open_connection addr in
    lwt db = D.make ~data_address ich och ~role:!role ~password:!password in
      match !serverside, !raw_dump_dstdir with
        | true, _ ->
            lwt raw_dump = D.Raw_dump.dump ~mode:`No_stream db in
            lwt localdir = D.Raw_dump.localdir raw_dump in
            lwt ()       = D.Raw_dump.release raw_dump ~keep_files:true in
              print_endline localdir;
              return_unit
        | _, None ->
            lwt output = match !output with
                "-" -> return Lwt_io.stdout
              | f -> Lwt_io.open_file
                       ~flags:[ Unix.O_CREAT; Unix.O_WRONLY; Unix.O_TRUNC; ]
                       ~perm:0o600 ~mode:Lwt_io.output f
            in
              try_lwt
                dump db ~keyspace:!keyspace ?only_tables output
              finally
                Lwt_io.close output
        | _, Some destdir ->
            let module DUMP =
              Obs_dump.Make(struct include D include D.Raw_dump end) in
            lwt raw_dump = D.Raw_dump.dump ~mode:`No_stream db in
            lwt st       = DUMP.dump_local ~verbose:!verbose ~destdir raw_dump in
            lwt ()       = D.Raw_dump.release raw_dump ~keep_files:false in
            let added    = List.enum st.Obs_dump.added_files |> S.of_enum in
            let all      = List.filter
                             (function
                                | "CURRENT" | "LOCK" | "LOG" | "LOG.old" -> false
                                | _ -> true)
                             st.Obs_dump.all_files |>
                           List.sort String.compare
            in
              List.iter
                (fun fname ->
                   if S.mem fname added then printf "+%s\n" fname
                   else printf "%s\n" fname)
                all;
              flush stdout;
              return_unit
  end
