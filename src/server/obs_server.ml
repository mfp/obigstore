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

let section = Lwt_log.Section.make "obigstore:server:connection"

let write_line och s = Lwt_io.write och s >> Lwt_io.write och "\r\n"

let perform_auth auth ich och =
  lwt role = Lwt_io.read_line ich in
  let challenge = Obs_auth.challenge auth in
    write_line och challenge >>
    lwt response = Lwt_io.read_line ich in
      try_lwt
        let perms = Obs_auth.check_response auth ~role ~challenge ~response in
          write_line och "+OK" >>
          return perms
      with e ->
        write_line och "-ERR" >>
        raise_lwt e

let is_compat (a1, a2, a3) (b1, b2, b3) = (a1 = b1 && b2 >= a2)

let find_compat_proto v l =
    try
      Some (List.find (fun (v', _) -> is_compat v v') l)
    with Not_found -> None

let find_protocol (bin, text) = function
    `Binary v -> find_compat_proto v bin
  | `Textual v -> find_compat_proto v text

let connection_handshake server auth ((bin, text) as protos) ich och =
  let read_version () =
    lwt s = Lwt_io.read_line ich in
      return (Scanf.sscanf s "%d.%d.%d" (fun a b c -> (a, b, c))) in
  let find_max_version = List.fold_left (fun v (w, _) -> max v w) (-1, 0, 0) in
  let to_s (m, n, o) = sprintf "%d.%d.%d" m n o in
  lwt perms          = perform_auth auth ich och in
    write_line och (to_s (find_max_version bin)) >>
    write_line och (to_s (find_max_version text)) >>
    lwt requested_proto =
      match_lwt Lwt_io.read_line ich with
          "TXT" -> lwt v = read_version () in return (`Textual v)
        | "BIN" -> lwt v = read_version () in return (`Binary v)
        | s -> raise_lwt (Failure (sprintf "Unknown protocol %s" s))
    in
      match find_protocol protos requested_proto with
          Some ((a, b, c), proto) ->
            write_line och (sprintf "%d.%d.%d" a b c) >>
            return (proto, perms)
        | None ->
            write_line och "-ERR" >>
            raise_lwt (Failure "Cannot find matching protocol")

module Make
  (D : sig
     include Obs_data_model.S
     include Obs_data_model.BACKUP_SUPPORT with type backup_cursor := backup_cursor

     module Replication : Obs_replication.REPLICATION_SERVER
       with type db := db and type raw_dump := Raw_dump.raw_dump

     val use_thread_pool : db -> bool -> unit

     val throttling : db -> float
   end) =
struct
  module S = Obs_protocol_server.Make(D)

  type conn =
      {
        addr : Lwt_unix.sockaddr;
        ich : Lwt_io.input_channel;
        och : Lwt_io.output_channel;
      }

  let string_of_addr = function
      Unix.ADDR_UNIX s -> sprintf "unix socket %S" s
    | Unix.ADDR_INET (a, p) -> sprintf "%s:%d" (Unix.string_of_inet_addr a) p

  let rec accept_loop handle_connection ~server auth protos sock =
    begin try_lwt
      lwt (fd, addr) = Lwt_unix.accept sock in
      lwt () = Lwt_log.info_f ~section
                 "Got connection from %s" (string_of_addr addr)
      in
        Lwt_unix.setsockopt fd Unix.TCP_NODELAY true;
        Lwt_unix.setsockopt fd Unix.SO_KEEPALIVE true;
        ignore begin try_lwt
          let ich = Lwt_io.of_fd Lwt_io.input fd in
          let och = Lwt_io.of_fd Lwt_io.output fd in
            try_lwt
              handle_connection server auth protos { ich; och; addr }
            finally
              (try_lwt Lwt_io.flush och with _ -> return ()) >>
              (try Lwt_io.abort och >> Lwt_io.abort ich with _ -> return_unit)
        with
          | End_of_file
          | Unix.Unix_error ((Unix.ECONNRESET | Unix.EPIPE), _, _) ->
              Lwt_log.info_f ~section "Closing connection from %s" (string_of_addr addr)
          | exn -> Lwt_log.error_f ~section ~exn "Error with connection"
        end;
        return ()
    with exn ->
      Lwt_log.error_f ~section ~exn "Got toplevel exception" >>
      Lwt_unix.sleep 0.05
    end >>
    accept_loop handle_connection ~server auth protos sock

  let make_sock ?(reuseaddr=true) ?(backlog=1024) address =
    let sock = Lwt_unix.socket (Unix.domain_of_sockaddr address) Unix.SOCK_STREAM 0 in
      if reuseaddr then Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
      Lwt_unix.bind sock address;
      Lwt_unix.listen sock backlog;
      sock

  let rec run_server
        ?(replication_wait = Obs_protocol_server.Await_commit)
        ?(max_async_reqs = 5000)
        db ~address ~data_address auth protos =
    let server = S.make ~max_async_reqs ~replication_wait db in
      Lwt.join
        [ accept_loop handle_connection ~server auth protos (make_sock address);
          accept_loop handle_data_connection ~server auth protos (make_sock data_address);
        ]

  and handle_connection server auth protos conn =
    lwt proto, perms = connection_handshake server auth protos conn.ich conn.och in
      S.service_client server proto conn.ich conn.och perms

  and handle_data_connection server auth protos conn =
    lwt perms = perform_auth auth conn.ich conn.och in
      S.service_data_client server conn.ich conn.och perms
end
