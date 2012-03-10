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

open Lwt
open Printf

module Make
  (D : sig
     include Obs_data_model.S
     include Obs_data_model.BACKUP_SUPPORT with type backup_cursor := backup_cursor
     val use_thread_pool : db -> bool -> unit
   end)
  (P : Obs_protocol.PAYLOAD) =
struct
  module S = Obs_protocol_server.Make(D)(Obs_protocol_payload.Version_0_0_0)

  type conn =
      {
        addr : Lwt_unix.sockaddr;
        ich : Lwt_io.input_channel;
        och : Lwt_io.output_channel;
      }

  let string_of_addr = function
      Unix.ADDR_UNIX s -> sprintf "unix socket %S" s
    | Unix.ADDR_INET (a, p) -> sprintf "%s:%d" (Unix.string_of_inet_addr a) p

  let rec accept_loop handle_connection ~debug ~server sock =
    begin try_lwt
      lwt (fd, addr) = Lwt_unix.accept sock in
        if debug then
          eprintf "Got connection from %s\n%!" (string_of_addr addr);
        Lwt_unix.setsockopt fd Unix.TCP_NODELAY true;
        ignore begin try_lwt
          let ich = Lwt_io.of_fd Lwt_io.input fd in
          let och = Lwt_io.of_fd Lwt_io.output fd in
            try_lwt
              handle_connection ~debug server { ich; och; addr }
            finally
              Lwt_io.abort och
        with
          | End_of_file ->
              eprintf "Closing connection from %s\n%!" (string_of_addr addr);
              return ()
          | e ->
              let bt = match Printexc.get_backtrace () with
                  "" -> "<no backtrace>"
                | s -> s
              in
                eprintf "Error with connection: %s\n%s\n%!" (Printexc.to_string e) bt;
                return ()
        end;
        return ()
    with e ->
      eprintf "Got toplevel exception: %s\n%!" (Printexc.to_string e);
      Printexc.print_backtrace stderr;
      Lwt_unix.sleep 0.05
    end >>
    accept_loop handle_connection ~debug ~server sock

  let make_sock ?(reuseaddr=true) ?(backlog=1024) address =
    let sock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
      if reuseaddr then Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
      Lwt_unix.bind sock address;
      Lwt_unix.listen sock backlog;
      sock

  let rec run_plain_server ?(debug=false) db ~address ~data_address =
    let server = S.make db in
      Lwt.join
        [ accept_loop handle_connection ~debug ~server (make_sock address);
          accept_loop handle_data_connection ~debug ~server (make_sock data_address);
        ]

  and handle_connection ~debug server conn =
    S.service_client ~debug server conn.ich conn.och

  and handle_data_connection ~debug server conn =
    S.service_data_client ~debug server conn.ich conn.och
end
