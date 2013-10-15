(*
 * Copyright (C) 2011 Mauricio Fernandez <mfp@acm.org>
 *               2009 Jérémie Dimino
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

(* taken from lwt_io.ml *)
let open_connection ?buffer_size sockaddr =
  let fd = Lwt_unix.socket (Unix.domain_of_sockaddr sockaddr) Unix.SOCK_STREAM 0 in
  let close = lazy begin
    try_lwt
      Lwt_unix.shutdown fd Unix.SHUTDOWN_ALL;
      return_unit
    with Unix.Unix_error(Unix.ENOTCONN, _, _) ->
      (* This may happen if the server closed the connection before us *)
      return_unit
    finally
      Lwt_unix.close fd
  end in
  try_lwt
    lwt () = Lwt_unix.connect fd sockaddr in
    (try Lwt_unix.set_close_on_exec fd with Invalid_argument _ -> ());
    return (fd,
            Lwt_io.make ?buffer_size
              ~close:(fun _ -> Lazy.force close)
              ~mode:Lwt_io.input (Lwt_bytes.read fd),
            Lwt_io.make ?buffer_size
              ~close:(fun _ -> Lazy.force close)
              ~mode:Lwt_io.output (Lwt_bytes.write fd))
  with exn ->
    lwt () = Lwt_unix.close fd in
    raise_lwt exn
