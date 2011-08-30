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
open Test_00util

module CLIENT = Protocol_client.Make(Protocol_payload.Version_0_0_0)

module C =
struct
  let id = "Protocol_client atop Storage server"

  module SERVER = Protocol_server.Make(Storage)(Protocol_payload.Version_0_0_0)

  let with_db f =
    let dir = make_temp_dir () in
    let db = Storage.open_db dir in
    let ch1_in, ch1_out = Lwt_io.pipe () in
    let ch2_in, ch2_out = Lwt_io.pipe () in
    let srv_client_handle = SERVER.init db ch1_in ch2_out in
    let client = CLIENT.make ch2_in ch1_out in
      Lwt_unix.run begin
        try_lwt
          ignore (try_lwt SERVER.service srv_client_handle with _ -> return ());
          f client
        finally
          CLIENT.close client;
          Storage.close_db db
      end
end

module TEST = Test_data_model.Run_test(CLIENT)(C)
