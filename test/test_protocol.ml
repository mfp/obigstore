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
open Test_00util

module CLIENT = Obs_protocol_client.Make(Obs_protocol_payload.Version_0_0_0)

let dummy_addr = Unix.ADDR_INET (Unix.inet_addr_of_string "127.0.0.1", 0)

module C =
struct
  let id = "Obs_protocol_client atop Obs_storage server"

  module SERVER = Obs_protocol_server.Make(Obs_storage)(Obs_protocol_payload.Version_0_0_0)

  let with_db f =
    let dir = make_temp_dir () in
    let db = Obs_storage.open_db dir in
    let ch1_in, ch1_out = Lwt_io.pipe () in
    let ch2_in, ch2_out = Lwt_io.pipe () in
    let server = SERVER.make db in
    let client = CLIENT.make ~data_address:dummy_addr ch2_in ch1_out in
      Lwt_unix.run begin
        try_lwt
          ignore
            (try_lwt
               SERVER.service_client server ch1_in ch2_out
             with _ -> return ());
          f client
        finally
          CLIENT.close client;
          Obs_storage.close_db db
      end

  let with_db_pool f =
    let dir = make_temp_dir () in
    let db = Obs_storage.open_db dir in
    let server = SERVER.make db in
    let clients = ref [] in
    let mk_client () =
      let ch1_in, ch1_out = Lwt_io.pipe () in
      let ch2_in, ch2_out = Lwt_io.pipe () in
      let client = CLIENT.make ~data_address:dummy_addr ch2_in ch1_out in
        clients := client :: !clients;
        ignore
          (try_lwt
             SERVER.service_client server ch1_in ch2_out
           with _ -> return ());
        return  client in
    let pool = Lwt_pool.create 100 mk_client in
      Lwt_unix.run begin
        try_lwt
          f pool
        finally
          List.iter CLIENT.close !clients;
          Obs_storage.close_db db
      end
end

module TEST = Test_data_model.Run_test(CLIENT)(C)
