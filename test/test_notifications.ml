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

open OUnit
open Printf
open Lwt
open Test_00util

module CLIENT = Obs_protocol_client.Make(Obs_protocol_payload.Version_0_0_0)
module SERVER = Obs_protocol_server.Make(Obs_storage)(Obs_protocol_payload.Version_0_0_0)
module DM = Obs_data_model

let dummy_addr = Unix.ADDR_INET (Unix.inet_addr_of_string "127.0.0.1", 0)

let with_conn server f =
  let ch1_in, ch1_out = Lwt_io.pipe () in
  let ch2_in, ch2_out = Lwt_io.pipe () in
  let client = CLIENT.make ~data_address:dummy_addr ch2_in ch1_out in
    try_lwt
      ignore
        (try_lwt
           SERVER.service_client server ch1_in ch2_out
         with _ -> return ());
      f client
    finally
      CLIENT.close client;
      return ()

let expect ?msg ks l =
  CLIENT.await_notifications ks >|= aeq ?msg (string_of_list (sprintf "%S")) l

let make_server () =
  let dir = make_temp_dir () in
  let db = Obs_storage.open_db dir in
    SERVER.make db

let do_test_notifications ks1a ks1b ks2 =
  CLIENT.listen ks1a "foo" >>
  CLIENT.notify ks1b "foobar" >>
  CLIENT.notify ks1b "foo" >>
  expect ks1a ["foo"] >>
  CLIENT.unlisten ks1a "foo" >>
  CLIENT.listen ks1a "bar" >>
  CLIENT.listen ks1a "foobar" >>
  CLIENT.listen ks1a "babar" >>
  CLIENT.listen ks1b "bar" >>
  CLIENT.notify ks1b "bar" >>
  CLIENT.notify ks1b "babar" >>
  expect ~msg:"ks1a" ks1a ["bar"; "babar"] >>
  expect ~msg:"ks1b" ks1b ["bar"] >>
  CLIENT.notify ks2 "bar" >>
  CLIENT.notify ks1b "babar" >>
  expect ks1a ["babar"]

let do_test_notifications_in_tx ks ks' =
  CLIENT.listen ks "foo" >>
  CLIENT.listen ks "bar" >>
  CLIENT.listen ks "baz" >>
  CLIENT.notify ks' "bar" >>
  CLIENT.read_committed_transaction ks'
    (fun ks' ->
       CLIENT.notify ks' "baz" >>
       try_lwt
         CLIENT.read_committed_transaction ks'
           (fun ks' ->
              CLIENT.notify ks' "foo" >>
              (* abort the tx *)
              raise_lwt Exit)
       with Exit -> return ()) >>
  expect ks ["bar"; "baz"]

let test_notifications_same_conn () =
  let server = make_server () in
    with_conn server
      (fun c1 ->
         lwt ks1a = CLIENT.register_keyspace c1 "ks" in
         lwt ks1b = CLIENT.register_keyspace c1 "ks" in
         lwt ks2 = CLIENT.register_keyspace c1 "ks2" in
           do_test_notifications ks1a ks1b ks2)

let test_notifications_in_tx_same_conn () =
  let server = make_server () in
     with_conn server
       (fun c1 ->
          lwt ks = CLIENT.register_keyspace c1 "ks" in
          lwt ks' = CLIENT.register_keyspace c1 "ks" in
            do_test_notifications_in_tx ks ks')

let test_notifications_diff_conn () =
  let server = make_server () in
    with_conn server
      (fun c1 ->
         with_conn server
           (fun c2 ->
              lwt ks1a = CLIENT.register_keyspace c1 "ks" in
              lwt ks1b = CLIENT.register_keyspace c2 "ks" in
              lwt ks2 = CLIENT.register_keyspace c1 "ks2" in
                do_test_notifications ks1a ks1b ks2))

let test_notifications_in_tx_diff_conn () =
  let server = make_server () in
     with_conn server
       (fun c1 ->
          with_conn server
            (fun c2 ->
               lwt ks = CLIENT.register_keyspace c1 "ks" in
               lwt ks' = CLIENT.register_keyspace c2 "ks" in
                 do_test_notifications_in_tx ks ks'))

let column_without_timestamp (name, data) =
  { DM.name; data; timestamp = DM.No_timestamp }

let put ks tbl key l =
  CLIENT.put_columns ks tbl key (List.map column_without_timestamp l)

let table_tbl = DM.table_of_string "tbl"

let test_notification_follows_commit () =
  let server = make_server () in
     with_conn server
       (fun c1 ->
          with_conn server
            (fun c2 ->
               lwt ks = CLIENT.register_keyspace c1 "ks" in
               lwt ks' = CLIENT.register_keyspace c2 "ks" in
               CLIENT.listen ks "foo" >>
               let t, u = Lwt.wait () in
               ignore begin
                 CLIENT.await_notifications ks >>
                 match_lwt CLIENT.get_column ks table_tbl "a" "k" with
                     Some ("v", _) -> Lwt.wakeup u `OK; return ()
                   | Some _ -> Lwt.wakeup u `Wrong_data; return ()
                   | None -> Lwt.wakeup u `No_data; return ()
               end;
               CLIENT.read_committed_transaction ks'
                 (fun ks' ->
                    CLIENT.notify ks' "foo" >>
                    put ks' table_tbl "a" ["k", "v"]) >>
               match_lwt t with
                   `OK -> return ()
                 | `No_data ->
                     assert_failure "Data was not committed before notification"
                 | `Wrong_data ->
                     assert_failure "Wrong data committed before notification"))

let test_await_before_subscription () =
  let server = make_server () in
    with_conn server
      (fun c1 ->
         lwt ks = CLIENT.register_keyspace c1 "ks" in
         lwt ks' = CLIENT.register_keyspace c1 "ks" in
         let ok = expect ~msg:"foo subscribed to after await" ks ["foo"] in
           CLIENT.listen ks "foo" >>
           CLIENT.listen ks "bar" >>
           CLIENT.notify ks' "foo" >>
           ok)

let tests =
  List.map
    (fun (k, f) ->
       k >:: (fun () -> Lwt_unix.run (Lwt_unix.with_timeout 0.1 f)))
    [
      "simple notifications, same conn", test_notifications_same_conn;
      "notify in transaction, same conn", test_notifications_in_tx_same_conn;
      "simple notifications, diff conn", test_notifications_diff_conn;
      "notify in transaction, diff conn", test_notifications_in_tx_diff_conn;
      "notification follows data commit", test_notification_follows_commit;
      "await before subscription", test_await_before_subscription;
    ]

let () =
  register_tests ("Notifications") tests
