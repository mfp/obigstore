
open Printf
open Lwt
open Test_00util
open OUnit

module D = Data_model

let test_keyspace_management db =
  aeq_list (sprintf "%S") [] (D.list_keyspaces db);
  let k1 = D.register_keyspace db "foo" in
  let k2 = D.register_keyspace db "bar" in
    aeq_list (sprintf "%S") ["bar"; "foo";] (D.list_keyspaces db);
    aeq_some D.keyspace_name k1 (D.get_keyspace db "foo");
    aeq_some D.keyspace_name k2 (D.get_keyspace db "bar");
    return ()

let with_db f () =
  let dir = make_temp_dir () in
  let db = D.open_db dir in
    try
      Lwt_unix.run (f db)
    with e -> D.close_db db; raise e

let test_with_db f = with_db f

let tests =
  List.map (fun (n, f) -> n >:: test_with_db f)
  [
    "keyspace management", test_keyspace_management;
  ]

let () =
  register_tests "Data_model" tests
