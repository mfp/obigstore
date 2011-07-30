
open Printf
open Lwt
open Test_00util
open OUnit

module D = Data_model
module DD = D.Data
module DU = D.Update

let test_keyspace_management db =
  aeq_string_list [] (D.list_keyspaces db);
  let k1 = D.register_keyspace db "foo" in
  let k2 = D.register_keyspace db "bar" in
    aeq_string_list ["bar"; "foo";] (D.list_keyspaces db);
    aeq_some D.keyspace_name k1 (D.get_keyspace db "foo");
    aeq_some D.keyspace_name k2 (D.get_keyspace db "bar");
    return ()

let test_list_tables db =
  let ks1 = D.register_keyspace db "test_list_tables" in
  let ks2 = D.register_keyspace db "test_list_tables2" in
    aeq_string_list [] (D.list_tables ks1);
    lwt () =
      D.read_committed_transaction ks1
        (fun tx -> D.put_columns tx "tbl1" "somekey" []) in
    aeq_string_list [] (D.list_tables ks1);
    lwt () =
      D.read_committed_transaction ks1
        (fun tx ->
           D.put_columns tx "tbl1" "somekey"
             [ { DU.name = "somecol"; data = "";
                 timestamp = DU.No_timestamp; }; ] >>
           D.put_columns tx "tbl2" "someotherkey"
             [ { DU.name = "someothercol"; data = "xxx";
                 timestamp = DU.No_timestamp; }; ]) in
    aeq_string_list ["tbl1"; "tbl2"] (D.list_tables ks1);
    aeq_string_list [] (D.list_tables ks2);
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
    "list tables", test_list_tables;
  ]

let () =
  register_tests "Data_model" tests
