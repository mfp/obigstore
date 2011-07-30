
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

let column_without_timestamp (name, data) =
  { DU.name; data; timestamp = DU.No_timestamp }

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

let test_get_keys_in_range_ranges db =
  let ks1 = D.register_keyspace db "test_get_keys_in_range_ranges" in
  let ks2 = D.register_keyspace db "test_get_keys_in_range_ranges2" in

  let put tx tbl key l =
    D.put_columns tx tbl key
      (List.map column_without_timestamp l) in

  let get_keys ks ?first ?up_to tbl =
    D.read_committed_transaction ks
      (fun tx ->
         D.get_keys_in_range tx tbl (DD.Key_range { DD.first; up_to })) in

    get_keys ks1 "tbl1" >|= aeq_string_list [] >>
    get_keys ks2 "tbl1" >|= aeq_string_list [] >>

    D.read_committed_transaction ks1
      (fun tx ->
         Lwt_list.iter_s
           (fun k -> put tx "tbl1" k ["name", k])
           ["a"; "ab"; "b"; "cde"; "d"; "e"; "f"; "g"]) >>

    get_keys ks2 "tbl1" >|= aeq_string_list [] >>
    get_keys ks1 "tbl1" >|=
      aeq_string_list ["a"; "ab"; "b"; "cde"; "d"; "e"; "f"; "g"] >>
    get_keys ks1 "tbl1" ~up_to:"c" >|=
      aeq_string_list ["a"; "ab"; "b"; ] >>
    get_keys ks1 "tbl1" ~first:"c" ~up_to:"c" >|=
      aeq_string_list [ ] >>
    get_keys ks1 "tbl1" ~first:"c" ~up_to:"f"  >|=
      aeq_string_list [ "cde"; "d"; "e"; "f" ] >>
    get_keys ks1 "tbl1" ~first:"b" >|=
      aeq_string_list [ "b"; "cde"; "d"; "e"; "f"; "g" ] >>

    D.read_committed_transaction ks1
      (fun tx ->
         put tx "tbl1" "fg" ["x", ""] >>
         put tx "tbl1" "x" ["x", ""] >>
         get_keys ks1 "tbl1" ~first:"e" >|=
           aeq_string_list ~msg:"read updates in transaction"
             [ "e"; "f"; "fg"; "g"; "x" ] >>
         get_keys ks1 "tbl1" ~first:"f" ~up_to:"g" >|=
           aeq_string_list [ "f"; "fg"; "g"; ] >>
         begin try_lwt
           D.read_committed_transaction ks1
             (fun tx ->
                D.delete_columns tx "tbl1" "fg" ["x"] >>
                D.delete_columns tx "tbl1" "xx" ["x"] >>
                put tx "tbl1" "fgh" ["x", ""] >>
                put tx "tbl1" "xx" ["x", ""] >>
                get_keys ks1 "tbl1" ~first:"f" >|=
                  aeq_string_list ~msg:"nested transactions"
                    [ "f"; "fgh"; "g"; "x"; "xx" ] >>
                raise Exit)
         with Exit -> return ()
         end >>
         get_keys ks1 "tbl1" ~first:"e" >|=
           aeq_string_list [ "e"; "f"; "fg"; "g"; "x" ])


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
    "get_keys_in_range ranges", test_get_keys_in_range_ranges;
  ]

let () =
  register_tests "Data_model" tests
