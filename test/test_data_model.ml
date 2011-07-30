
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


let put tx tbl key l =
  D.put_columns tx tbl key
    (List.map column_without_timestamp l)

let delete = D.delete_columns

let get_key_range ks ?first ?up_to tbl =
  D.read_committed_transaction ks
    (fun tx ->
       D.get_keys_in_range tx tbl (DD.Key_range { DD.first; up_to }))

let get_keys ks tbl l =
  D.read_committed_transaction ks
    (fun tx -> D.get_keys_in_range tx tbl (DD.Keys l))

let test_get_keys_in_range_ranges db =
  let ks1 = D.register_keyspace db "test_get_keys_in_range_ranges" in
  let ks2 = D.register_keyspace db "test_get_keys_in_range_ranges2" in

    get_key_range ks1 "tbl1" >|= aeq_string_list [] >>
    get_key_range ks2 "tbl1" >|= aeq_string_list [] >>

    D.read_committed_transaction ks1
      (fun tx ->
         Lwt_list.iter_s
           (fun k -> put tx "tbl1" k ["name", k])
           ["a"; "ab"; "b"; "cde"; "d"; "e"; "f"; "g"]) >>

    get_key_range ks2 "tbl1" >|= aeq_string_list [] >>
    get_key_range ks1 "tbl1" >|=
      aeq_string_list ["a"; "ab"; "b"; "cde"; "d"; "e"; "f"; "g"] >>
    get_key_range ks1 "tbl1" ~up_to:"c" >|=
      aeq_string_list ["a"; "ab"; "b"; ] >>
    get_key_range ks1 "tbl1" ~first:"c" ~up_to:"c" >|=
      aeq_string_list [ ] >>
    get_key_range ks1 "tbl1" ~first:"c" ~up_to:"f"  >|=
      aeq_string_list [ "cde"; "d"; "e"; "f" ] >>
    get_key_range ks1 "tbl1" ~first:"b" >|=
      aeq_string_list [ "b"; "cde"; "d"; "e"; "f"; "g" ] >>

    D.read_committed_transaction ks1
      (fun tx ->
         put tx "tbl1" "fg" ["x", ""] >>
         put tx "tbl1" "x" ["x", ""] >>
         get_key_range ks1 "tbl1" ~first:"e" >|=
           aeq_string_list ~msg:"read updates in transaction"
             [ "e"; "f"; "fg"; "g"; "x" ] >>
         get_key_range ks1 "tbl1" ~first:"f" ~up_to:"g" >|=
           aeq_string_list [ "f"; "fg"; "g"; ] >>
         begin try_lwt
           D.read_committed_transaction ks1
             (fun tx ->
                D.delete_columns tx "tbl1" "fg" ["x"] >>
                D.delete_columns tx "tbl1" "xx" ["x"] >>
                put tx "tbl1" "fgh" ["x", ""] >>
                put tx "tbl1" "xx" ["x", ""] >>
                get_key_range ks1 "tbl1" ~first:"f" >|=
                  aeq_string_list ~msg:"nested transactions"
                    [ "f"; "fgh"; "g"; "x"; "xx" ] >>
                raise Exit)
         with Exit -> return ()
         end >>
         get_key_range ks1 "tbl1" ~first:"e" >|=
           aeq_string_list [ "e"; "f"; "fg"; "g"; "x" ])

let test_get_keys_in_range_discrete db =
  let ks1 = D.register_keyspace db "test_get_keys_in_range_discrete" in
    get_keys ks1 "tbl" ["a"; "b"] >|= aeq_string_list [] >>
    D.read_committed_transaction ks1
      (fun tx ->
         Lwt_list.iter_s (fun k -> put tx "tbl" k ["x", ""])
           [ "a"; "b"; "c"; "d" ]) >>
    get_keys ks1 "tbl" ["a"; "b"; "d"] >|= aeq_string_list ["a"; "b"; "d"] >>
    get_keys ks1 "tbl" ["c"; "x"; "b"] >|= aeq_string_list ["b"; "c"] >>
    begin try_lwt
      D.read_committed_transaction ks1
        (fun tx ->
           delete tx "tbl" "b" ["x"] >>
           delete tx "tbl" "d" ["x"] >>
           put tx "tbl" "x" ["x", ""] >>
           get_keys ks1 "tbl" ["a"; "d"; "x"] >|=
             aeq_string_list ~msg:"data in transaction" ["a"; "x" ] >>
           raise Exit)
    with Exit -> return () end >>
    get_keys ks1 "tbl" ["c"; "x"; "b"] >|= aeq_string_list ["b"; "c"]

let string_of_column c =
  sprintf "{ name = %S; data = %S; }" c.DD.name c.DD.data

let string_of_key_data kd =
  sprintf "{ key = %S; last_column = %s; columns = %s }"
    kd.DD.key (string_of_option (sprintf "%S") kd.DD.last_column)
    (string_of_list string_of_column kd.DD.columns)

let string_of_slice (last_key, l) =
  sprintf "(%s, %s)"
    (string_of_option (sprintf "%S") last_key)
    (string_of_list string_of_key_data l)

let rd_col (name, data) =
  { DD.name; data; timestamp = DD.No_timestamp }

let put_slice ks tbl l =
  D.read_committed_transaction ks
    (fun tx -> Lwt_list.iter_s (fun (k, cols) -> put tx tbl k cols) l)

let aeq_slice ?msg (last_key, data) actual =
  aeq ?msg string_of_slice
    (last_key,
     List.map
       (fun (key, last_column, cols) ->
          { DD.key; last_column; columns = List.map rd_col cols })
       data)
    actual

let test_get_slice_discrete db =
  let ks = D.register_keyspace db "test_get_slice_discrete" in
    put_slice ks "tbl"
      [
       "a", ["k", "kk"; "v", "vv"];
       "b", ["k1", "kk1"; "v", "vv1"];
       "c", ["k2", "kk2"; "w", "ww"];
      ] >>
    D.read_committed_transaction ks
      (fun tx ->
         D.get_slice tx "tbl" (DD.Keys ["a"; "c"]) DD.All_columns >|=
           aeq_slice
             (Some "c",
              ["a", Some "v", [ "k", "kk"; "v", "vv" ];
               "c", Some "w", [ "k2", "kk2"; "w", "ww" ]]) >>
         delete tx "tbl" "a" ["v"] >>
         delete tx "tbl" "c" ["k2"] >>
         put tx "tbl" "a" ["v2", "v2"] >>
         D.get_slice tx "tbl" (DD.Keys ["a"; "c"]) DD.All_columns >|=
           aeq_slice
             (Some "c",
              ["a", Some "v2", [ "k", "kk"; "v2", "v2"; ];
               "c", Some "w", [ "w", "ww" ]])) >>
    D.read_committed_transaction ks
      (fun tx ->
         D.get_slice tx "tbl" (DD.Keys ["a"; "c"]) DD.All_columns >|=
           aeq_slice
             (Some "c",
              ["a", Some "v2", [ "k", "kk"; "v2", "v2"; ];
               "c", Some "w", [ "w", "ww" ]]))

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
    "get_keys_in_range discrete keys", test_get_keys_in_range_discrete;
    "get_slice discrete", test_get_slice_discrete;
  ]

let () =
  register_tests "Data_model" tests
