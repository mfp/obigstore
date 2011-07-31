
open Printf
open Lwt
open Test_00util
open OUnit

module List = struct include BatList include List end

module D = Data_model
module DD = D.Data
module DU = D.Update

let string_of_column c =
  sprintf "{ name = %S; data = %S; }" c.DD.name c.DD.data

let string_of_key_data kd =
  sprintf "{ key = %S; last_column = %S; columns = %s }"
    kd.DD.key kd.DD.last_column
    (string_of_list string_of_column kd.DD.columns)

let string_of_slice (last_key, l) =
  sprintf "(%s, %s)"
    (string_of_option (sprintf "%S") last_key)
    (string_of_list string_of_key_data l)

let rd_col (name, data) =
  { DD.name; data; timestamp = DD.No_timestamp }

let column_without_timestamp (name, data) =
  { DU.name; data; timestamp = DU.No_timestamp }

let put tx tbl key l =
  D.put_columns tx tbl key
    (List.map column_without_timestamp l)

let delete = D.delete_columns

let key_range ?first ?up_to () = DD.Key_range { DD.first; up_to }

let put_slice ks tbl l =
  D.read_committed_transaction ks
    (fun tx -> Lwt_list.iter_s (fun (k, cols) -> put tx tbl k cols) l)

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

let get_key_range ks ?max_keys ?first ?up_to tbl =
  D.read_committed_transaction ks
    (fun tx ->
       D.get_keys_in_range tx tbl ?max_keys (key_range ?first ?up_to ()))

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

let test_get_keys_in_range_with_del_put db =
  let ks = D.register_keyspace db "test_get_keys_in_range_with_del_put" in
  let key_name i = sprintf "%03d" i in
    get_key_range ks "tbl" >|= aeq_string_list [] >>
    put_slice ks "tbl"
      (List.init 5 (fun i -> (key_name i, [ "x", "" ]))) >>
    D.read_committed_transaction ks
      (fun tx ->
         get_key_range ks "tbl" >|= aeq_string_list (List.init 5 key_name) >>
         D.delete_key tx "tbl" "001" >>
         D.delete_columns tx "tbl" "002" ["y"; "x"] >>
         D.delete_columns tx "tbl" "003" ["y"; "x"] >>
         put_slice ks "tbl" ["002", ["z", "z"]] >>
         get_key_range ks "tbl" >|= aeq_string_list ["000"; "002"; "004"]) >>
    get_key_range ks "tbl" >|= aeq_string_list ["000"; "002"; "004"]

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

let test_get_keys_in_range_max_keys db =
  let ks = D.register_keyspace db "test_get_keys_in_range_max_keys" in
  let key_name i = sprintf "%03d" i in
    put_slice ks "tbl"
      (List.init 10 (fun i -> (key_name i, [ "x", "" ]))) >>
    D.read_committed_transaction ks
      (fun tx ->
         get_key_range ks "tbl" >|=
           aeq_string_list (List.init 10 key_name) >>
         get_key_range ks "tbl" ~first:"002" ~max_keys:2 >|=
           aeq_string_list ["002"; "003"] >>
         get_key_range ks "tbl" ~up_to:"002" ~max_keys:5 >|=
           aeq_string_list ["000"; "001"; "002"] >>
         get_key_range ks "tbl" ~first:"008" ~max_keys:5 >|=
           aeq_string_list ["008"; "009"] >>
         D.delete_key tx "tbl" "001" >>
         D.delete_columns tx "tbl" "003" ["x"] >>
         D.delete_columns tx "tbl" "002" ["xxxx"] >>
         get_key_range ks "tbl" ~max_keys:3 >|=
           aeq_string_list ["000"; "002"; "004"]) >>
    D.read_committed_transaction ks
      (fun tx ->
         get_key_range ks "tbl" ~max_keys:4 >|=
           aeq_string_list ["000"; "002"; "004"; "005"])

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
              ["a", "v", [ "k", "kk"; "v", "vv" ];
               "c", "w", [ "k2", "kk2"; "w", "ww" ]]) >>
         delete tx "tbl" "a" ["v"] >>
         delete tx "tbl" "c" ["k2"] >>
         put tx "tbl" "a" ["v2", "v2"] >>
         D.get_slice tx "tbl" (DD.Keys ["a"; "c"]) DD.All_columns >|=
           aeq_slice
             (Some "c",
              ["a", "v2", [ "k", "kk"; "v2", "v2"; ];
               "c", "w", [ "w", "ww" ]])) >>
    D.read_committed_transaction ks
      (fun tx ->
         D.get_slice tx "tbl" (DD.Keys ["a"; "c"]) DD.All_columns >|=
           aeq_slice
             (Some "c",
              ["a", "v2", [ "k", "kk"; "v2", "v2"; ];
               "c", "w", [ "w", "ww" ]]))

let test_get_slice_key_range db =
  let ks = D.register_keyspace db "test_get_slice_key_range" in
  let all = DD.All_columns in
    put_slice ks "tbl"
      [
       "a", ["k", "kk"; "v", "vv"];
       "b", ["k1", "kk1"; "v", "vv1"];
       "c", ["k2", "kk2"; "w", "ww"];
      ] >>
    D.read_committed_transaction ks
      (fun tx ->
         let expect1 =
           aeq_slice
             (Some "b",
              [ "a", "v", [ "k", "kk"; "v", "vv"; ];
                "b", "v", [ "k1", "kk1"; "v", "vv1" ]])
         in
           D.get_slice tx "tbl" (key_range ~first:"a" ~up_to:"b" ()) all >|= expect1 >>
           D.get_slice tx "tbl" (key_range ~up_to:"b" ()) all >|= expect1 >>
           D.get_slice tx "tbl" (key_range ~first:"b" ~up_to:"c" ())
             (DD.Columns ["k1"; "w"]) >|=
             aeq_slice
             (Some "c",
              [ "b", "k1", [ "k1", "kk1" ];
                "c", "w", [ "w", "ww" ] ]))

let test_get_slice_max_keys db =
  let ks = D.register_keyspace db "test_get_slice_max_keys" in
  let all = DD.All_columns in
    put_slice ks "tbl"
      (List.init 100 (fun i -> (sprintf "%03d" i, [ "x", "" ]))) >>
    D.read_committed_transaction ks
      (fun tx ->
         D.get_slice tx "tbl" ~max_keys:2 (key_range ()) all >|=
           aeq_slice
             (Some "001", [ "000", "x", ["x", ""]; "001", "x", ["x", ""]]) >>
         D.get_slice tx "tbl" ~max_keys:2 (key_range ~first:"002" ()) all >|=
           aeq_slice
             (Some "003", [ "002", "x", ["x", ""]; "003", "x", ["x", ""]]) >>
         D.delete_columns tx "tbl" "002" ["x"] >>
         D.get_slice tx "tbl" ~max_keys:2 (key_range ~first:"002" ()) all >|=
           aeq_slice
             (Some "004", [ "003", "x", ["x", ""]; "004", "x", ["x", ""]]) >>
         put_slice ks "tbl" [ "002", [ "y", "" ] ] >>
         D.get_slice tx "tbl" ~max_keys:2 (key_range ~first:"002" ()) all >|=
           aeq_slice
             (Some "003", [ "002", "y", ["y", ""]; "003", "x", ["x", ""]]) >>
         D.get_slice tx "tbl" ~max_keys:1 (DD.Keys ["001"; "002"])
           (DD.Columns ["y"]) >|=
           aeq_slice (Some "002", [ "002", "y", ["y", ""] ])) >>
    D.read_committed_transaction ks
      (fun tx ->
         D.get_slice tx "tbl" ~max_keys:2 (key_range ~first:"002" ()) all >|=
           aeq_slice
             (Some "003", [ "002", "y", ["y", ""]; "003", "x", ["x", ""]]) >>
         D.delete_columns tx "tbl" "002" ["y"] >>
         put_slice ks "tbl" [ "002", ["x", ""] ] >>
         D.get_slice tx "tbl" ~max_keys:1000 (key_range ()) all >|=
           aeq_slice
             (Some "099",
              List.init 100
                (fun i -> (sprintf "%03d" i, "x", ["x", ""]))) >>
         D.get_slice tx "tbl" ~max_keys:2 (DD.Keys ["003"; "001"; "002"]) all >|=
           aeq_slice (Some "002",
                      [ "001", "x", ["x", ""];
                        "002", "x", ["x", ""]]))

let test_get_slice_nested_transactions db =
  let ks = D.register_keyspace db "test_get_slice_nested_transactions" in
  let all = DD.All_columns in
    put_slice ks "tbl"
      [
        "a", [ "c1", ""; "c2", "" ];
        "b", [ "c1", ""; "c2", "" ];
        "c", [ "c1", ""; "c2", "" ];
      ] >>
    D.read_committed_transaction ks
      (fun tx ->
         D.get_slice tx "tbl" (key_range ~first:"b" ()) all >|=
           aeq_slice (Some "c",
                      ["b", "c2", [ "c1", ""; "c2", "" ];
                       "c", "c2",  [ "c1", ""; "c2", "" ]]) >>
         put_slice ks "tbl" ["a", ["c4", "c4"]] >>
         begin try_lwt
           D.read_committed_transaction ks
             (fun tx ->
                delete tx "tbl" "b" ["c1"; "c2"] >>
                delete tx "tbl" "c" ["c2"] >>
                put_slice ks "tbl" ["c", ["c3", "c3"]] >>
                D.get_slice tx "tbl" (key_range ()) all >|=
                  aeq_slice
                    ~msg:"nested tx data"
                    (Some "c",
                     ["a", "c4", ["c1", ""; "c2", ""; "c4", "c4"];
                      "c", "c3", ["c1", ""; "c3", "c3"]]) >>
                raise Exit)
         with Exit -> return () end >>
         D.get_slice tx "tbl" (key_range ()) all >|=
           aeq_slice ~msg:"after aborted transaction"
             (Some "c",
              ["a", "c4", ["c1", ""; "c2", ""; "c4", "c4"];
               "b", "c2", ["c1", ""; "c2", ""];
               "c", "c2", ["c1", ""; "c2", ""]]))

let test_delete_key db =
  let ks = D.register_keyspace db "test_delete_key" in
  let get_all tx = D.get_slice tx "tbl" (key_range ()) DD.All_columns in
    put_slice ks "tbl"
      [ "a", [ "x", ""; "y", ""; "z", "" ];
        "b", [ "x", ""; "y", ""; "z", "" ]] >>
    D.read_committed_transaction ks
      (fun tx ->
         get_all tx >|=
           aeq_slice ~msg:"before delete"
             (Some "b",
              [ "a", "z", [ "x", ""; "y", ""; "z", "" ];
                "b", "z", [ "x", ""; "y", ""; "z", "" ]]) >>
         D.delete_key tx "tbl" "b" >>
         let expect_after_del msg =
           aeq_slice ~msg
             (Some "a",
              [ "a", "z", [ "x", ""; "y", ""; "z", "" ]])
         in get_all tx >|= expect_after_del "with key range">>
            D.get_slice tx "tbl" (DD.Keys ["a"; "b"]) DD.All_columns >|=
              expect_after_del "with discrete keys") >>
    D.read_committed_transaction ks
      (fun tx ->
         get_all tx >|=
           aeq_slice ~msg:"after delete, after transaction commit"
             (Some "a", [ "a", "z", [ "x", ""; "y", ""; "z", "" ]]))

let get_all tx tbl =
  D.get_slice tx tbl (key_range ()) DD.All_columns

let test_delete_columns db =
  let ks = D.register_keyspace db "test_delete_columns" in
    put_slice ks "tbl"
      [ "a", [ "x", ""; "y", ""; "z", "" ];
        "b", [ "x", ""; "y", ""; "z", "" ]] >>
    D.read_committed_transaction ks
      (fun tx ->
         get_all tx "tbl" >|=
           aeq_slice ~msg:"before delete_columns"
             (Some "b",
              [ "a", "z", [ "x", ""; "y", ""; "z", "" ];
                "b", "z", [ "x", ""; "y", ""; "z", "" ]]) >>
         D.delete_columns tx "tbl" "a" ["x"; "z"] >>
         D.delete_columns tx "tbl" "b" ["x"; "y"; "z"] >>
         get_all tx "tbl" >|=
           aeq_slice ~msg:"after delete, in transaction"
             (Some "a", ["a", "y", ["y", ""]])) >>
    D.read_committed_transaction ks
      (fun tx ->
         get_all tx "tbl" >|=
           aeq_slice ~msg:"after transaction commit"
             (Some "a", ["a", "y", ["y", ""]]))

let test_put_columns db =
  let ks = D.register_keyspace db "test_put_columns" in
    put_slice ks "tbl"
      [ "a", [ "x", ""; "y", ""; "z", "" ];
        "b", [ "x", ""; "y", ""; "z", "" ]] >>
    put_slice ks "tbl"
      [ "a", [ "x", "x"; "zz", ""];
        "c", [ "z", ""] ] >>
    D.read_committed_transaction ks
      (fun tx ->
         get_all tx "tbl" >|=
           aeq_slice
             (Some "c",
              [ "a", "zz", [ "x", "x"; "y", ""; "z", ""; "zz", "" ];
                "b", "z", [ "x", ""; "y", ""; "z", "" ];
                "c", "z", [ "z", "" ]]) >>
         put_slice ks "tbl" [ "c", ["c", "c"]] >>
         D.get_slice tx "tbl" (DD.Keys ["c"]) DD.All_columns >|=
           aeq_slice (Some "c", [ "c", "z", ["c", "c"; "z", ""] ])) >>
    D.read_committed_transaction ks
      (fun tx ->
         D.get_slice tx "tbl" (DD.Keys ["c"]) DD.All_columns >|=
           aeq_slice (Some "c", [ "c", "z", ["c", "c"; "z", ""] ]))

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
    "get_keys_in_range honors max_keys", test_get_keys_in_range_max_keys;
    "get_keys_in_range with delete/put", test_get_keys_in_range_with_del_put;
    "get_slice discrete", test_get_slice_discrete;
    "get_slice key range", test_get_slice_key_range;
    "get_slice honors max_keys", test_get_slice_max_keys;
    "get_slice nested transactions", test_get_slice_nested_transactions;
    "put_columns", test_put_columns;
    "delete_key", test_delete_key;
    "delete_columns", test_delete_columns;
  ]

let () =
  register_tests "Data_model" tests
