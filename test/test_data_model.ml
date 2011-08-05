
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

let key_range ?first ?last () =
  let up_to = match last with
      None -> None
    | Some l -> Some (l ^ "\000")
  in DD.Key_range { DD.first; up_to; }

let col_range ?first ?last ?up_to () =
  let up_to = match up_to with
      Some _ as x -> x
    | None -> match last with
          None -> None
        | Some x -> Some (x ^ "\000")
  in DD.Column_range { DD.first; up_to; }

let put_slice ks tbl l =
  D.read_committed_transaction ks
    (fun tx -> Lwt_list.iter_s (fun (k, cols) -> put tx tbl k cols) l)

let test_custom_comparator db =
  let ks = D.register_keyspace db "test_custom_comparator" in
  let ks2 = D.register_keyspace db "test_custom_comparator2" in

  let encode ks (table, key, column) =
    let b = Bytea.create 13 in
      D.Encoding.encode_datum_key b ks ~table ~key ~column ~timestamp:Int64.min_int;
      Bytea.contents b in
  let cmp ks1 a ks2 b = D.apply_custom_comparator (encode ks1 a) (encode ks2 b) in
  let printer (tbl, key, col) =
    sprintf "{ table = %S; key = %S; column = %S }" tbl key col in
  let ksname () ks = D.keyspace_name ks in
  let aeq ?(ks1 = ks) ?(ks2 = ks) a b =
              match cmp ks1 a ks2 b with
        0 -> ()
      | n -> assert_failure_fmt
               "Expected equality for %a:%s vs %a:%s, got %d"
               ksname ks1 (printer a) ksname ks2 (printer b) n in
  let alt ?(ks1 = ks) ?(ks2 = ks) a b =
    match cmp ks1 a ks2 b with
        n when n < 0 -> ()
      | n -> assert_failure_fmt
               "Expected LT for %a:%s vs %a:%s, got %d"
               ksname ks1 (printer a) ksname ks2 (printer b) n in
  let agt ?(ks1 = ks) ?(ks2 = ks) a b =
    match cmp ks1 a ks2 b with
        n when n > 0 -> ()
      | n -> assert_failure_fmt
               "Expected GT for %a:%s vs %a:%s, got %d"
               ksname ks1 (printer a) ksname ks2 (printer b) n in
  let agt ?ks1 ?ks2 a b =
    aeq ?ks1 ?ks2:ks1 a a;
    aeq ?ks1 ?ks2:ks1 b b;
    aeq ?ks1:ks2 ?ks2 b b;
    aeq ?ks1:ks2 ?ks2 a a;
    agt ?ks1 ?ks2 a b;
    alt ?ks1:ks2 ?ks2:ks1 b a
  in
    agt ~ks1:ks2 ("", "", "") ("", "", "");
    agt ~ks1:ks2 ("", "", "") ("x", "", "");
    agt ~ks1:ks2 ("a", "b", "c") ("a", "b", "d");
    agt ~ks1:ks2 ("a", "b", "c") ("a", "c", "c");
    agt ~ks1:ks2 ("a", "b", "c") ("b", "b", "c");
    agt ("x", "", "") ("a", "", "");
    agt ("", "x", "") ("", "", "");
    agt ("", "x", "") ("", "", "");
    agt ("\000", "", "") ("", "", "");
    agt ("\000", "", "") ("", "\000", "");
    agt ("\000", "", "") ("", "\000", "b");
    agt ("1", "\000", "") ("1", "", "\000");
    agt ("1", "1\000", "") ("1", "1", "\000");
    agt ("tbl", "k\000", "2") ("tbl", "k", "\0003");
    return ()

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

let get_key_range ks ?max_keys ?first ?last tbl =
  D.read_committed_transaction ks
    (fun tx ->
       D.get_keys tx tbl ?max_keys (key_range ?first ?last ()))

let get_keys ks tbl l =
  D.read_committed_transaction ks
    (fun tx -> D.get_keys tx tbl (DD.Keys l))

let test_get_keys_ranges db =
  let ks1 = D.register_keyspace db "test_get_keys_ranges" in
  let ks2 = D.register_keyspace db "test_get_keys_ranges2" in

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
    get_key_range ks1 "tbl1" ~last:"c" >|=
      aeq_string_list ["a"; "ab"; "b"; ] >>
    get_key_range ks1 "tbl1" ~first:"c" ~last:"c" >|=
      aeq_string_list [ ] >>
    get_key_range ks1 "tbl1" ~first:"c" ~last:"f"  >|=
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
         get_key_range ks1 "tbl1" ~first:"f" ~last:"g" >|=
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

let test_get_keys_with_del_put db =
  let ks = D.register_keyspace db "test_get_keys_with_del_put" in
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

let test_get_keys_discrete db =
  let ks1 = D.register_keyspace db "test_get_keys_discrete" in
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

let test_get_keys_max_keys db =
  let ks = D.register_keyspace db "test_get_keys_max_keys" in
  let key_name i = sprintf "%03d" i in
    put_slice ks "tbl"
      (List.init 10 (fun i -> (key_name i, [ "x", ""; "y", "" ]))) >>
    D.read_committed_transaction ks
      (fun tx ->
         get_key_range ks "tbl" >|=
           aeq_string_list (List.init 10 key_name) >>
         get_key_range ks "tbl" ~first:"002" ~max_keys:2 >|=
           aeq_string_list ["002"; "003"] >>
         get_key_range ks "tbl" ~last:"002" ~max_keys:5 >|=
           aeq_string_list ["000"; "001"; "002"] >>
         get_key_range ks "tbl" ~first:"008" ~max_keys:5 >|=
           aeq_string_list ["008"; "009"] >>
         D.delete_key tx "tbl" "001" >>
         D.delete_columns tx "tbl" "003" ["x"; "y"] >>
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

let string_of_slice_columns (last_key, l) =
  sprintf "(%s, %s)"
    (string_of_option (sprintf "%S") last_key)
    (string_of_list
       (string_of_tuple2
          (sprintf "%S")
          (string_of_list (string_of_option (sprintf "%S"))))
       l)

let aeq_slice_columns ?msg x actual =
  aeq ?msg string_of_slice_columns x actual

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
           D.get_slice tx "tbl" (key_range ~first:"a" ~last:"b" ()) all >|= expect1 >>
           D.get_slice tx "tbl" (key_range ~last:"b" ()) all >|= expect1 >>
           D.get_slice tx "tbl" (key_range ~first:"b" ~last:"c" ())
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

let test_get_slice_max_columns db =
  let ks = D.register_keyspace db "test_get_slice_max_columns" in
    put_slice ks "tbl"
      (List.init 10
         (fun i -> (sprintf "%02d" i,
                    List.init 10 (fun i -> string_of_int i, "")))) >>
    D.read_committed_transaction ks
      (fun tx ->
         D.get_slice tx "tbl" ~max_keys:2 ~max_columns:2
           (key_range ()) DD.All_columns >|=
           aeq_slice
             (Some "01", ["00", "1", ["0", ""; "1", ""];
                          "01", "1", ["0", ""; "1", ""]]) >>
         D.get_slice tx "tbl" ~max_keys:2 ~max_columns:1
           (key_range ()) (DD.Columns ["2"; "1"]) >|=
           aeq_slice
             (Some "01", ["00", "1", ["1", ""]; "01", "1", ["1", ""]]) >>
         D.delete_columns tx "tbl" "01" ["1"; "2"] >>
         D.get_slice tx "tbl" ~max_keys:2 ~max_columns:3
           (key_range ()) (DD.Columns ["2"; "1"; "0"]) >|=
           aeq_slice
             (Some "01", ["00", "2", ["0", ""; "1", ""; "2", ""];
                          "01", "0", ["0", ""]]) >>
         put_slice ks "tbl" [ "01",  ["10", "a"; "11", "b"] ] >>
         D.get_slice tx "tbl" ~max_keys:2 ~max_columns:3
           (key_range ()) DD.All_columns >|=
           aeq_slice
             (Some "01", ["00", "2", ["0", ""; "1", ""; "2", ""];
                          "01", "11", ["0", ""; "10", "a"; "11", "b"]]) >>
         D.get_slice tx "tbl" ~max_keys:2 ~max_columns:3
           (key_range ()) (DD.Columns ["2"; "1"; "0"]) >|=
           aeq_slice ~msg:"all keys, Columns 2, 1, 0"
             (Some "01", ["00", "2", ["0", ""; "1", ""; "2", ""];
                          "01", "0", ["0", ""]])) >>
    D.read_committed_transaction ks
      (fun tx ->
         D.get_slice tx "tbl" ~max_keys:2 ~max_columns:3
           (key_range ()) DD.All_columns >|=
           aeq_slice
             (Some "01", ["00", "2", ["0", ""; "1", ""; "2", ""];
                          "01", "11", ["0", ""; "10", "a"; "11", "b"]]) >>
         D.get_slice tx "tbl" ~max_keys:2 ~max_columns:3
           (DD.Keys ["01"; "00"]) (DD.Columns ["2"; "1"; "0"; "10"]) >|=
           aeq_slice
             (Some "01", ["00", "2", ["0", ""; "1", ""; "2", ""];
                          "01", "10", ["0", ""; "10", "a"]]))

let test_get_slice_column_ranges db =
  let ks = D.register_keyspace db "test_get_slice_column_ranges" in
  let expect tx =
    D.get_slice tx "tbl" ~max_keys:1
      (key_range ~first:"02" ~last:"03" ())
      (col_range ~first:"5" ~last:"6" ()) >|=
    aeq_slice (Some "02", ["02", "6", ["5", ""; "6", ""]]) >>
    D.get_slice tx "tbl" ~max_keys:1 ~max_columns:1
      (key_range ~first:"02" ())
      (col_range ~first:"5" ~last:"6" ()) >|=
    aeq_slice (Some "02", ["02", "5", ["5", ""]]) >>
    D.get_slice tx "tbl" ~max_keys:1 ~max_columns:1
      (key_range ~first:"02" ())
      (col_range ~first:"50" ~last:"6" ()) >|=
        aeq_slice (Some "02", ["02", "6", ["6", ""]]) >>
    D.get_slice tx "tbl" ~max_keys:1 ~max_columns:1
      (key_range ~first:"02" ())
      (col_range ~first:"7" ~last:"6" ()) >|=
        aeq_slice (None, [])
  in
    D.read_committed_transaction ks
      (fun tx ->
         put_slice ks "tbl"
           (List.init 10
              (fun i -> (sprintf "%02d" i,
                         List.init 10 (fun i -> string_of_int i, "")))) >>
         expect tx) >>
    D.read_committed_transaction ks expect

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

let test_get_slice_read_tx_data db =
  let ks = D.register_keyspace db "test_get_slice_read_tx_data" in
    put_slice ks "tbl" [ "a", ["0", ""; "1", ""]; "b", ["0", ""; "1", ""] ] >>
    D.read_committed_transaction ks
      (fun tx ->
         put_slice ks "tbl" [ "a", ["00", ""]; "b", [ "10", ""] ] >>
         D.get_slice tx "tbl" (key_range ()) DD.All_columns >|=
           aeq_slice
             (Some "b", ["a", "1", ["0", ""; "00", ""; "1", ""];
                         "b", "10", ["0", ""; "1", ""; "10", ""]]) >>
         D.get_slice tx "tbl" (key_range ()) (DD.Columns ["0"; "00"]) >|=
           aeq_slice ~msg:"key range"
             (Some "b", ["a", "00", ["0", ""; "00", ""];
                         "b", "0", ["0", ""]]) >>
         D.get_slice tx "tbl" (DD.Keys ["a"; "b"]) (DD.Columns ["0"; "00"]) >|=
           aeq_slice ~msg:"discrete keys"
             (Some "b", ["a", "00", ["0", ""; "00", ""];
                         "b", "0", ["0", ""]]))

(* check that iteration over datum keys is performed correctly
 *   key "foo"  column "\0001"
 * should precede
 *   key "foo\000" column "0"
 * *)
let test_get_slice_tricky_columns db =
  let ks = D.register_keyspace db "test_get_slice_tricky_columns" in
    put_slice ks "tbl"
      [ "k", [ "\0003", "" ];
        "k\000", [ "2", "" ] ] >>
    D.read_committed_transaction ks
      (fun tx ->
         D.get_slice tx "tbl" (DD.Keys ["k"]) DD.All_columns >|=
           aeq_slice (Some "k", [ "k", "\0003", [ "\0003", "" ] ]) >>
         D.get_slice tx "tbl" (DD.Keys ["k\000"]) DD.All_columns >|=
           aeq_slice (Some "k\000", [ "k\000", "2", [ "2", "" ] ]))

let test_get_slice_values db =
  let ks = D.register_keyspace db "test_get_slice_values" in
  let add_data () =
    put_slice ks "tbl"
      [ "a", (List.init 10 (fun n -> (sprintf "%d" n, sprintf "a%d" n)));
        "b", ["0", "b0"; "3", "b3"];
        "c", ["1", "c1"] ] in
  let assertions tx =
    D.get_slice_values tx "tbl" (DD.Keys ["a"; "b"]) ["0"; "1"] >|=
      aeq_slice_columns
        (Some "b", ["a", [Some "a0"; Some "a1"];
                    "b", [Some "b0"; None]]) >>
    D.get_slice_values tx "tbl" (DD.Keys ["a"; "b"]) ["1"] >|=
      aeq_slice_columns
        (Some "b", ["a", [Some "a1"];
                    "b", [None]]) >>
    D.get_slice_values tx "tbl" (key_range ()) ["0"; "2"] >|=
      aeq_slice_columns
        (Some "c", ["a", [Some "a0"; Some "a2"];
                    "b", [Some "b0"; None];
                    "c", [None; None]])
  in
    D.read_committed_transaction ks (fun tx -> add_data () >> assertions tx) >>
    (* also after commit *)
    D.read_committed_transaction ks assertions

let test_get_column_values db =
  let ks = D.register_keyspace db "test_get_column_values" in
  let aeq ?msg =
    aeq ?msg (string_of_list (string_of_option (sprintf "%S")))
  in
    put_slice ks "tbl"
      [ "a", [ "0", ""; "1", "1"; "3", "" ] ] >>
    D.read_committed_transaction ks
      (fun tx ->
         D.get_column_values tx "tbl" "a" ["1"; "2"; "0"] >|=
           aeq [Some "1"; None; Some ""] >>
         D.get_column_values tx "tbl" "b" ["1"] >|=
           aeq [None] >>
         put_slice ks "tbl" [ "b", [ "1", "b1" ] ] >>
         D.get_column_values tx "tbl" "b" ["1"; "2"] >|=
           aeq [Some "b1"; None]) >>
    D.read_committed_transaction ks
      (fun tx ->
         D.get_column_values tx "tbl" "b" ["1"; "2"] >|=
           aeq [Some "b1"; None])

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
    "custom comparator", test_custom_comparator;
    "keyspace management", test_keyspace_management;
    "list tables", test_list_tables;
    "get_keys ranges", test_get_keys_ranges;
    "get_keys discrete keys", test_get_keys_discrete;
    "get_keys honors max_keys", test_get_keys_max_keys;
    "get_keys with delete/put", test_get_keys_with_del_put;
    "get_slice discrete", test_get_slice_discrete;
    "get_slice key range", test_get_slice_key_range;
    "get_slice honors max_keys", test_get_slice_max_keys;
    "get_slice nested transactions", test_get_slice_nested_transactions;
    "get_slice in open transaction", test_get_slice_read_tx_data;
    "get_slice honor max_columns", test_get_slice_max_columns;
    "get_slice with column ranges", test_get_slice_column_ranges;
    "get_slice correct iteration with tricky columns", test_get_slice_tricky_columns;
    "get_slice_values", test_get_slice_values;
    "get_column_values", test_get_column_values;
    "put_columns", test_put_columns;
    "delete_key", test_delete_key;
    "delete_columns", test_delete_columns;
  ]

let () =
  register_tests "Data_model" tests
