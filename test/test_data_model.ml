
open Printf
open Lwt
open Test_00util
open OUnit

module D = Data_model

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
  ]

let () =
  register_tests "Data_model" tests
