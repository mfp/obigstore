open OUnit

let () =
  ignore (run_test_tt_main ("All" >::: Test_00util.get_all_tests ()))
