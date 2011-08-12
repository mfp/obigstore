
open Printf
open Test_00util
open OUnit

let aeq expected b =
  assert_equal ~printer:to_hex expected (Bytea.contents b)

let test_add_int32_le b =
  Bytea.add_int32_le b (-1);
  aeq (string_of_bytes [ 0xFF; 0xFF; 0xFF; 0xFF ]) b;
  Bytea.add_int32_le b 0xabcd;
  aeq (string_of_bytes [ 0xFF; 0xFF; 0xFF; 0xFF; 0xcd; 0xab; 0x00; 0x00; ]) b;
  Bytea.clear b;
  Bytea.add_int32_le b 0xabcd0102;
  aeq (string_of_bytes [ 0x02; 0x01; 0xcd; 0xab; ]) b

let test_add_int64_le b =
  Bytea.add_int64_le b (-1L);
  aeq (String.make 8 '\255') b;
  Bytea.add_int64_le b 0x1bcd010203040506L;
  aeq (String.make 8 '\255' ^
       string_of_bytes [ 0x06; 0x05; 0x04; 0x03; 0x02; 0x01; 0xcd; 0x1b ]) b

let with_buf f () = f (Bytea.create 10)

let tests =
  List.map (fun (n, f) -> n >:: with_buf f)
    [
      "add_int32_le", test_add_int32_le;
      "add_int64_le", test_add_int64_le;
    ]

let () =
  register_tests "Bytea" tests
