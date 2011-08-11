open Printf
open Test_00util
open OUnit
module List = struct include List include BatList end
module String = struct include String include BatString end

let range min max =
  List.init (max - min + 1) (fun n -> n + min)

let test_vectors =
  [
    range 0x01 0x28, [0x7F; 0x15; 0x2C; 0x0E];
    range 0x29 0x50, [0xF6; 0xEB; 0x80; 0xE9];
    range 0x51 0x78, [0xED; 0xBD; 0x74; 0xDE];
    range 0x79 0xA0, [0x62; 0xC8; 0x79; 0xD5];
    range 0xA1 0xC8, [0xD0; 0x9A; 0x97; 0xBA];
    range 0xC9 0xF0, [0x13; 0xD9; 0x29; 0x2B];
    range 0x01 0xF0, [0x75; 0xD3; 0xC5; 0x24];
  ]

let mk_chr_str n = String.make 1 (Char.chr n)

let mk_str l = String.concat "" (List.map mk_chr_str l)

let to_hex s =
  String.concat " "
    (List.map (fun c -> sprintf "%02x" (Char.code c))
       (String.explode s))

let aeq_crc ?msg expected actual =
  assert_equal ?msg ~printer:to_hex expected actual

let test_crc32c () =
  let test (input, output) =
    let input = mk_str input in
    let output = mk_str output in
      aeq_crc ~msg:"CRC32C output" output (Crc32c.string input)
  in List.iter test test_vectors

let () =
  register_tests "Crc32c" ["test vectors" >:: test_crc32c]
