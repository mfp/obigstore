module L = LevelDB
module IT = L.Iterator
open Data_model

type cursor =
  { bc_remaining_tables : table list; bc_key : key; bc_column : column_name; }

module DEC =
struct
  exception EOS

  type t = { data : string; mutable off : int; max : int; }

  let make data = { data; off = 0; max = String.length data; }

  let get_vint t =
    let rec loop_get_vint t n shift off =
      if off >= t.max then raise EOS;
      let m = Char.code (String.unsafe_get t.data off) in
      let n = n lor ((m land 0x7F) lsl shift) in
        if m < 128 then (t.off <- off + 1; n)
        else loop_get_vint t n (shift + 7) (off+1)
    in loop_get_vint t 0 0 t.off

  let read_raw_string dst t len =
    if t.off + len > t.max then raise EOS;
    if String.length !dst < len then dst := String.create len;
    String.blit t.data t.off !dst 0 len;
    t.off <- t.off + len

  let read_string dst t =
    let len = get_vint t in
      read_raw_string dst t len

  let get_string t =
    let s = ref "" in
      read_string s t;
      !s

  let get_timestamp t =
    let s = ref "" in
      read_raw_string s t 8;
      Datum_key.decode_timestamp' !s

  let get_list f t =
    let rec loop_get_list f t acc = function
        0 -> List.rev acc
      | n -> let x = f t in loop_get_list f t (x :: acc) (n - 1) in
    let len = get_vint t in
      loop_get_list f t [] len
end

let string_of_cursor c =
  let b = Bytea.create 13 in
  let add_string b s =
    Bytea.add_vint b (String.length s);
    Bytea.add_string b s
  in
    Bytea.add_vint b (List.length c.bc_remaining_tables);
    List.iter (add_string b) c.bc_remaining_tables;
    add_string b c.bc_key;
    add_string b c.bc_column;
    Bytea.contents b

let cursor_of_string c =
    let src = DEC.make c in
    let bc_remaining_tables = DEC.get_list DEC.get_string src in
    let bc_key = DEC.get_string src in
    let bc_column = DEC.get_string src in
      Some { bc_remaining_tables; bc_key; bc_column; }

let add_datum dst table it
      ~key_buf ~key_len ~column_buf ~column_len ~timestamp_buf ~value_buf =
  Bytea.add_vint dst (String.length table);
  Bytea.add_string dst table;
  Bytea.add_vint dst key_len;
  Bytea.add_substring dst key_buf 0 key_len;
  Bytea.add_vint dst column_len;
  Bytea.add_substring dst column_buf 0 column_len;
  Bytea.add_string dst (timestamp_buf : Datum_key.timestamp_buf :> string);
  let len = IT.fill_value it value_buf in
    Bytea.add_vint dst len;
    Bytea.add_substring dst !value_buf 0 len

let load encode_datum_key writebatch data =
  let datum_key = Bytea.create 13 in

  let load_datum src =
    let table = DEC.get_string src in
    let key = DEC.get_string src in
    let column = DEC.get_string src in
    let timestamp = DEC.get_timestamp src in
    let value_len = DEC.get_vint src in
      encode_datum_key datum_key ~table ~key ~column ~timestamp;
      L.Batch.put_substring writebatch
        (Bytea.unsafe_string datum_key) 0 (Bytea.length datum_key)
        src.DEC.data src.DEC.off value_len;
      src.DEC.off <- src.DEC.off + value_len

  in
    try
      let src = DEC.make data in
        while true do
          load_datum src
        done
    with DEC.EOS -> ()
