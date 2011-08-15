module String = struct include String include BatString end

type ks = int

external custom_comparator_ : unit ->  LevelDB.comparator =
  "ostore_custom_comparator"

let custom_comparator = custom_comparator_ ()

external apply_custom_comparator : string -> string -> int =
  "ostore_apply_custom_comparator"

module TS : sig
  type timestamp_buf = private string
  val make_timestamp_buf : unit -> timestamp_buf
end = struct
  type timestamp_buf = string
  let make_timestamp_buf () = String.create 8
end

include TS

let version = 0

let keyspace_table_prefix = "00"
let keyspace_table_key ksname = "00" ^ ksname

let end_of_db_key = String.make 8 (Char.chr 0xFF)

let decode_keyspace_table_name k =
  if String.slice k ~last:(String.length keyspace_table_prefix) <>
     keyspace_table_prefix then
    None
  else
    Some (String.slice ~first:2 k)

(* datum key format:
 * '1' uint8(keyspace) string(table) string(key) string(column)
 * uint64_LE(timestamp lxor 0xFFFFFFFFFFFFFFFF)
 * var_int(key_len) var_int(col_len) uint8(tbl_len)
 * uint8(len(var_int(key_len)) lsl 3 | len(var_int(col_len)))
 * uint8(version)
 * *)

let encode_datum_key dst ks ~table ~key ~column ~timestamp =
  Bytea.clear dst;
  Bytea.add_char dst '1';
  Bytea.add_byte dst ks;
    Bytea.add_string dst table;
    Bytea.add_string dst key;
    Bytea.add_string dst column;
    Bytea.add_int64_complement_le dst timestamp;
    let off = Bytea.length dst in
      Bytea.add_vint dst (String.length key);
      let klen_len = Bytea.length dst - off in
      let off = Bytea.length dst in
        Bytea.add_vint dst (String.length column);
        let clen_len = Bytea.length dst - off in
          Bytea.add_byte dst (String.length table);
          Bytea.add_byte dst ((klen_len lsl 3) lor clen_len);
          Bytea.add_byte dst version

let encode_table_successor dst ks table =
  encode_datum_key dst ks ~table:(table ^ "\000") ~key:"" ~column:"" ~timestamp:Int64.min_int

external ostore_decode_int64_complement_le : string -> int -> Int64.t =
  "ostore_decode_int64_complement_le"

let decode_timestamp (s : timestamp_buf) =
  ostore_decode_int64_complement_le (s :> string) 0

let decode_timestamp' s =
  if String.length s <> 8 then
    invalid_arg "Datum_key.decode_timestamp': want string of length 8";
  ostore_decode_int64_complement_le s 0

let encode_datum_key_to_string ks ~table ~key ~column ~timestamp =
  let b = Bytea.create 13 in
    encode_datum_key b ks ~table ~key ~column ~timestamp;
    Bytea.contents b

let encode_table_successor_to_string ks table =
  let b = Bytea.create 13 in
    encode_table_successor b ks table;
    Bytea.contents b

let decode_var_int_at s off =
  let rec loop s off shift n =
    match Char.code s.[off] with
        m when m > 128 ->
          loop s (off + 1) (shift + 7) (n lor ((m land 0x7F) lsl shift))
      | m -> n lor (m lsl shift)
  in loop s off 0 0

let decode_datum_key
      ~table_buf_r ~table_len_r
      ~key_buf_r ~key_len_r
      ~column_buf_r ~column_len_r
      ~timestamp_buf
      datum_key len =
  if datum_key.[0] <> '1' then false else
  let last_byte = Char.code datum_key.[len - 2] in
  let clen_len = last_byte land 0x7 in
  let klen_len = (last_byte lsr 3) land 0x7 in (* safer *)
  let t_len = Char.code datum_key.[len - 3] in
  let c_len = decode_var_int_at datum_key (len - 3 - clen_len) in
  let k_len = decode_var_int_at datum_key (len - 3 - clen_len - klen_len) in
  let expected_len =
    2 + t_len + k_len + c_len + 8 + clen_len + klen_len + 1 + 1 + 1
  in
    if expected_len <> len then
      false
    else begin
      begin match table_buf_r, table_len_r with
          None, _ | _, None -> ()
        | Some b, Some l ->
              if String.length !b < t_len then
                b := String.create t_len;
              String.blit datum_key 2 !b 0 t_len;
              l := t_len
      end;
      begin match key_buf_r, key_len_r with
          None, _ | _, None -> ()
        | Some b, Some l ->
            if String.length !b < k_len then
              b := String.create k_len;
            String.blit datum_key (2 + t_len) !b 0 k_len;
            l := k_len
      end;
      begin match column_buf_r, column_len_r with
          None, _ | _,  None -> ()
        | Some b, Some l ->
            if String.length !b < c_len then
              b := String.create c_len;
            String.blit datum_key (2 + t_len + k_len) !b 0 c_len;
            l := c_len
      end;
      begin match timestamp_buf with
          None -> ()
        | Some (b : timestamp_buf) ->
            String.blit datum_key (2 + t_len + k_len + c_len) (b :> string) 0 8;
      end;
      true
    end
