
type t = string

external create : unit -> t = "ostore_crc32c_init"
external update_unsafe : t -> string -> int -> int -> unit = "ostore_crc32c_update" "noalloc"
external string : string -> string = "ostore_crc32c_string"
external fix_endianness : t -> unit = "ostore_crc32c_ensure_lsb" "noalloc"

let reset t =
  String.unsafe_set t 0 '\000';
  String.unsafe_set t 1 '\000';
  String.unsafe_set t 2 '\000';
  String.unsafe_set t 3 '\000'

let update t s off len =
  if off < 0 || len < 0 || off + len > String.length s then
    invalid_arg "Crc32c.update";
  update_unsafe t s off len

let result t =
  let s = String.copy t in
    fix_endianness s;
    s

let unsafe_result t = fix_endianness t; t
