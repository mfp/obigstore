type t = Extprot.Msg_buffer.t

val create : int -> t
val contents : t -> string

val copy : t -> t

val sub : t -> int -> int -> string
val blit : t -> int -> string -> int -> int -> unit
val nth : t -> int -> char
val length : t -> int
val clear : t -> unit
val reset : t -> unit

val add_char : t -> char -> unit
val add_substring : t -> string -> int -> int -> unit
val add_string : t -> string -> unit
val add_bytea : t -> t -> unit
val add_byte : t -> int -> unit
val add_vint : t -> int -> unit
val add_rev_vint : t -> int -> unit

val add_int64_complement_le : t -> Int64.t -> unit
val add_int64_le : t -> Int64.t -> unit
val add_int32_le : t -> int -> unit

val add_int64_complement_be : t -> Int64.t -> unit
val add_int64_be : t -> Int64.t -> unit
val add_int32_be : t -> int -> unit

val add_ieee754_float : t -> float -> unit

(** @raise Invalid_arg if [off < 0 || off > length - 4] *)
val blit_int32_le_at : t -> off:int -> int -> unit

val unsafe_blit_int32_le_at : t -> off:int -> int -> unit

val unsafe_string : t -> string
