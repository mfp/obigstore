
open Obs_request

module type PARSED_VALUE =
sig
  include Obs_key_encoding.CODEC_OPS
  val v : key
end

type parsed_value = (module PARSED_VALUE)

type req =
    Command of Request.request * string option (* redirect to file if Some *)
  | Error of string
  | Directive of string * string list
  | Nothing
  | Dump_local of string option
  | Codec_directive of string * codec

and codec =
    Simple_codec of string
  | Complex_codec of string * codec list

type key_value =
    Atom of atom
  | Tuple of key_value list

and atom = Literal of string | Max_value | Min_value

let curr_keyspace : (string * int) option ref = ref None

let with_ks f =
  match !curr_keyspace with
    None -> Error "Select a keyspace first with   keyspace xxx"
  | Some (_, ks) -> Command (f ks, None)

let with_ks_unwrap f =
  match !curr_keyspace with
    None -> Error "Select a keyspace first with   keyspace xxx"
  | Some (_, ks) -> f ks

let key_codecs :
  (Obs_data_model.table,
   (strict:bool -> Format.formatter -> string -> unit) *
   (key_value -> parsed_value)) Hashtbl.t = Hashtbl.create 13
