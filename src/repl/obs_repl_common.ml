
open Obs_request

type req =
    Command of Request.request * string option (* redirect to file if Some *)
  | Error of string
  | Directive of string * string list
  | Nothing
  | Dump_local of string option
  | Printer_directive of string * printer

and printer =
    Simple_printer of string
  | Complex_printer of string * printer list

type generic_range =
    Range of string Range.range
  | List of string list

let curr_keyspace : (string * int) option ref = ref None

let with_ks f =
  match !curr_keyspace with
    None -> Error "Select a keyspace first with   keyspace xxx"
  | Some (_, ks) -> Command (f ks, None)

let with_ks_unwrap f =
  match !curr_keyspace with
    None -> Error "Select a keyspace first with   keyspace xxx"
  | Some (_, ks) -> f ks
