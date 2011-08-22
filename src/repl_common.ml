
open Request

type req =
    Command of Request.request
  | Error of string
  | Directive of unit
  | Nothing

type generic_range =
    Range of Range.range
  | List of string list

let curr_keyspace : (string * int) option ref = ref None

let with_ks f =
  match !curr_keyspace with
    None -> Error "Select a keyspace first with   keyspace xxx"
  | Some (_, ks) -> Command (f ks)
