open Lwt
open Obs_data_model

module Option = BatOption
module List = struct include List include BatList end

module type RAW =
sig
  type keyspace

  val get_slice :
    keyspace -> table ->
    ?max_keys:int -> ?max_columns:int -> ?decode_timestamps:bool ->
    string key_range -> ?predicate:row_predicate -> column_range ->
    (string, string) slice Lwt.t

  val get_slice_values :
    keyspace -> table ->
    ?max_keys:int ->
    string key_range -> column_name list ->
    (key option * (key * string option list) list) Lwt.t

  val get_slice_values_with_timestamps :
    keyspace -> table ->
    ?max_keys:int ->
    string key_range -> column_name list ->
    (key option * (key * (string * Int64.t) option list) list) Lwt.t

  val get_columns :
    keyspace -> table ->
    ?max_columns:int -> ?decode_timestamps:bool ->
    key -> column_range ->
    (column_name * (string column list)) option Lwt.t

  val get_column_values :
    keyspace -> table ->
    key -> column_name list ->
    string option list Lwt.t

  val get_column :
    keyspace -> table ->
    key -> column_name -> (string * timestamp) option Lwt.t

  val put_columns :
    keyspace -> table -> key -> string column list ->
    unit Lwt.t

  val put_multi_columns :
    keyspace -> table -> (key * string column list) list -> unit Lwt.t
end

module type STRUCTURED =
sig
  type keyspace

  val get_bson_slice :
    keyspace -> table ->
    ?max_keys:int -> ?max_columns:int -> ?decode_timestamps:bool ->
    string key_range -> ?predicate:row_predicate -> column_range ->
    (string, decoded_data) slice Lwt.t

  val get_bson_slice_values :
    keyspace -> table ->
    ?max_keys:int ->
    string key_range -> column_name list ->
    (key option * (key * decoded_data option list) list) Lwt.t

  val get_bson_slice_values_with_timestamps :
    keyspace -> table ->
    ?max_keys:int ->
    string key_range -> column_name list ->
    (key option * (key * (decoded_data * Int64.t) option list) list) Lwt.t

  val get_bson_columns :
    keyspace -> table ->
    ?max_columns:int -> ?decode_timestamps:bool ->
    key -> column_range ->
    (column_name * (decoded_data column list)) option Lwt.t

  val get_bson_column_values :
    keyspace -> table ->
    key -> column_name list ->
    decoded_data option list Lwt.t

  val get_bson_column :
    keyspace -> table ->
    key -> column_name -> (decoded_data * timestamp) option Lwt.t

  val put_bson_columns :
    keyspace -> table -> key -> decoded_data column list ->
    unit Lwt.t

  val put_multi_bson_columns :
    keyspace -> table -> (key * decoded_data column list) list -> unit Lwt.t
end

module Make(M : RAW) =
struct
  open M
  type keyspace = M.keyspace

  let try_decode s =
    try
      BSON (Obs_bson.document_of_string s)
    with Obs_bson.Malformed _ ->
      Malformed_BSON s

  let map_column c =
    if String.length c.name < 1 || c.name.[0] <> '@' then
      { c with data = Binary c.data }
    else
      { c with data = try_decode c.data }

  let revmap_column c =
    let data = match c.data with
        Binary s | Malformed_BSON s -> s
      | BSON x -> Obs_bson.string_of_document x
    in { c with data }

  let map_key_data kd = { kd with columns = List.map map_column kd.columns }

  let get_bson_slice ks table ?max_keys ?max_columns ?decode_timestamps
        key_range ?predicate column_range =
    lwt k, l = get_slice ks table ?max_keys ?max_columns
                 ?decode_timestamps key_range ?predicate column_range
    in return (k, List.map map_key_data l)

  let is_bson_col columns =
    let a =
      Array.of_list
        (List.map (fun c -> c <> "" && c.[0] = '@') columns)
    in `Staged (Array.get a)

  let get_bson_slice_values ks table ?max_keys key_range columns =
    let `Staged is_bson = is_bson_col columns in
    let map_col i s = match s with
        None -> None
      | Some s ->
          if not (is_bson i) then Some (Binary s)
          else Some (try_decode s) in
    lwt k, l = get_slice_values ks table ?max_keys key_range columns in
      return (k, List.map (fun (k, vs) -> (k, List.mapi map_col vs)) l)

  let get_bson_slice_values_with_timestamps ks table ?max_keys key_range columns =
    let `Staged is_bson = is_bson_col columns in
    let map_col i s = match s with
        None -> None
      | Some (s, ts) ->
          if not (is_bson i) then Some (Binary s, ts)
          else Some (try_decode s, ts) in
    lwt k, l = get_slice_values_with_timestamps ks table ?max_keys key_range columns in
      return (k, List.map (fun (k, vs) -> (k, List.mapi map_col vs)) l)

  let get_bson_columns ks table ?max_columns ?decode_timestamps key col_range =
    get_columns ks table ?max_columns ?decode_timestamps key col_range >|=
    Option.map (fun (c, l) -> (c, List.map map_column l))

  let get_bson_column_values ks table key cols =
    let `Staged is_bson = is_bson_col cols in
    let map_col i s = match s with
        None -> None
      | Some s ->
          if not (is_bson i) then Some (Binary s)
          else Some (try_decode s)
    in get_column_values ks table key cols >|= List.mapi map_col

  let get_bson_column ks table key col =
    get_column ks table key col >|=
      Option.map
        (fun (d, ts) -> if col = "" || col.[0] <> '@' then (Binary d, ts)
                     else (try_decode d, ts))

  let put_bson_columns ks table key cols =
    put_columns ks table key (List.map revmap_column cols)

  let put_multi_bson_columns ks table l =
    put_multi_columns ks table
      (List.map (fun (k, cols) -> (k, List.map revmap_column cols)) l)
end
