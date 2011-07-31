
open Lwt

module String = struct include String include BatString end
module List = struct include BatList include List end
module Option = BatOption
module L = LevelDB
module RA = L.Read_access
module IT = L.Iterator

type db =
    {
      basedir : string;
      db : L.db;
      keyspaces : (string, int) Hashtbl.t;
    }

type keyspace = { ks_db : db; ks_name : string; ks_id : int }

module Data =
struct
  type table = string

  type key = string

  type column = { name : column_name; data : string; timestamp : timestamp; }

  and column_name = string

  and timestamp = No_timestamp | Timestamp of Int64.t

  type key_data = { key : key; last_column : string; columns : column list }

  type slice = key option * key_data list (** last_key * data *)

  type range =
    {
      first : string option;
      up_to : string option;
    }

  type key_range =
      Key_range of range
    | Keys of string list

  type column_range =
      All_columns
    | Columns of string list
end

module Update =
struct
  type column_data = { name : Data.column_name; data : string;
                       timestamp : timestamp }

  and timestamp = No_timestamp | Auto_timestamp | Timestamp of Int64.t
end


module Encoding =
struct
  let keyspace_table_prefix = "00"
  let keyspace_table_key ksname = "00" ^ ksname

  let decode_keyspace_table_name k =
    if String.slice k ~last:(String.length keyspace_table_prefix) <>
       keyspace_table_prefix then
      None
    else
      Some (String.slice ~first:2 k)

  let rec decode_var_int_upto s ~offset prev =
    match Char.code s.[!offset] with
        n when n < 128 -> decr offset; n
      | n -> decr offset;
             decode_var_int_upto s ~offset
               ((prev lsl 7) lor (n land 0x7f))

  (* datum key format:
   * '1' uint8(keyspace) asciiz(table) stringz(key) stringz(column)
   * rev_var_int(key_len) rev_var_int(col_len)
   * *)

  let encode_datum_key dst ks ~table ~key ~column =
    Bytea.clear dst;
    Bytea.add_char dst '1';
    Bytea.add_byte dst ks.ks_id;
    let add_strz dst s =
      Bytea.add_string dst s;
      Bytea.add_byte dst 0
    in
      add_strz dst table;
      add_strz dst key;
      add_strz dst column;
      Bytea.add_rev_vint dst (String.length key);
      Bytea.add_rev_vint dst (String.length column)

  let encode_table_successor dst ks table =
    encode_datum_key dst ks ~table ~key:"" ~column:"";
    (* find the '\000' after the table name, increment it to \001 *)
    let idx = String.index_from (Bytea.unsafe_string dst) 2 '\000' in
      (Bytea.unsafe_string dst).[idx] <- '\001'

  let encode_datum_key_to_string ks ~table ~key ~column =
    let b = Bytea.create 13 in
      encode_datum_key b ks ~table ~key ~column;
      Bytea.contents b

  let encode_table_successor_to_string ks table =
    let b = Bytea.create 13 in
      encode_table_successor b ks table;
      Bytea.contents b

  let decode_datum_key
        ?table_buf ?table_len
        ?key_buf ?key_len
        ?column_buf ?column_len
        datum_key len =
    if datum_key.[0] <> '1' then false else
    let offset = ref (len - 1) in
    let c_len = decode_var_int_upto datum_key ~offset 0 in
    let k_len = decode_var_int_upto datum_key ~offset 0 in
    let t_len = String.index_from datum_key 2 '\000' - 2 in
      if c_len + k_len + 5 > len then
        false
      else begin
        begin match table_buf, table_len with
            None, _ | _, None -> ()
          | Some b, Some l ->
                if String.length !b < t_len then
                  b := String.create t_len;
                String.blit datum_key 2 !b 0 t_len;
                l := t_len
        end;
        begin match key_buf, key_len with
            None, _ | _, None -> ()
          | Some b, Some l ->
              if String.length !b < k_len then
                b := String.create k_len;
              String.blit datum_key (t_len + 3) !b 0 k_len;
              l := k_len
        end;
        begin match column_buf, column_len with
            None, _ | _,  None -> ()
          | Some b, Some l ->
              if String.length !b < c_len then
                b := String.create c_len;
              String.blit datum_key (t_len + k_len + 4) !b 0 c_len;
              l := c_len
        end;
        true
      end
end

let read_keyspaces db =
  let h = Hashtbl.create 13 in
    L.iter_from
      (fun k v ->
         match Encoding.decode_keyspace_table_name k with
             None -> false
           | Some keyspace_name ->
               Hashtbl.add h keyspace_name (int_of_string v);
               true)
      db
      Encoding.keyspace_table_prefix;
    h

let open_db basedir =
  let db = L.open_db basedir in
    { basedir; db; keyspaces = read_keyspaces db; }

let close_db t = L.close t.db

let list_keyspaces t =
  List.sort String.compare (Hashtbl.fold (fun k v l -> k :: l) t.keyspaces [])

let register_keyspace t ks_name =
  try
    { ks_db = t; ks_name; ks_id = Hashtbl.find t.keyspaces ks_name }
  with Not_found ->
    let max_id = Hashtbl.fold (fun _ v id -> max v id) t.keyspaces 0 in
    let ks_id = max_id + 1 in
      L.put ~sync:true t.db (Encoding.keyspace_table_key ks_name) (string_of_int ks_id);
      Hashtbl.add t.keyspaces ks_name ks_id;
      { ks_db = t; ks_name; ks_id; }

let get_keyspace t ks_name =
  try
    Some ({ ks_db = t; ks_name; ks_id = Hashtbl.find t.keyspaces ks_name })
  with Not_found -> None

let keyspace_name ks = ks.ks_name

let list_tables ks =
  let it = L.iterator ks.ks_db.db in
  let table_buf = ref "" in
  let table_len = ref 0 in

  let jump_to_next_table () =
    match String.sub !table_buf 0 !table_len with
        "" ->
          let datum_key =
            Encoding.encode_datum_key_to_string ks ~table:"" ~key:"" ~column:""
          in IT.seek it datum_key 0 (String.length datum_key);
      | table ->
          let datum_key = Encoding.encode_table_successor_to_string ks table in
            IT.seek it datum_key 0 (String.length datum_key); in

  let rec collect_tables acc =
    jump_to_next_table ();
    if not (IT.valid it) then acc
    else begin
      let k = IT.get_key it in
        if not (Encoding.decode_datum_key ~table_buf ~table_len
                  k (String.length k))
        then
          collect_tables acc
        else
          collect_tables ((String.sub !table_buf 0 !table_len) :: acc)
    end

  in List.rev (collect_tables [])

module S =
struct
  include Set.Make(String)

  let of_list l = List.fold_left (fun s x -> add x s) empty l
  let to_list s = List.rev (fold (fun x l -> x :: l) s [])

  let subset ?first ?up_to s =
    (* trim entries before the first, if proceeds *)
    let s = match first with
        None -> s
      | Some first ->
          let _, found, after = split first s in
            if found then add first after else after in
    (* trim entries after the last, if proceeds *)
    let s = match up_to with
        None -> s
      | Some last ->
          let before, found, _ = split last s in
            if found then add last before else before
    in s
end

module M =
struct
  include Map.Make(String)
  let find_default x k m = try find k m with Not_found -> x
  let values m = List.rev (fold (fun _ v l -> v :: l) m [])

  let submap ?first ?up_to m =
    (* trim entries before the first, if proceeds *)
    let m = match first with
        None -> m
      | Some first ->
          let _, found, after = split first m in
            match found with
                Some v -> add first v after
              | None -> after in
    (* trim entries after the last, if proceeds *)
    let m = match up_to with
        None -> m
      | Some last ->
          let before, found, _ = split last m in
            match found with
                Some v -> add last v before
              | None -> before
    in m

  let maybe_max_binding m =
    try Some (max_binding m) with Not_found -> None

  let update f default k m =
    let x = find_default default k m in
      match f x with
          None -> remove k m
        | Some y -> add k y m
end

type transaction =
    {
      mutable deleted_keys : S.t M.t; (* table -> key set *)
      mutable added_keys : S.t M.t; (* table -> key set *)
      mutable added : string M.t M.t M.t; (* table -> key -> column -> value *)
      mutable deleted : S.t M.t M.t; (* table -> key -> column set *)
      access : L.read_access;
      ks : keyspace;
    }

let tx_key = Lwt.new_key ()

let transaction_aux make_access ks f =
  match Lwt.get tx_key with
    | None -> begin
        let tx =
          { added_keys = M.empty; deleted_keys = M.empty;
            added = M.empty; deleted = M.empty;
            access = make_access ks.ks_db.db; ks;
          } in
        lwt y = Lwt.with_value tx_key (Some tx) (fun () -> f tx) in
        let b = L.Batch.make () in
        let datum_key = Bytea.create 13 in
          (* TODO: should iterate in Lwt monad so we can yield every once in a
           * while*)
          M.iter
            (fun table m ->
               M.iter
                 (fun key s ->
                    S.iter
                      (fun column ->
                         Encoding.encode_datum_key datum_key ks ~table ~key ~column;
                         L.Batch.delete_substring b
                           (Bytea.unsafe_string datum_key) 0
                           (Bytea.length datum_key))
                      s)
                 m)
            tx.deleted;
          M.iter
            (fun table m ->
               M.iter
                 (fun key m ->
                    M.iter
                      (fun column v ->
                         Encoding.encode_datum_key datum_key ks ~table ~key ~column;
                         L.Batch.put_substring b
                           (Bytea.unsafe_string datum_key) 0 (Bytea.length datum_key)
                           v 0 (String.length v))
                      m)
                 m)
            tx.added;
          L.Batch.write ~sync:true ks.ks_db.db b; (* FIXME: should not block *)
          return y
      end
    | Some parent_tx ->
        (* we make a new access so as to honor repeatable_read_transaction
         * nested in a read_committed_transaction *)
        let tx = { parent_tx with access = make_access ks.ks_db.db } in
        lwt y = Lwt.with_value tx_key (Some tx) (fun () -> f tx) in
          parent_tx.deleted_keys <- tx.deleted_keys;
          parent_tx.added <- tx.added;
          parent_tx.deleted <- tx.deleted;
          parent_tx.added_keys <- tx.added_keys;
          return y

let read_committed_transaction f = transaction_aux L.read_access f

let repeatable_read_transaction f =
  transaction_aux
    (fun db -> L.Snapshot.read_access (L.Snapshot.make db))
    f

let (|>) x f = f x

(* string -> string ref -> int -> bool *)
let is_same_value v buf len =
  String.length v = !len && String_util.strneq v 0 !buf 0 !len

(* In [let f = exists_key tx table in ... f key], [f key] returns true iff
 * there's some column with the given key in the given [table]. *)
let exists_key tx table =
  let datum_key = Bytea.create 13 in
  let it = RA.iterator tx.access in
  let buf = ref "" in
  let table_buf = ref "" and table_len = ref 0 in
  let key_buf = ref "" and key_len = ref 0 in
  let column_buf = ref "" and column_len = ref 0 in
  let deleted_col_tbl = M.find_default M.empty table tx.deleted

  in begin fun key ->
    Encoding.encode_datum_key datum_key tx.ks ~table ~key ~column:"";
    IT.seek it (Bytea.unsafe_string datum_key) 0 (Bytea.length datum_key);
    if not (IT.valid it) then
      false
    else begin
      let len = IT.fill_key it buf in
      let ok =
        Encoding.decode_datum_key
          ~table_buf ~table_len ~key_buf ~key_len
          ~column_buf ~column_len
          !buf len
      in
        if not ok then false
        else
          (* verify that it's the same table and key, and the column is not
           * deleted *)
          is_same_value table table_buf table_len &&
          is_same_value key key_buf key_len &&
          (let col = String.sub !column_buf 0 !column_len in
             not (M.find_default S.empty key deleted_col_tbl |>
                  S.mem col))
    end
  end

let fold_over_data tx table f acc first up_to =
  let it = RA.iterator tx.access in

  let buf = ref "" in
  let table_buf = ref "" and table_len = ref 0 in
  let key_buf = ref "" and key_len = ref 0 in
  let column_buf = ref "" and column_len = ref 0 in
  let past_upto = match up_to with
      None -> (fun () -> false)
    | Some k ->
        (fun () ->
           String_util.cmp_substrings
             !key_buf 0 !key_len
             k 0 (String.length k) > 0) in

  let rec do_fold_over_data acc =
    if not (IT.valid it) then
      acc
    else begin
      let len = IT.fill_key it buf in
        if not (Encoding.decode_datum_key
                  ~table_buf ~table_len
                  ~key_buf ~key_len
                  ~column_buf ~column_len
                  !buf len)
        then acc (* if this happens, we must have hit the end of the data area *)
        else begin
          (* check that we're still in the table *)
          if not (is_same_value table table_buf table_len) then
            acc
          (* check if we're past up_to *)
          else if past_upto () then
            acc
          else begin
            match (f acc it ~key_buf ~key_len ~column_buf ~column_len)
            with
                (* TODO: allow to
                 * * skip key
                 * * finish recursion
                 * by using sum type like
                 *   Skip_to_key of 'acc * key | Finish of 'acc
                 * | Continue of 'a | Continue_no_next of 'a *)
                x -> IT.next it; do_fold_over_data x
          end
      end
    end in

  (* jump to first entry *)
  let first_datum_key =
    Encoding.encode_datum_key_to_string tx.ks ~table
      ~key:(Option.default "" first) ~column:""
  in
    IT.seek it first_datum_key 0 (String.length first_datum_key);
    do_fold_over_data acc

let get_keys_in_range tx table ?(max_keys = max_int) = function
    Data.Keys l ->
      let exists_key = exists_key tx table in
      let s = S.of_list l in
      let s = S.diff s (M.find_default S.empty table tx.deleted_keys) in
      let s =
        S.filter
          (fun k ->
             S.mem k (try M.find table tx.added_keys with Not_found -> S.empty) ||
             exists_key k)
          s
      in List.take max_keys (S.to_list s)
  | Data.Key_range { Data.first; up_to } ->
      (* we recover all the keys added in the transaction *)
      let s = (try M.find table tx.added_keys with Not_found -> S.empty) in
      let s = S.subset ?first ?up_to s in
      (* now s contains the added keys in the wanted range *)
      let deleted_keys = M.find_default S.empty table tx.deleted_keys in
      let module M = struct exception Finished of S.t end in
      let keys_on_disk_kept = ref 0 in
      let fold_datum s it
            ~key_buf ~key_len
            ~column_buf ~column_len =
        (* TODO: optimize by caching previous key, ignoring new entry
         * if same key *)
        if !keys_on_disk_kept >= max_keys then
          raise (M.Finished s)
        else
          let k = String.sub !key_buf 0 !key_len in
            if S.mem k deleted_keys then s
            else begin
              incr keys_on_disk_kept;
              S.add k s
            end
          (* TODO: seek to the datum_key corresponding to a successor
           * of the key when the key has been repeated more than
           * THRESHOLD times. *)
      in
        (try fold_over_data tx table fold_datum s first up_to with M.Finished s -> s) |>
        S.to_list |> List.take max_keys

let get_keys_in_range tx table ?max_keys range =
  return (get_keys_in_range tx table ?max_keys range)

let get_slice tx table
      ?(max_keys = max_int)
      key_range column_range =
  let column_selected = match column_range with
      Data.All_columns -> (fun c -> true)
    | Data.Columns l -> let s = S.of_list l in (fun c -> S.mem c s) in

  let deleted_col_table = tx.deleted |> M.find_default M.empty table in

  let is_column_deleted ~key_buf ~key_len ~column_buf ~column_len =
    let key =
      if key_len = String.length key_buf then key_buf
      else String.sub key_buf 0 key_len in
    let col =
      if column_len = String.length column_buf then column_buf
      else String.sub column_buf 0 column_len
    in
      S.mem col (M.find_default S.empty key deleted_col_table)

  in match key_range with
    Data.Keys l ->
      let s = S.of_list l in
      let s = S.diff s (M.find_default S.empty table tx.deleted_keys) in
      let key_data_list, _ =
        S.fold
          (fun key (key_data_list, keys_so_far) ->
             if keys_so_far >= max_keys then (key_data_list, keys_so_far)
             else
               let fold_datum m it ~key_buf ~key_len ~column_buf ~column_len =
                 (* TODO: use substring sets and avoid allocating a string
                  * before the following checks *)
                 let col = String.sub !column_buf 0 !column_len in
                   (* must skip deleted columns *)
                   if is_column_deleted
                        ~key_buf:key ~key_len:(String.length key)
                        ~column_buf:!column_buf ~column_len:!column_len then
                     m
                   (* also skip non-selected columns *)
                   else if not (column_selected col) then
                     m
                   (* otherwise, if the column is not deleted and is selected *)
                   else begin
                     let data = IT.get_value it in
                       M.add col { Data.name = col; data; timestamp = Data.No_timestamp } m
                   end in
               let m = fold_over_data tx table fold_datum M.empty
                         (Some key) (Some (key ^ "\000")) in
               let m =
                 M.fold
                   (fun col data m ->
                      M.add col { Data.name = col; data; timestamp = Data.No_timestamp} m)
                   (tx.added |>
                    M.find_default M.empty table |> M.find_default M.empty key)
                   m
               in
                 try
                   let last_column = fst (M.max_binding m) in
                     ({ Data.key; last_column; columns = M.values m; } :: key_data_list,
                      keys_so_far + 1)
                 with Not_found -> (key_data_list, keys_so_far))
          s ([], 0) in
      let max_key =
        List.fold_left
          (fun max_key key_data -> match max_key with
               None -> Some key_data.Data.key
             | Some x -> Some (max x key_data.Data.key))
          None key_data_list
      in (max_key, List.rev key_data_list)

    | Data.Key_range { Data.first; up_to } ->
        let fold_datum m it ~key_buf ~key_len ~column_buf ~column_len =
          (* TODO: use substring sets/maps and avoid allocating the following
           * unless we want the data *)
          let col = String.sub !column_buf 0 !column_len in
            if is_column_deleted
                 ~key_buf:!key_buf ~key_len:!key_len
                 ~column_buf:col ~column_len:(String.length col)
            then
              m
            else if not (column_selected col) then
              m
            else begin
              (* column not deleted, and selected *)
              let key = String.sub !key_buf 0 !key_len in
              let data = IT.get_value it in
              let col_data = { Data.name = col; data; timestamp = Data.No_timestamp} in
              let key_data = M.find_default M.empty key m in
                M.add key (M.add col col_data key_data) m
            end in

        (* m : key -> column_name -> column *)
        let m = fold_over_data tx table fold_datum M.empty
                  first up_to in
        let m =
          M.fold
            (fun key key_data_in_mem m ->
               let key_data' =
                 M.fold
                   (fun col data m ->
                      let col_data = { Data.name = col; data; timestamp = Data.No_timestamp} in
                        M.add col col_data m)
                   key_data_in_mem
                   (M.find_default M.empty key m)
               in M.add key key_data' m)
            (M.submap ?first ?up_to
               (M.find_default M.empty table tx.added))
            m in
        let key_data_list, _ =
          M.fold
            (fun key key_data (l, keys_so_far) ->
               if keys_so_far >= max_keys then (l, keys_so_far)
               else
                 try
                   let last_column = fst (M.max_binding key_data) in
                     ({ Data.key; last_column; columns = M.values key_data; } :: l,
                      keys_so_far + 1)
                 with Not_found -> (l, keys_so_far))
            m
            ([], 0) in
        let last_key = match key_data_list with
            { Data.key; _ } :: _ -> Some key
          | [] -> None
        in (last_key, List.rev key_data_list)

let get_slice tx table ?max_keys key_range column_range =
  return (get_slice tx table ?max_keys key_range column_range)

let get_columns tx table ?(max_columns = max_int) key column_range =
  match_lwt get_slice tx table (Data.Keys [key]) column_range with
    | (_, { Data.last_column = last_column;
            columns = ((_ :: _ as columns)) } :: _ ) ->
        return (Some (last_column, columns))
    | _ -> return None

let get_column tx table key column_name =
  match_lwt get_columns tx table key (Data.Columns [column_name]) with
      Some (_, c :: _) -> return (Some (c.Data.data, c.Data.timestamp))
    | _ -> return None

let put_columns tx table key columns =
  tx.added_keys <- M.update
                     (fun s -> Some (S.add key s))
                     S.empty table tx.added_keys;
  tx.deleted_keys <- M.update
                       (fun s -> Some (S.remove key s))
                       S.empty table tx.deleted_keys;
  tx.deleted <-
    M.update
      (fun m ->
         Some
           (M.update
              (fun m ->
                 Some (List.fold_left (fun m c -> S.remove c.Update.name m) m columns))
              S.empty key m))
      M.empty table tx.deleted;
  tx.added <-
    M.update
      (fun m ->
         Some
           (M.update
              (fun m ->
                 Some (List.fold_left
                         (fun m c -> M.add c.Update.name c.Update.data m) m columns))
              M.empty key m))
      M.empty table tx.added;
  return ()

let delete_columns tx table key cols =
  tx.added <-
    M.update
      (fun m ->
         Some
           (M.update
             (fun m ->
                Some (List.fold_left (fun m c -> M.remove c m) m cols))
             M.empty
             key m))
      M.empty table tx.added;
  if M.is_empty (M.find_default M.empty table tx.added |> M.find_default M.empty key) then
    tx.added_keys <- M.update (fun s -> Some (S.remove key s)) S.empty table tx.added_keys;
  tx.deleted <-
    M.update
      (fun m ->
         Some
           (M.update
              (fun s -> Some (List.fold_left (fun s c -> S.add c s) s cols))
              S.empty
              key m))
      M.empty table tx.deleted;
  return ()

let delete_key tx table key =
  match_lwt get_columns tx table key Data.All_columns with
      None -> return ()
    | Some (_, columns) ->
        delete_columns tx table key (List.map (fun c -> c.Data.name) columns)
