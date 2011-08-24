(*
 * Copyright (C) 2011 Mauricio Fernandez <mfp@acm.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version,
 * with the special exception on linking described in file LICENSE.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *)

open Lwt
open Data_model

module List = struct include BatList include List end
module Option = BatOption
module L = LevelDB
module RA = L.Read_access
module IT = L.Iterator

module String =
struct
  include String
  include BatString

  let compare x y =
    String_util.cmp_substrings x 0 (String.length x) y 0 (String.length y)
end

module LOCKS =
  Weak.Make(struct
              type t = (string * Lwt_mutex.t option)
              let hash (s, _) = Hashtbl.hash s
              let equal (s1, _) (s2, _) = String.compare s1 s2 = 0
            end)

type db =
    {
      basedir : string;
      db : L.db;
      keyspaces : (string, keyspace) Hashtbl.t;
      mutable use_thread_pool : bool;
    }

and keyspace =
  {
    ks_db : db; ks_name : string; ks_id : int;
    ks_tables : (string, int) Hashtbl.t;
    ks_rev_tables : (int, string) Hashtbl.t;
    ks_locks : LOCKS.t;
  }

type backup_cursor = Backup.cursor

let (|>) x f = f x

let find_maybe h k = try Some (Hashtbl.find h k) with Not_found -> None

let read_keyspace_tables db keyspace =
  let module T = Datum_encoding.Keyspace_tables in
  let h = Hashtbl.create 13 in
    L.iter_from
      (fun k v ->
         match T.decode_ks_table_key k with
             None -> false
           | Some (ks, table) when ks <> keyspace -> false
           | Some (_, table) -> Hashtbl.add h table (int_of_string v);
                                true)
      db.db
      (T.ks_table_table_prefix_for_ks keyspace);
    h

let read_keyspaces db =
  let h = Hashtbl.create 13 in
    L.iter_from
      (fun k v ->
         match Datum_encoding.decode_keyspace_table_name k with
             None -> false
           | Some keyspace_name ->
               let ks_db = db in
               let ks_name = keyspace_name in
               let ks_id = int_of_string v in
               let ks_tables = read_keyspace_tables db ks_name in
               let ks_rev_tables = Hashtbl.create (Hashtbl.length ks_tables) in
               let ks_locks = LOCKS.create 13 in
                 Hashtbl.iter
                   (fun k v -> Hashtbl.add ks_rev_tables v k)
                   ks_tables;
                 Hashtbl.add h keyspace_name
                   { ks_db; ks_id; ks_name; ks_tables; ks_rev_tables; ks_locks; };
                 true)
      db.db
      Datum_encoding.keyspace_table_prefix;
    h

let open_db basedir =
  let db = L.open_db ~comparator:Datum_encoding.custom_comparator basedir in
    (* ensure we have a end_of_db record *)
    if not (L.mem db Datum_encoding.end_of_db_key) then
      L.put ~sync:true db Datum_encoding.end_of_db_key (String.make 8 '\000');
    let db = { basedir; db; keyspaces = Hashtbl.create 13; use_thread_pool = false; } in
    let keyspaces = read_keyspaces db in
      Hashtbl.iter (Hashtbl.add db.keyspaces) keyspaces;
      db

let use_thread_pool db v = db.use_thread_pool <- v

let close_db t = L.close t.db

let detach_ks_op ks f x =
  if ks.ks_db.use_thread_pool then Lwt_preemptive.detach f x
  else return (f x)

let list_keyspaces t =
  Hashtbl.fold (fun k v l -> k :: l) t.keyspaces [] |>
  List.sort String.compare |> return

let register_keyspace t ks_name =
  try
    return (Hashtbl.find t.keyspaces ks_name)
  with Not_found ->
    let max_id = Hashtbl.fold (fun _ ks id -> max ks.ks_id id) t.keyspaces 0 in
    let ks_id = max_id + 1 in
      L.put ~sync:true t.db (Datum_encoding.keyspace_table_key ks_name) (string_of_int ks_id);
      let ks =
        { ks_db = t; ks_id; ks_name;
          ks_tables = Hashtbl.create 13;
          ks_rev_tables = Hashtbl.create 13;
          ks_locks = LOCKS.create 31;
        }
      in
        Hashtbl.add t.keyspaces ks_name ks;
        return ks

let get_keyspace t ks_name =
  return begin try
    Some (Hashtbl.find t.keyspaces ks_name)
  with Not_found -> None end

let keyspace_name ks = ks.ks_name

let keyspace_id ks = ks.ks_id

let list_tables ks =
  let it = L.iterator ks.ks_db.db in
  let table_r = ref (-1) in

  let jump_to_next_table () =
    match !table_r with
        -1 ->
          let datum_key =
            Datum_encoding.encode_datum_key_to_string ks.ks_id
              ~table:0 ~key:"" ~column:"" ~timestamp:Int64.min_int
          in IT.seek it datum_key 0 (String.length datum_key);
      | table ->
          let datum_key = Datum_encoding.encode_table_successor_to_string ks.ks_id table in
            IT.seek it datum_key 0 (String.length datum_key); in

  let rec collect_tables acc =
    jump_to_next_table ();
    if not (IT.valid it) then acc
    else begin
      let k = IT.get_key it in
        if not (Datum_encoding.decode_datum_key
                  ~table_r
                  ~key_buf_r:None ~key_len_r:None
                  ~column_buf_r:None ~column_len_r:None
                  ~timestamp_buf:None
                  k (String.length k)) ||
           Datum_encoding.get_datum_key_keyspace_id k <> ks.ks_id
        then
          (* we have hit the end of the keyspace or the data area *)
          acc
        else begin
          let acc = match find_maybe ks.ks_rev_tables !table_r with
              Some table -> table :: acc
            | None -> acc
          in collect_tables acc
        end
    end

  in detach_ks_op ks collect_tables [] >|= List.sort String.compare

let table_size_on_disk ks table_name =
  match find_maybe ks.ks_tables table_name with
      None -> return 0L
    | Some table ->
        let _from =
          Datum_encoding.encode_datum_key_to_string ks.ks_id
           ~table ~key:"" ~column:"" ~timestamp:Int64.min_int in
        let _to =
          Datum_encoding.encode_datum_key_to_string ks.ks_id
            ~table ~key:"\255\255\255\255\255\255" ~column:"\255\255\255\255\255\255"
            ~timestamp:Int64.zero
        in detach_ks_op ks (L.get_approximate_size ks.ks_db.db _from) _to

let key_range_size_on_disk ks ?first ?up_to table_name =
  match find_maybe ks.ks_tables table_name with
      None -> return 0L
    | Some table ->
        let _from =
          Datum_encoding.encode_datum_key_to_string ks.ks_id
            ~table ~key:(Option.default "" first) ~column:""
            ~timestamp:Int64.min_int in
        let _to =
          Datum_encoding.encode_datum_key_to_string ks.ks_id
            ~table
            ~key:(Option.default "\255\255\255\255\255\255" up_to)
            ~column:"\255\255\255\255\255\255"
            ~timestamp:Int64.zero
        in detach_ks_op ks (L.get_approximate_size ks.ks_db.db _from) _to

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
    (* trim entries starting from up_to if proceeds *)
    let s = match up_to with
        None -> s
      | Some last ->
          let before, _, _ = split last s in
            before
    in s
end

module M =
struct
  include Map.Make(String)

  let find_default x k m = try find k m with Not_found -> x

  let submap ?first ?up_to m =
    (* trim entries before the first, if proceeds *)
    let m = match first with
        None -> m
      | Some first ->
          let _, found, after = split first m in
            match found with
                Some v -> add first v after
              | None -> after in
    (* trim entries starting with up_to, if proceeds *)
    let m = match up_to with
        None -> m
      | Some last ->
          let before, _, _ = split last m in
            before
    in m

  let modify f default k m = add k (f (find_default default k m)) m

  let modify_if_found f k m = try add k (f (find k m)) m with Not_found -> m
end

type transaction =
    {
      mutable deleted_keys : S.t M.t; (* table -> key set *)
      mutable added_keys : S.t M.t; (* table -> key set *)
      mutable added : string M.t M.t M.t; (* table -> key -> column -> value *)
      mutable deleted : S.t M.t M.t; (* table -> key -> column set *)
      access : L.read_access;
      repeatable_read : bool;
      iter_pool : L.iterator Lwt_pool.t option;
      ks : keyspace;
      mutable backup_writebatch : L.writebatch Lazy.t;
      (* approximate size of data written in current backup_writebatch *)
      mutable backup_writebatch_size : int;
      outermost_tx : transaction;
      mutable locks : (string * Lwt_mutex.t option) M.t;
    }

let tx_key = Lwt.new_key ()

let register_new_table_and_add_to_writebatch ks wb table_name =
  let last = Hashtbl.fold (fun _ idx m -> max m idx) ks.ks_tables 0 in
  (* TODO: find first free one (if existent) LT last *)
  let table = last + 1 in
  let k = Datum_encoding.Keyspace_tables.ks_table_table_key ks.ks_name table_name in
  let v = string_of_int table in
    L.Batch.put_substring wb
      k 0 (String.length k) v 0 (String.length v);
    Hashtbl.add ks.ks_tables table_name table;
    Hashtbl.add ks.ks_rev_tables table table_name;
    table

let rec transaction_aux make_access_and_iters ks f =
  match Lwt.get tx_key with
    | None -> begin
        let access, iter_pool, repeatable_read =
          make_access_and_iters ks.ks_db None in
        let rec tx =
          { added_keys = M.empty; deleted_keys = M.empty;
            added = M.empty; deleted = M.empty;
            access; repeatable_read; iter_pool; ks;
            backup_writebatch = lazy (L.Batch.make ());
            backup_writebatch_size = 0;
            outermost_tx = tx; locks = M.empty;
          } in
        try_lwt
          lwt y = Lwt.with_value tx_key (Some tx) (fun () -> f tx) in
            commit_outermost_transaction ks tx >>
            return y
        finally
          (* release locks *)
          M.iter (fun _ (_, m) -> Option.may Lwt_mutex.unlock m) tx.locks;
          return ()
      end
    | Some parent_tx ->
        let access, iter_pool, repeatable_read =
          make_access_and_iters ks.ks_db (Some parent_tx) in
        let tx = { parent_tx with access; iter_pool; repeatable_read; } in
        lwt y = Lwt.with_value tx_key (Some tx) (fun () -> f tx) in
          parent_tx.deleted_keys <- tx.deleted_keys;
          parent_tx.added <- tx.added;
          parent_tx.deleted <- tx.deleted;
          parent_tx.added_keys <- tx.added_keys;
          return y

and commit_outermost_transaction ks tx =
  (* early termination for read-only or NOP txs if:
   * * backup_writebatch has not be forced (i.e., not used for bulk
   *   load)
   * * no columns have been added / deleted *)
  if not (Lazy.lazy_is_val tx.backup_writebatch) &&
     M.is_empty tx.deleted && M.is_empty tx.added
  then return ()

  else
  (* normal procedure: write to writebatch, then sync it *)

  let b = Lazy.force tx.backup_writebatch in
  let datum_key = Bytea.create 13 in
  let timestamp = Int64.of_float (Unix.gettimeofday () *. 1e6) in
    (* TODO: should iterate in Lwt monad so we can yield every once in a
     * while*)
    M.iter
      (fun table_name m ->
         match find_maybe ks.ks_tables table_name with
             Some table ->
               M.iter
                 (fun key s ->
                    S.iter
                      (fun column ->
                         Datum_encoding.encode_datum_key datum_key ks.ks_id
                           ~table ~key ~column ~timestamp;
                         L.Batch.delete_substring b
                           (Bytea.unsafe_string datum_key) 0
                           (Bytea.length datum_key))
                      s)
                 m
           | None -> (* unknown table *) ())
      tx.deleted;
    M.iter
      (fun table_name m ->
         let table = match find_maybe ks.ks_tables table_name with
             Some t -> t
           | None ->
               (* must add table to keyspace table list *)
               register_new_table_and_add_to_writebatch ks b table_name
         in M.iter
           (fun key m ->
              M.iter
                (fun column v ->
                   (* FIXME: should save timestamp provided in
                    * put_columns and use it here if it wasn't
                    * Auto_timestamp *)
                   Datum_encoding.encode_datum_key datum_key ks.ks_id
                     ~table ~key ~column ~timestamp;
                   L.Batch.put_substring b
                     (Bytea.unsafe_string datum_key) 0 (Bytea.length datum_key)
                     v 0 (String.length v))
                m)
           m)
      tx.added;
    detach_ks_op ks (L.Batch.write ~sync:true ks.ks_db.db) b

let read_committed_transaction f =
  transaction_aux
    (fun db -> function
         None -> (L.read_access db.db, None, false)
       | Some tx -> (tx.access, tx.iter_pool, false))
    f

let repeatable_read_transaction f =
  transaction_aux
    (fun db -> function
         Some { access; repeatable_read = true; iter_pool = Some pool; _} ->
           (access, Some pool, true)
       | _ ->
           let access = L.Snapshot.read_access (L.Snapshot.make db.db) in
           let pool = Lwt_pool.create 1000 (* FIXME: which limit? *)
                        (fun () -> return (RA.iterator access))
           in (access, Some pool, true))
    f

let lock ks name =
  match Lwt.get tx_key with
      None -> return ()
    | Some tx ->
      if M.mem name tx.locks then return ()
      else
        let ks = tx.ks in
        let k, mutex =
          try
            let k = LOCKS.find ks.ks_locks (name, None) in
              (k, Option.get (snd k))
          with Not_found ->
            let m = Lwt_mutex.create () in
            let k = (name, Some m) in
              LOCKS.add ks.ks_locks k;
              (k, m)
        in
          tx.outermost_tx.locks <- M.add name k tx.outermost_tx.locks;
          Lwt_mutex.lock mutex

(* string -> string ref -> int -> bool *)
let is_same_value v buf len =
  String.length v = !len && String_util.strneq v 0 !buf 0 !len

let is_column_deleted tx table =
  let deleted_col_table = tx.deleted |> M.find_default M.empty table in
    begin fun ~key_buf ~key_len ~column_buf ~column_len ->
      (* TODO: use substring sets to avoid allocation *)
      if M.is_empty deleted_col_table then
        false
      else begin
        let key =
          if key_len = String.length key_buf then key_buf
          else String.sub key_buf 0 key_len in
        let deleted_cols = M.find_default S.empty key deleted_col_table in
          if S.is_empty deleted_cols then
            false
          else
            let col =
              if column_len = String.length column_buf then column_buf
              else String.sub column_buf 0 column_len
            in S.mem col deleted_cols
      end
    end

(* In [let f = exists_key tx table in ... f key], [f key] returns true iff
 * there's some column with the given key in the table named [table]. *)
let exists_key tx table_name =
  let datum_key = Bytea.create 13 in
  let it = RA.iterator tx.access in
  let buf = ref "" in
  let key_buf = ref "" and key_len = ref 0 in
  let column_buf = ref "" and column_len = ref 0 in
  let key_buf_r = Some key_buf and key_len_r = Some key_len in
  let column_buf_r = Some column_buf and column_len_r = Some column_len in
  let is_column_deleted = is_column_deleted tx table_name in
  let table_r = ref (-1)

  in begin fun key ->
    match find_maybe tx.ks.ks_tables table_name with
        Some table ->
          Datum_encoding.encode_datum_key datum_key tx.ks.ks_id
            ~table ~key ~column:"" ~timestamp:Int64.min_int;
          IT.seek it (Bytea.unsafe_string datum_key) 0 (Bytea.length datum_key);
          if not (IT.valid it) then
            false
          else begin
            let len = IT.fill_key it buf in
            let ok =
              Datum_encoding.decode_datum_key
                ~table_r ~key_buf_r ~key_len_r
                ~column_buf_r ~column_len_r ~timestamp_buf:None
                !buf len
            in
              if not ok then false
              else begin
                (* verify that it's the same table and key, and the column is not
                 * deleted *)
                !table_r = table &&
                is_same_value key key_buf key_len &&
                not (is_column_deleted ~key_buf:key ~key_len:(String.length key)
                       ~column_buf:!column_buf ~column_len:!column_len)
              end
          end
      | None ->
          (* we couldn't translate the table_name to a table_id *)
          false
  end

type 'a fold_result =
    Continue
  | Skip_key
  | Continue_with of 'a
  | Skip_key_with of 'a
  | Finish_fold of 'a

(* fold over cells whose key is in the range
 * { reverse; first = first_key; up_to = up_to_key }
 * with the semantics defined in Data_model
 * *)
let fold_over_data_aux it tx table_name f acc ?(first_column="")
      ~reverse ~first_key ~up_to_key =
  let buf = ref "" in
  let table_r = ref (-1) in
  let key_buf = ref "" and key_len = ref 0 in
  let column_buf = ref "" and column_len = ref 0 in
  let key_buf_r = Some key_buf and key_len_r = Some key_len in
  let column_buf_r = Some column_buf and column_len_r = Some column_len in
  let timestamp_buf = Datum_encoding.make_timestamp_buf () in
  let some_timestamp_buf = Some timestamp_buf in

  let past_limit = match reverse, up_to_key with
      _, None -> (fun () -> false)
    | false, Some k -> (* normal *)
        (fun () ->
           String_util.cmp_substrings
             !key_buf 0 !key_len
             k 0 (String.length k) >= 0)
    | true, Some k -> (* reverse *)
        (fun () ->
           String_util.cmp_substrings
             !key_buf 0 !key_len
             k 0 (String.length k) < 0) in

  let next_key () =
    let s = String.create (!key_len + 1) in
      String.blit !key_buf 0 s 0 !key_len;
      s.[!key_len] <- '\000';
      s in

  let datum_key_buf = Bytea.create 13 in
  let table_id = ref (-2) in

  let rec do_fold_over_data acc =
    if not (IT.valid it) then
      acc
    else begin
      let len = IT.fill_key it buf in
        (* try to decode datum key, check if it's the same KS *)
        if not (Datum_encoding.decode_datum_key
                  ~table_r
                  ~key_buf_r ~key_len_r
                  ~column_buf_r ~column_len_r
                  ~timestamp_buf:some_timestamp_buf
                  !buf len) ||
           Datum_encoding.get_datum_key_keyspace_id !buf <> tx.ks.ks_id
        then
          (* we have hit the end of the keyspace or the data area *)
          acc
        else begin
          (* check that we're still in the table *)
          if !table_r <> !table_id then
            acc
          (* check if we're past the limit *)
          else if past_limit () then
            acc
          else begin
            let r = f acc it ~key_buf:!key_buf ~key_len:!key_len
                     ~column_buf:!column_buf ~column_len:!column_len
                     ~timestamp_buf
            in
              (* seeking to next datum/key/etc *)
              begin match r with
                  Continue | Continue_with _ when not reverse -> IT.next it
                | Continue | Continue_with _ (* reverse *) -> IT.prev it
                | Skip_key | Skip_key_with _ when not reverse ->
                    Datum_encoding.encode_datum_key datum_key_buf tx.ks.ks_id
                      ~table:!table_id ~key:(next_key ())
                      ~column:"" ~timestamp:Int64.min_int;
                    IT.seek it
                      (Bytea.unsafe_string datum_key_buf) 0
                      (Bytea.length datum_key_buf)
                | Skip_key | Skip_key_with _ (* when reverse *) ->
                    (* we jump to the first datum for the current key, then go
                     * one back *)
                    Datum_encoding.encode_datum_key datum_key_buf tx.ks.ks_id
                      ~table:!table_id ~key:(String.sub !key_buf 0 !key_len)
                      ~column:"" ~timestamp:Int64.max_int;
                    IT.seek it
                      (Bytea.unsafe_string datum_key_buf) 0
                      (Bytea.length datum_key_buf);
                    if IT.valid it then IT.prev it
                | Finish_fold _ -> ()
              end;
              match r with
                  Continue | Skip_key -> do_fold_over_data acc
                | Continue_with x | Skip_key_with x -> do_fold_over_data x
                | Finish_fold x -> x
          end
      end
    end

  in match find_maybe tx.ks.ks_tables table_name with
      Some table ->
        table_id := table;
        (* jump to first entry *)
        if not reverse then begin
          let first_datum_key =
            Datum_encoding.encode_datum_key_to_string tx.ks.ks_id ~table
              ~key:(Option.default "" first_key) ~column:first_column
              ~timestamp:Int64.min_int
          in
            IT.seek it first_datum_key 0 (String.length first_datum_key);
        end else begin
          match first_key with
              None ->
                (* we go to the first datum of the next table, then back *)
                let datum_key =
                  Datum_encoding.encode_datum_key_to_string tx.ks.ks_id
                    ~table:(table + 1)
                    ~key:"" ~column:"" ~timestamp:Int64.min_int
                in
                  IT.seek it datum_key 0 (String.length datum_key);
                  if IT.valid it then IT.prev it
            | Some k ->
                (* we go to the datum for the key, then back (it's exclusive) *)
                let datum_key =
                  Datum_encoding.encode_datum_key_to_string tx.ks.ks_id
                    ~table
                    ~key:k ~column:"" ~timestamp:Int64.min_int
                in
                  IT.seek it datum_key 0 (String.length datum_key);
                  if IT.valid it then IT.prev it
        end;
        do_fold_over_data acc
    | None -> (* unknown table *) acc

let fold_over_data_aux
      it tx table f acc ?first_column ~reverse ~first_key ~up_to_key =
  detach_ks_op tx.ks
    (fun () ->
       fold_over_data_aux it tx table f acc ?first_column
         ~reverse ~first_key ~up_to_key)
    ()

let fold_over_data tx table f ?first_column acc ~reverse ~first_key ~up_to_key =
  match tx.iter_pool with
      None ->
        let it = RA.iterator tx.access in
          try_lwt
            fold_over_data_aux it tx table f acc ?first_column
            ~reverse ~first_key ~up_to_key
          finally
            IT.close it;
            return ()
    | Some pool ->
        Lwt_pool.use pool
          (fun it -> fold_over_data_aux it tx table f acc ?first_column
                       ~reverse ~first_key ~up_to_key)

let get_keys_aux init_f map_f fold_f tx table ?(max_keys = max_int) = function
    Keys l ->
      detach_ks_op tx.ks
        (fun () ->
           let exists_key = exists_key tx table in
           let s = S.of_list l in
           let s = S.diff s (M.find_default S.empty table tx.deleted_keys) in
           let s =
             S.filter
               (fun k ->
                  S.mem k (M.find_default S.empty table tx.added_keys) ||
                  exists_key k)
               s
           in map_f s)
        ()

  | Key_range { first; up_to; reverse; } ->
      (* we recover all the keys added in the transaction *)
      let s = M.find_default S.empty table tx.added_keys in
      let s =
        let first, up_to = if reverse then (up_to, first) else (first, up_to) in
          S.subset ?first ?up_to s in
      (* now s contains the added keys in the wanted range *)

      let is_key_deleted =
        let s = M.find_default S.empty table tx.deleted_keys in
          if S.is_empty s then
            (fun buf len -> false)
          else
            (fun buf len -> S.mem (String.sub buf 0 len) s) in

      let is_column_deleted = is_column_deleted tx table in
      let keys_on_disk_kept = ref 0 in

      let fold_datum acc it
            ~key_buf ~key_len
            ~column_buf ~column_len ~timestamp_buf =
        if !keys_on_disk_kept >= max_keys then
          Finish_fold acc
        else begin
          (* check if the key is deleted *)
          if is_key_deleted key_buf key_len then
            Continue
          (* check if the column has been deleted *)
          else if is_column_deleted ~key_buf ~key_len ~column_buf ~column_len then
            Continue
          (* if neither the key nor the column were deleted *)
          else begin
            incr keys_on_disk_kept;
            Skip_key_with (fold_f acc key_buf key_len);
          end
        end
      in
        fold_over_data tx table fold_datum (init_f s)
          ~reverse ~first_key:first ~up_to_key:up_to

let get_keys tx table ?(max_keys = max_int) range =
  lwt keys_in_lexicographic_order =
    get_keys_aux
      (fun s -> s)
      (fun s -> s)
      (fun s key_buf key_len -> S.add (String.sub key_buf 0 key_len) s)
      tx table ~max_keys range >|= S.to_list in
  let all_keys_in_wanted_order =
    match range with
        Key_range { reverse = true; } -> List.rev keys_in_lexicographic_order
      | _ -> keys_in_lexicographic_order
  in return (List.take max_keys all_keys_in_wanted_order)

let count_keys tx table range =
  let s = ref S.empty in
  get_keys_aux
    (fun _s -> s := _s; S.cardinal _s)
    (fun s -> S.cardinal s)
    (fun n key_buf key_len ->
       if S.mem (String.sub key_buf 0 key_len) !s then n
       else (n + 1))
    tx table ~max_keys:max_int range >|= Int64.of_int

let merge_rev cmp l1 l2 =
  let rec loop_merge_rev cmp acc l1 l2 =
    match l1, l2 with
        [], [] -> acc
      | [], x :: tl | x :: tl, [] -> loop_merge_rev cmp (x :: acc) [] tl
      | x1 :: tl1, x2 :: tl2 ->
          match cmp x1 x2 with
              n when n > 0 -> loop_merge_rev cmp (x1 :: acc) tl1 l2
            | n when n < 0 -> loop_merge_rev cmp (x2 :: acc) l1 tl2
            | _ -> loop_merge_rev cmp (x2 :: acc) tl1 tl2
  in loop_merge_rev cmp [] l1 l2

let filter_map_merge cmp map merge ~limit l1 l2 =

  let rec loop_fmm cmp f g acc l1 l2 = function
      n when n > 0 -> begin
        match l1, l2 with
            [], [] -> acc
          | [], x :: tl | x :: tl, [] ->
              (match f x with None -> loop_fmm cmp f g acc [] tl n
                 | Some y -> loop_fmm cmp f g (y :: acc) [] tl (n-1))
          | x1 :: tl1, x2 :: tl2 -> match cmp x1 x2 with
                m when m < 0 ->
                  (match f x1 with None -> loop_fmm cmp f g acc tl1 l2 n
                     | Some y -> loop_fmm cmp f g (y :: acc) tl1 l2 (n-1))
              | m when m > 0 ->
                  (match f x2 with None -> loop_fmm cmp f g acc l1 tl2 n
                     | Some y -> loop_fmm cmp f g (y :: acc) l1 tl2 (n-1))
              | _ ->
                  (match f (g x1 x2) with None -> loop_fmm cmp f g acc tl1 tl2 n
                     | Some y -> loop_fmm cmp f g (y :: acc) tl1 tl2 (n-1))
      end
    | _ -> acc

  in loop_fmm cmp map merge [] l1 l2 limit


let get_slice_aux
      postproc_keydata get_keydata_key keep_columnless_keys
      tx table
      ~max_keys
      ~max_columns
      ~decode_timestamps
      key_range column_range =
  let column_selected = match column_range with
      All_columns -> (fun ~buf ~len -> true)
    | Columns l ->
        if List.length l < 5 then (* TODO: determine threshold *)
          (fun ~buf ~len ->
             List.exists
               (fun s ->
                  String_util.cmp_substrings s 0 (String.length s) buf 0 len = 0)
               l)
        else begin
          let s = S.of_list l in
            (fun ~buf ~len ->
               let c =
                 if len = String.length buf then buf
                 else String.sub buf 0 len
               in S.mem c s)
        end
    | Column_range { first; up_to; reverse; } ->
        let first, up_to = if reverse then (up_to, first) else (first, up_to) in
        let cmp_first = match first with
            None -> (fun ~buf ~len -> true)
          | Some x ->
              (fun ~buf ~len ->
                 String_util.cmp_substrings buf 0 len x 0 (String.length x) >= 0) in
        let cmp_up_to = match up_to with
            None -> (fun ~buf ~len -> true)
          | Some x ->
              (fun ~buf ~len ->
                 String_util.cmp_substrings buf 0 len x 0 (String.length x) < 0)
        in (fun ~buf ~len -> cmp_first ~buf ~len && cmp_up_to ~buf ~len) in

  let is_column_deleted = is_column_deleted tx table

  in match key_range with
    Keys l ->
      let l =
        List.filter
          (fun k -> not (S.mem k (M.find_default S.empty table tx.deleted_keys)))
          (List.sort String.compare l) in
      let reverse = match column_range with
          Column_range { reverse } -> reverse
        | _ -> false in
      lwt key_data_list, _ =
        Lwt_list.fold_left_s
          (fun (key_data_list, keys_so_far) key ->
             if keys_so_far >= max_keys then return (key_data_list, keys_so_far)
             else
               let columns_selected = ref 0 in

               let fold_datum rev_cols it
                     ~key_buf ~key_len ~column_buf ~column_len
                     ~timestamp_buf =
                 (* must skip deleted columns *)
                 if is_column_deleted
                      ~key_buf:key ~key_len:(String.length key)
                      ~column_buf ~column_len then
                   Continue
                 (* also skip non-selected columns *)
                 else if not (column_selected column_buf column_len) then
                   Continue
                 (* otherwise, if the column is not deleted and is selected *)
                 else begin
                   incr columns_selected;
                   let data = IT.get_value it in
                   let col = String.sub column_buf 0 column_len in
                   let timestamp = match decode_timestamps with
                       false -> No_timestamp
                     | true -> Timestamp (Datum_encoding.decode_timestamp timestamp_buf) in
                   let rev_cols = { name = col; data; timestamp; } :: rev_cols in
                     if !columns_selected >= max_columns then
                       Finish_fold rev_cols
                     else Continue_with rev_cols
                 end in

               lwt rev_cols1 =
                 let first_key, up_to_key =
                   if reverse then
                     (Some (key ^ "\000"), Some key)
                   else
                     (Some key, Some (key ^ "\000"))
                 in fold_over_data tx table fold_datum []
                      ~reverse ~first_key ~up_to_key in

               let rev_cols2 =
                 M.fold
                   (fun col data l ->
                      if column_selected col (String.length col) then
                        { name = col; data; timestamp = No_timestamp } :: l
                      else l)
                   (tx.added |>
                    M.find_default M.empty table |> M.find_default M.empty key)
                   [] in

               (* rev_cols2 is G-to-S, want S-to-G if ~reverse *)
               let rev_cols2 = if reverse then List.rev rev_cols2 else rev_cols2 in

               let cmp_cols =
                 if reverse then
                   (fun c1 c2 -> String.compare c2.name c1.name)
                 else
                   (fun c1 c2 -> String.compare c1.name c2.name) in

               let cols = merge_rev cmp_cols rev_cols1 rev_cols2 in

                 match postproc_keydata (key, List.rev cols) with
                   None -> return (key_data_list, keys_so_far)
                 | Some x -> return (x :: key_data_list, keys_so_far + 1))
          ([], 0) l in
      let last_key = match key_data_list with
          x :: _ -> Some (get_keydata_key x)
        | [] -> None
      in return (last_key, List.rev key_data_list)

    | Key_range { first; up_to; reverse; } ->
        let first_pass = ref true in
        let keys_so_far = ref 0 in
        let prev_key = Bytea.create 13 in
        let cols_in_this_key = ref 0 in
        let cols_kept = ref 0 in

        let fold_datum ((key_data_list, key_data) as acc)
              it ~key_buf ~key_len ~column_buf ~column_len ~timestamp_buf =
          let ((key_data_list, key_data) as acc') =
            if String_util.cmp_substrings
                 (Bytea.unsafe_string prev_key) 0 (Bytea.length prev_key)
                 key_buf 0 key_len = 0
            then
              acc
            else begin
              (* new key, increment count and copy the key to prev_key *)
                cols_kept := 0;
                cols_in_this_key := 0;
                incr keys_so_far;
                let acc' =
                  match key_data with
                    _ :: _ ->
                      ((Bytea.contents prev_key, key_data) :: key_data_list, [])
                  | [] ->
                      if not !first_pass && keep_columnless_keys then
                        ((Bytea.contents prev_key, key_data) :: key_data_list, [])
                      else acc
                in
                  Bytea.clear prev_key;
                  Bytea.add_substring prev_key key_buf 0 key_len;
                  acc'
            end

          in
            first_pass := false;
            incr cols_in_this_key;
            (* see if we already have enough columns for this key*)
            if !cols_kept >= max_columns then begin
              (* seeking is very expensive, so we only do it when it looks
               * like the key has got a lot of columns *)
              if !cols_in_this_key - max_columns < 50 then (* FIXME: determine constant *)
                (if acc == acc' then Continue else Continue_with acc')
              else
                (if acc == acc' then Skip_key else Skip_key_with acc')
            end else if not (column_selected column_buf column_len) then begin
              (* skip column *)
              (if acc == acc' then Continue else Continue_with acc')
            end else if is_column_deleted ~key_buf ~key_len ~column_buf ~column_len then
              (if acc == acc' then Continue else Continue_with acc')
            else begin
              let () = incr cols_kept in
              let data = IT.get_value it in
              let col = String.sub column_buf 0 column_len in
              let timestamp = match decode_timestamps with
                  false -> No_timestamp
                | true -> Timestamp (Datum_encoding.decode_timestamp timestamp_buf) in
              let col_data = { name = col; data; timestamp; } in

              let key_data = col_data :: key_data in
              let acc = (key_data_list, key_data) in
                (* early termination w/o iterating over further keys if we
                 * already have enough  *)
                (* we cannot use >= because further columns for the same key
                 * could follow *)
                if !keys_so_far > max_keys then
                  Finish_fold acc
                (* alternatively, if we're in the last key, and have enough
                 * columns, finish *)
                else if !keys_so_far = max_keys && !cols_kept >= max_columns then
                  Finish_fold acc
                else
                  Continue_with acc
            end
        in

        lwt rev_key_data_list1 =
          lwt l1, d1 =
            fold_over_data tx table fold_datum ([], [])
              ~reverse ~first_key:first ~up_to_key:up_to
          in match d1 with
              [] when not !first_pass && keep_columnless_keys ->
                return ((Bytea.contents prev_key, d1) :: l1)
            | [] -> return l1
            | cols -> return ((Bytea.contents prev_key, cols) :: l1) in

        let rev_key_data_list2 =
          M.fold
            (fun key key_data_in_mem l ->
               let cols =
                 M.fold
                   (fun col data l ->
                      if column_selected col (String.length col) then
                        let col_data = { name = col; data; timestamp = No_timestamp} in
                          col_data :: l
                      else l)
                   key_data_in_mem
                   []
               in (key, if reverse then List.rev cols else cols) :: l)
            (let first, up_to =
               if reverse then (up_to, first) else (first, up_to)
             in M.submap ?first ?up_to (M.find_default M.empty table tx.added))
            [] in

        (* if the key range was reverse,
         * rev_key_data_list1 : (key * column list) list
         * holds the keys in normal order (smaller to greater),
         * otherwise in reverse order (greater to smaller).
         * rev_key_data_list2 is always in reverse order.
         * *)

        let key_data_list1 = List.rev rev_key_data_list1 in
        let key_data_list2 =
          if not reverse then
            List.rev rev_key_data_list2
          else rev_key_data_list2 in

        (* now both are in the desired order *)

        let cmp_keys =
          if reverse then
            (fun (k1, _) (k2, _) -> - String.compare k1 k2)
          else
            (fun (k1, _) (k2, _) -> String.compare k1 k2) in

        let cmp_cols c1 c2 = String.compare c1.name c2.name in

        let merge =
          if reverse then
            (* both are S-to-G *)
            (fun cols1 cols2 -> List.merge cmp_cols cols1 cols2)
          else (* both are G-to-S *)
            (fun cols1 cols2 -> merge_rev cmp_cols cols1 cols2) in

        let rev_key_data_list =
          filter_map_merge
            cmp_keys
            postproc_keydata
            (fun (k, rev_cols1) (_, rev_cols2) ->
               let cols = merge rev_cols1 rev_cols2 in
                 (k, List.rev cols))
            ~limit:max_keys key_data_list1 key_data_list2 in

        let last_key = match rev_key_data_list with
            x :: _ -> Some (get_keydata_key x)
          | [] -> None
        in return (last_key, List.rev rev_key_data_list)

let get_slice tx table
      ?(max_keys = max_int) ?(max_columns = max_int)
      ?(decode_timestamps = false) key_range column_range =
  let postproc_keydata (key, rev_cols) =
    match rev_cols with
      | _ :: _ as l when max_columns > 0 ->
          let columns = List.take max_columns (List.rev l) in
          let last_column = (List.last columns).name in
            Some ({ key; last_column; columns; })
      | _ -> None in

  let get_keydata_key { key; _ } = key

  in get_slice_aux postproc_keydata get_keydata_key false tx table
      ~max_keys ~max_columns ~decode_timestamps key_range column_range

let get_slice_values tx table
      ?(max_keys = max_int) key_range columns =
  let postproc_keydata (key, cols) =
    let l =
      List.map
        (fun column ->
           try Some (List.find (fun c -> c.name = column) cols).data
           with Not_found -> None)
        columns
    in Some (key, l) in

  let get_keydata_key (key, _) = key

  in get_slice_aux postproc_keydata get_keydata_key true tx table
       ~max_keys ~max_columns:(List.length columns)
       ~decode_timestamps:false key_range (Columns columns)

let get_columns tx table ?(max_columns = max_int) ?decode_timestamps
                key column_range =
  match_lwt
    get_slice tx table ~max_columns ?decode_timestamps
      (Keys [key]) column_range
  with
    | (_, { last_column = last_column;
            columns = ((_ :: _ as columns)) } :: _ ) ->
        return (Some (last_column, columns))
    | _ -> return None

let get_column_values tx table key columns =
  match_lwt get_slice_values tx table (Keys [key]) columns with
    | (_, (_, l):: _) -> return l
    | _ -> assert false

let get_column tx table key column_name =
  match_lwt
    get_columns tx table key ~decode_timestamps:true (Columns [column_name])
  with
      Some (_, c :: _) -> return (Some (c.data, c.timestamp))
    | _ -> return None

let put_columns tx table key columns =
  tx.added_keys <- M.modify (S.add key) S.empty table tx.added_keys;
  tx.deleted_keys <- M.modify_if_found (S.remove key) table tx.deleted_keys;
  tx.deleted <-
    M.modify_if_found
      (fun m ->
         if M.is_empty m then m
         else
           M.modify_if_found
             (fun s ->
                List.fold_left (fun s c -> S.remove c.name s) s columns)
             key m)
      table tx.deleted;
  tx.added <-
    M.modify
      (fun m ->
         (M.modify
            (fun m ->
               List.fold_left
                 (fun m c -> M.add c.name c.data m)
                 m columns)
            M.empty key m))
      M.empty table tx.added;
  return ()

let delete_columns tx table key cols =
  tx.added <-
    M.modify_if_found
      (fun m ->
         (M.modify
             (fun m -> List.fold_left (fun m c -> M.remove c m) m cols)
             M.empty key m))
      table tx.added;
  if M.is_empty (M.find_default M.empty table tx.added |>
                 M.find_default M.empty key) then
    tx.added_keys <- M.modify_if_found (S.remove key) table tx.added_keys;
  tx.deleted <-
    M.modify
      (fun m ->
         M.modify
           (fun s -> List.fold_left (fun s c -> S.add c s) s cols)
           S.empty key m)
      M.empty table tx.deleted;
  return ()

let delete_key tx table key =
  match_lwt
    get_columns tx table ~max_columns:max_int ~decode_timestamps:false
      key All_columns
  with
      None -> return ()
    | Some (_, columns) ->
        lwt () = delete_columns tx table key
                   (List.map (fun c -> c.name) columns)
        in
          tx.deleted_keys <- M.modify (S.add key) S.empty table tx.deleted_keys;
          return ()

let dump tx ?(format = 0) ?only_tables ?offset () =
  let open Backup in
  let max_chunk = 65536 in
  let module ENC = (val Backup.encoder format : Backup.ENCODER) in
  let enc = ENC.make () in
  lwt pending_tables = match offset with
      Some c -> return c.bc_remaining_tables
    | None -> (* FIXME: should use the tx's iterator to list the tables *)
        match only_tables with
            None -> list_tables tx.ks
          | Some tables -> return tables in
  let curr_key, curr_col = match offset with
      Some c -> Some c.bc_key, Some c.bc_column
    | None -> None, None in

  let next_key = ref "" in
  let next_col = ref "" in

  let got_enough_data () = ENC.approximate_size enc > max_chunk in
  let value_buf = ref "" in

  let fold_datum curr_table it
        ~key_buf ~key_len ~column_buf ~column_len ~timestamp_buf =
    if got_enough_data () then begin
      next_key := String.sub key_buf 0 key_len;
      next_col := String.sub column_buf 0 column_len;
      Finish_fold curr_table
    end else begin
      ENC.add_datum enc curr_table it
        ~key_buf ~key_len ~column_buf ~column_len ~timestamp_buf ~value_buf;
      Continue
    end in

  let rec collect_data ~curr_key ~curr_col = function
      [] ->
        if ENC.is_empty enc then return None
        else return (Some (Bytea.contents (ENC.finish enc), None))
    | curr_table :: tl as tables ->
        lwt _ =
          fold_over_data tx curr_table fold_datum curr_table
            ~reverse:false
            ~first_key:curr_key ?first_column:curr_col ~up_to_key:None
        in
          if got_enough_data () then begin
            let cursor =
              { bc_remaining_tables = tables; bc_key = !next_key;
                bc_column = !next_col; }
            in return (Some (Bytea.contents (ENC.finish enc), Some cursor))

          (* we don't have enough data yet, move to next table *)
          end else begin
            collect_data ~curr_key:None ~curr_col:None tl
          end

  in collect_data ~curr_key ~curr_col pending_tables

let load tx data =
  let wb = Lazy.force tx.backup_writebatch in
  let enc_datum_key datum_key ~table:table_name ~key ~column ~timestamp =
    match find_maybe tx.ks.ks_tables table_name with
        Some table -> Datum_encoding.encode_datum_key datum_key tx.ks.ks_id
                        ~table ~key ~column ~timestamp
      | None ->
          (* register table first *)
          let table =
            register_new_table_and_add_to_writebatch tx.ks wb table_name
          in Datum_encoding.encode_datum_key datum_key tx.ks.ks_id
               ~table ~key ~column ~timestamp in

  let ok = Backup.load enc_datum_key wb data in
    if not ok then
      return false
    else begin
      tx.backup_writebatch_size <- tx.backup_writebatch_size + String.length data;
      begin
        (* we check if there's enough data in this batch, and write it if so *)
        if tx.backup_writebatch_size < 40_000_000 then
          (* FIXME: allow to set this constant somewhere *)
          return ()
        else begin
          tx.backup_writebatch <- lazy (L.Batch.make ());
          tx.backup_writebatch_size <- 0;
          detach_ks_op tx.ks (L.Batch.write ~sync:true tx.ks.ks_db.db) wb
        end
      end >>
      return true
    end

let cursor_of_string = Backup.cursor_of_string
let string_of_cursor = Backup.string_of_cursor

let with_transaction ks f =
  match Lwt.get tx_key with
      None -> read_committed_transaction ks f
    | Some tx -> f tx

let get_keys ks table ?max_keys key_range =
  with_transaction ks (fun tx -> get_keys tx table ?max_keys key_range)

let count_keys ks table key_range =
  with_transaction ks (fun tx -> count_keys tx table key_range)

let get_slice ks table ?max_keys ?max_columns ?decode_timestamps
      key_range column_range =
  with_transaction ks
    (fun tx ->
       get_slice tx table ?max_keys ?max_columns ?decode_timestamps
         key_range column_range)

let get_slice_values ks table ?max_keys key_range columns =
  with_transaction ks
    (fun tx -> get_slice_values tx table ?max_keys key_range columns)

let get_columns ks table ?max_columns ?decode_timestamps key column_range =
  with_transaction ks
    (fun tx -> get_columns tx table ?max_columns ?decode_timestamps key column_range)

let get_column_values ks table key columns =
  with_transaction ks
    (fun tx -> get_column_values tx table key columns)

let get_column ks table key column =
  with_transaction ks (fun tx -> get_column tx table key column)

let put_columns ks table key columns =
  with_transaction ks (fun tx -> put_columns tx table key columns)

let delete_columns ks table key columns =
  with_transaction ks (fun tx -> delete_columns tx table key columns)

let delete_key ks table key =
  with_transaction ks (fun tx -> delete_key tx table key)

let dump ks ?format ?only_tables ?offset () =
  with_transaction ks (fun tx -> dump tx ?format ?only_tables ?offset ())

let load ks data = with_transaction ks (fun tx -> load tx data)

let read_committed_transaction ks f =
  read_committed_transaction ks (fun _ -> f ks)

let repeatable_read_transaction ks f =
  repeatable_read_transaction ks (fun _ -> f ks)
