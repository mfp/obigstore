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

open Printf
open Lwt
open Obs_data_model

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
    Obs_string_util.cmp_substrings x 0 (String.length x) y 0 (String.length y)
end

module LOCKS =
  Weak.Make(struct
              type t = (string * Obs_shared_mutex.t option)
              let hash (s, _) = Hashtbl.hash s
              let equal (s1, _) (s2, _) = String.compare s1 s2 = 0
            end)

let make_iter_pool db =
  Lwt_pool.create 100 (* FIXME: which limit? *)
    (fun () -> return (L.iterator db))

module WRITEBATCH : sig
  type t
  type tx
  val make : L.db -> L.iterator Lwt_pool.t ref -> flush_period:float -> t
  val close : t -> unit Lwt.t
  val set_period : t -> float -> unit
  val perform : t -> (tx -> unit Lwt.t) -> unit Lwt.t Lwt.t

  val delete_substring : tx -> string -> int -> int -> unit
  val put_substring : tx -> string -> int -> int -> string -> int -> int -> unit
end =
struct
  type t =
      { db : L.db;
        mutable wb : L.writebatch;
        mutable dirty : bool;
        mutable flush_period : float;
        mutable closed : bool;
        mutex : Lwt_mutex.t;
        mutable sync_wait : unit Lwt.t * unit Lwt.u;
        (* TODO: allow shared mutex locking for [perform], only
         * the actual sync needs to be exclusive *)
        mutable wait_last_sync : unit Lwt.t * unit Lwt.u;
        (* used to wait for last sync before close *)
        iter_pool : L.iterator Lwt_pool.t ref;
        (* will be overwritten after an update *)
      }

  type tx = { t : t; mutable valid : bool; }

  let make db iter_pool ~flush_period =
    let t =
      { db; wb = L.Batch.make (); dirty = false; closed = false;
        mutex = Lwt_mutex.create (); sync_wait = Lwt.wait ();
        flush_period; wait_last_sync = Lwt.wait ();
        iter_pool;
      }
    in
      ignore begin try_lwt
        let rec writebatch_loop () =
          Lwt_unix.sleep t.flush_period >>
          if t.closed && not t.dirty then begin
            Lwt.wakeup (snd t.wait_last_sync) ();
            return ()
          end else if not t.dirty then
            writebatch_loop ()
          else begin
            Lwt_mutex.with_lock t.mutex
              (fun () ->
                 lwt () = Lwt_preemptive.detach (L.Batch.write ~sync:true t.db) t.wb in
                 let u = snd t.sync_wait in
                 let u' = snd t.wait_last_sync in
                   iter_pool := make_iter_pool t.db;
                   t.dirty <- false;
                   t.wb <- L.Batch.make ();
                   t.sync_wait <- Lwt.wait ();
                   t.wait_last_sync <- Lwt.wait ();
                   Lwt.wakeup u ();
                   Lwt.wakeup u' ();
                   return ())
          end >>
          if t.closed then begin
            Lwt.wakeup (snd t.wait_last_sync) ();
            return ()
          end else writebatch_loop ()
        in writebatch_loop ()
      with e ->
        eprintf "WRITEBATCH error: %s\n%!" (Printexc.to_string e);
        return ()
      end;
      t

  let close t =
    t.closed <- true;
    if t.dirty then fst t.wait_last_sync else return ()

  let set_period t dt = t.flush_period <- (max 0.001 dt)

  let perform t f =
    if t.closed then
      raise_lwt (Failure "WRITEBATCH.perform: closed writebatch")
    else
      Lwt_mutex.with_lock t.mutex
        (fun () ->
           let waiter = fst t.sync_wait in
           let tx = { t; valid = true } in
             try_lwt
               f tx >>
               return waiter
             finally
               tx.valid <- false;
               return ())

  let delete_substring tx s off len =
    if not tx.valid then
      failwith "WRITEBATCH.delete_substring on expired transaction";
    tx.t.dirty <- true;
    L.Batch.delete_substring tx.t.wb s off len

  let put_substring tx k o1 l1 v o2 l2 =
    if not tx.valid then
      failwith "WRITEBATCH.put_substring on expired transaction";
    tx.t.dirty <- true;
    L.Batch.put_substring tx.t.wb k o1 l1 v o2 l2
end

module Miniregion : sig
  type 'a t

  val make : 'a -> 'a t
  val use : 'a t -> ('a -> 'b Lwt.t) -> 'b Lwt.t
  val update_value : 'a t -> ('a -> 'a Lwt.t) -> unit Lwt.t
end =
struct
  type 'a t =
      {
        mutable v : 'a;
        mutex : Obs_shared_mutex.t;
      }

  let make v = { v; mutex = Obs_shared_mutex.create (); }

  let use t f =
    Obs_shared_mutex.with_lock ~shared:true t.mutex (fun () -> f t.v)

  let update_value t f =
    Obs_shared_mutex.with_lock ~shared:false t.mutex
      (fun () ->
         lwt new_v = f t.v in
           t.v <- new_v;
           return ())
end

type db =
    {
      basedir : string;
      db : L.db Miniregion.t;
      keyspaces : (string, keyspace) Hashtbl.t;
      mutable use_thread_pool : bool;
      load_stats : Obs_load_stats.t;
      mutable writebatch : WRITEBATCH.t;
      db_iter_pool : L.iterator Lwt_pool.t ref;
      (* We use an iterator pool to reuse iterators for read committed
       * transactions whenever possible. After an update, the db_iter_pool is
       * replaced by a new one (so that only new iterators are used). *)

      reopen : unit -> L.db;
    }

and keyspace =
  {
    ks_db : db; ks_name : string; ks_id : int;
    ks_tables : (string, int) Hashtbl.t;
    ks_rev_tables : (int, string) Hashtbl.t;
    ks_locks : LOCKS.t;
  }

type backup_cursor = Obs_backup.cursor

let (|>) x f = f x

let find_maybe h k = try Some (Hashtbl.find h k) with Not_found -> None

let read_keyspace_tables lldb keyspace =
  let module T = Obs_datum_encoding.Keyspace_tables in
  let h = Hashtbl.create 13 in
    L.iter_from
      (fun k v ->
         match T.decode_ks_table_key k with
             None -> false
           | Some (ks, table) when ks <> keyspace -> false
           | Some (_, table) -> Hashtbl.add h table (int_of_string v);
                                true)
      lldb
      (T.ks_table_table_prefix_for_ks keyspace);
    h

let read_keyspaces db lldb =
  let h = Hashtbl.create 13 in
    L.iter_from
      (fun k v ->
         match Obs_datum_encoding.decode_keyspace_table_name k with
             None -> false
           | Some keyspace_name ->
               let ks_db = db in
               let ks_name = keyspace_name in
               let ks_id = int_of_string v in
               let ks_tables = read_keyspace_tables lldb ks_name in
               let ks_rev_tables = Hashtbl.create (Hashtbl.length ks_tables) in
               let ks_locks = LOCKS.create 13 in
                 Hashtbl.iter
                   (fun k v -> Hashtbl.add ks_rev_tables v k)
                   ks_tables;
                 Hashtbl.add h keyspace_name
                   { ks_db; ks_id; ks_name; ks_tables; ks_rev_tables; ks_locks; };
                 true)
      lldb
      Obs_datum_encoding.keyspace_table_prefix;
    h

let close_db t =
  Miniregion.use t.db
    (fun lldb ->
       lwt () = WRITEBATCH.close t.writebatch in
         L.close lldb;
         return ())

let open_db
      ?(write_buffer_size = 4 * 1024 * 1024)
      ?(block_size = 4096)
      ?(max_open_files = 1000)
      ?(group_commit_period = 0.002) basedir =
  let lldb = L.open_db
             ~write_buffer_size ~block_size ~max_open_files
             ~comparator:Obs_datum_encoding.custom_comparator basedir in
  let group_commit_period = max group_commit_period 0.001 in
  let db_iter_pool = ref (make_iter_pool lldb) in
  let writebatch =
    WRITEBATCH.make lldb db_iter_pool ~flush_period:group_commit_period
  in
    (* ensure we have a end_of_db record *)
    if not (L.mem lldb Obs_datum_encoding.end_of_db_key) then
      L.put ~sync:true lldb Obs_datum_encoding.end_of_db_key (String.make 8 '\000');
    let rec db =
      { basedir; db = Miniregion.make lldb;
        keyspaces = Hashtbl.create 13;
        use_thread_pool = false;
        load_stats = Obs_load_stats.make [1; 60; 300; 900];
        writebatch; db_iter_pool; reopen;
      }
    and reopen () =
      let lldb = L.open_db
                    ~write_buffer_size ~block_size ~max_open_files
                    ~comparator:Obs_datum_encoding.custom_comparator
                    basedir
      in
        db.db_iter_pool := make_iter_pool lldb;
        db.writebatch <- WRITEBATCH.make lldb db.db_iter_pool
                           ~flush_period:group_commit_period;
        lldb in
    let keyspaces = read_keyspaces db lldb in
      Gc.finalise (fun db -> ignore (close_db db)) db;
      Hashtbl.iter (Hashtbl.add db.keyspaces) keyspaces;
      db

let reset_iter_pool t =
  Miniregion.use t.db
    (fun lldb -> t.db_iter_pool := make_iter_pool lldb; return ())

let use_thread_pool db v = db.use_thread_pool <- v

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
      Miniregion.use t.db
        (fun lldb ->
           L.put ~sync:true lldb (Obs_datum_encoding.keyspace_table_key ks_name)
             (string_of_int ks_id);
           let ks =
             { ks_db = t; ks_id; ks_name;
               ks_tables = Hashtbl.create 13;
               ks_rev_tables = Hashtbl.create 13;
               ks_locks = LOCKS.create 31;
             }
           in
             Hashtbl.add t.keyspaces ks_name ks;
             return ks)

let get_keyspace t ks_name =
  return begin try
    Some (Hashtbl.find t.keyspaces ks_name)
  with Not_found -> None end

let keyspace_name ks = ks.ks_name

let keyspace_id ks = ks.ks_id

let it_next ks it =
  Obs_load_stats.record_near_seeks ks.ks_db.load_stats 1;
  IT.next it

let it_prev ks it =
  Obs_load_stats.record_near_seeks ks.ks_db.load_stats 1;
  IT.prev it

let it_seek ks it s off len =
  Obs_load_stats.record_seeks ks.ks_db.load_stats 1;
  IT.seek it s off len

let list_tables ks =
  let table_r = ref (-1) in

  let jump_to_next_table it =
    match !table_r with
        -1 ->
          let datum_key =
            Obs_datum_encoding.encode_datum_key_to_string ks.ks_id
              ~table:0 ~key:"" ~column:"" ~timestamp:Int64.min_int
          in it_seek ks it datum_key 0 (String.length datum_key);
      | table ->
          let datum_key =
            Obs_datum_encoding.encode_table_successor_to_string ks.ks_id table
          in
            it_seek ks it datum_key 0 (String.length datum_key); in

  let rec collect_tables it acc =
    jump_to_next_table it;
    if not (IT.valid it) then acc
    else begin
      let k = IT.get_key it in
        if not (Obs_datum_encoding.decode_datum_key
                  ~table_r
                  ~key_buf_r:None ~key_len_r:None
                  ~column_buf_r:None ~column_len_r:None
                  ~timestamp_buf:None
                  k (String.length k)) ||
           Obs_datum_encoding.get_datum_key_keyspace_id k <> ks.ks_id
        then
          (* we have hit the end of the keyspace or the data area *)
          acc
        else begin
          let acc = match find_maybe ks.ks_rev_tables !table_r with
              Some table -> table :: acc
            | None -> acc
          in collect_tables it acc
        end
    end

  in
    Miniregion.use ks.ks_db.db
      (fun lldb ->
         let it = L.iterator lldb in
           detach_ks_op ks (collect_tables it) [] >|= List.sort String.compare)

let table_size_on_disk ks table_name =
  match find_maybe ks.ks_tables table_name with
      None -> return 0L
    | Some table ->
        let _from =
          Obs_datum_encoding.encode_datum_key_to_string ks.ks_id
           ~table ~key:"" ~column:"" ~timestamp:Int64.min_int in
        let _to =
          Obs_datum_encoding.encode_datum_key_to_string ks.ks_id
            ~table ~key:"\255\255\255\255\255\255" ~column:"\255\255\255\255\255\255"
            ~timestamp:Int64.zero
        in
          Miniregion.use ks.ks_db.db
            (fun lldb ->
               detach_ks_op ks (L.get_approximate_size lldb _from) _to)

let key_range_size_on_disk ks ?first ?up_to table_name =
  match find_maybe ks.ks_tables table_name with
      None -> return 0L
    | Some table ->
        let _from =
          Obs_datum_encoding.encode_datum_key_to_string ks.ks_id
            ~table ~key:(Option.default "" first) ~column:""
            ~timestamp:Int64.min_int in
        let _to =
          Obs_datum_encoding.encode_datum_key_to_string ks.ks_id
            ~table
            ~key:(Option.default "\255\255\255\255\255\255" up_to)
            ~column:"\255\255\255\255\255\255"
            ~timestamp:Int64.zero
        in
          Miniregion.use ks.ks_db.db
            (fun lldb -> detach_ks_op ks (L.get_approximate_size lldb _from) _to)

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
      repeatable_read : bool;
      iter_pool : L.iterator Lwt_pool.t ref;
      ks : keyspace;
      mutable backup_writebatch : L.writebatch Lazy.t;
      (* approximate size of data written in current backup_writebatch *)
      mutable backup_writebatch_size : int;
      outermost_tx : transaction;
      mutable locks : (string * Obs_shared_mutex.t option) M.t;
      dump_buffer : Obs_bytea.t;
    }

let tx_key = Lwt.new_key ()

let register_new_table_and_add_to_writebatch_aux put_substring ks wb table_name =
  let last = Hashtbl.fold (fun _ idx m -> max m idx) ks.ks_tables 0 in
  (* TODO: find first free one (if existent) LT last *)
  let table = last + 1 in
  let k = Obs_datum_encoding.Keyspace_tables.ks_table_table_key ks.ks_name table_name in
  let v = string_of_int table in
    put_substring wb
      k 0 (String.length k) v 0 (String.length v);
    Hashtbl.add ks.ks_tables table_name table;
    Hashtbl.add ks.ks_rev_tables table table_name;
    table

let register_new_table_and_add_to_writebatch =
  register_new_table_and_add_to_writebatch_aux L.Batch.put_substring

let register_new_table_and_add_to_writebatch' =
  register_new_table_and_add_to_writebatch_aux WRITEBATCH.put_substring

let rec transaction_aux with_iter_pool ks f =
  match Lwt.get tx_key with
    | None ->
        with_iter_pool ks.ks_db None begin fun ~iter_pool ~repeatable_read ->
          let rec tx =
            { added_keys = M.empty; deleted_keys = M.empty;
              added = M.empty; deleted = M.empty;
              repeatable_read; iter_pool; ks;
              backup_writebatch = lazy (L.Batch.make ());
              backup_writebatch_size = 0;
              outermost_tx = tx; locks = M.empty;
              dump_buffer = Obs_bytea.create 16;
            } in
          try_lwt
            lwt y = Lwt.with_value tx_key (Some tx) (fun () -> f tx) in
              commit_outermost_transaction ks tx >>
              return y
          finally
            (* release locks *)
            M.iter (fun _ (_, m) -> Option.may Obs_shared_mutex.unlock m) tx.locks;
            return ()
        end
    | Some parent_tx ->
        with_iter_pool
          ks.ks_db (Some parent_tx) begin fun ~iter_pool ~repeatable_read ->
            let tx = { parent_tx with iter_pool; repeatable_read; } in
            lwt y = Lwt.with_value tx_key (Some tx) (fun () -> f tx) in
              parent_tx.deleted_keys <- tx.deleted_keys;
              parent_tx.added <- tx.added;
              parent_tx.deleted <- tx.deleted;
              parent_tx.added_keys <- tx.added_keys;
              return y
          end

and commit_outermost_transaction ks tx =
  (* early termination for read-only or NOP txs if:
   * * backup_writebatch has not be forced (i.e., not used for bulk
   *   load)
   * * no columns have been added / deleted *)
  if not (Lazy.lazy_is_val tx.backup_writebatch) &&
     M.is_empty tx.deleted && M.is_empty tx.added
  then return ()

  else begin
    (* normal procedure *)

    (* sync backup writebatch if needed *)
    begin if not (Lazy.lazy_is_val tx.backup_writebatch) then
      return ()
    else
      let b = Lazy.force tx.backup_writebatch in
        Obs_load_stats.record_writes tx.ks.ks_db.load_stats 1;
        Miniregion.use ks.ks_db.db
          (fun lldb ->
             detach_ks_op ks (L.Batch.write ~sync:true lldb) b >>
             reset_iter_pool ks.ks_db >>
             return ())
    end >>

    (* write normal data and wait for group commit sync *)
    let datum_key = Obs_bytea.create 13 in
    let timestamp = Int64.of_float (Unix.gettimeofday () *. 1e6) in
    let bytes_written = ref 0 in
    let cols_written = ref 0 in

    lwt wait_sync =
      WRITEBATCH.perform tx.ks.ks_db.writebatch begin fun b ->
        (* TODO: should iterate in Lwt monad so we can yield every once in a
         * while *)
        M.iter
          (fun table_name m ->
             match find_maybe ks.ks_tables table_name with
                 Some table ->
                   M.iter
                     (fun key s ->
                        S.iter
                          (fun column ->
                             Obs_datum_encoding.encode_datum_key datum_key ks.ks_id
                               ~table ~key ~column ~timestamp;
                             let len = Obs_bytea.length datum_key in
                               incr cols_written;
                               bytes_written := !bytes_written + len;
                               WRITEBATCH.delete_substring b
                                 (Obs_bytea.unsafe_string datum_key) 0
                                 len)
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
                   register_new_table_and_add_to_writebatch' ks b table_name
             in M.iter
               (fun key m ->
                  M.iter
                    (fun column v ->
                       (* FIXME: should save timestamp provided in
                        * put_columns and use it here if it wasn't
                        * Auto_timestamp *)
                       Obs_datum_encoding.encode_datum_key datum_key ks.ks_id
                         ~table ~key ~column ~timestamp;
                       let klen = Obs_bytea.length datum_key in
                       let vlen = String.length v in
                         incr cols_written;
                         bytes_written := !bytes_written + klen + vlen;
                         WRITEBATCH.put_substring b
                           (Obs_bytea.unsafe_string datum_key) 0 klen
                           v 0 vlen)
                    m)
               m)
          tx.added;
        return ()
      end in
    let stats = tx.ks.ks_db.load_stats in
    lwt () = wait_sync in
      Obs_load_stats.record_writes stats 1;
      Obs_load_stats.record_bytes_wr stats !bytes_written;
      Obs_load_stats.record_cols_wr stats !cols_written;
      return ()
  end

let read_committed_transaction ks f =
  Miniregion.use ks.ks_db.db
    (fun _ ->
       transaction_aux
         (fun db parent_tx f -> match parent_tx with
              None -> f ~iter_pool:db.db_iter_pool ~repeatable_read:false
            | Some tx -> f ~iter_pool:tx.iter_pool ~repeatable_read:false)
         ks f)

let repeatable_read_transaction ks f =
  Miniregion.use ks.ks_db.db
    (fun lldb ->
       transaction_aux
         (fun db parent_tx f -> match parent_tx with
              Some { repeatable_read = true; iter_pool; _} ->
                f ~iter_pool ~repeatable_read:true
            | _ ->
                let access = lazy (L.Snapshot.read_access (L.Snapshot.make lldb)) in
                let pool = Lwt_pool.create 1000 (* FIXME: which limit? *)
                             (fun () -> return (RA.iterator (Lazy.force access)))
                in
                  f ~iter_pool:(ref pool) ~repeatable_read:true)
         ks f)

let lock_one ks ~shared name =
  match Lwt.get tx_key with
      None -> return ()
    | Some tx ->
      (* we use tx.outermost_tx.locks instead of tx.locks because locks are
       * always associated to the outermost transaction! *)
      if M.mem name tx.outermost_tx.locks then return ()
      else
        let ks = tx.ks in
        let k, mutex =
          try
            let k = LOCKS.find ks.ks_locks (name, None) in
              (k, Option.get (snd k))
          with Not_found ->
            let m = Obs_shared_mutex.create () in
            let k = (name, Some m) in
              LOCKS.add ks.ks_locks k;
              (k, m)
        in
          tx.outermost_tx.locks <- M.add name k tx.outermost_tx.locks;
          Obs_shared_mutex.lock ~shared mutex

let lock ks ~shared names = Lwt_list.iter_s (lock_one ks ~shared) names

(* string -> string ref -> int -> bool *)
let is_same_value v buf len =
  String.length v = !len && Obs_string_util.strneq v 0 !buf 0 !len

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
let with_exists_key tx table_name f =
  let datum_key = Obs_bytea.create 13 in
  let buf = ref "" in
  let key_buf = ref "" and key_len = ref 0 in
  let column_buf = ref "" and column_len = ref 0 in
  let key_buf_r = Some key_buf and key_len_r = Some key_len in
  let column_buf_r = Some column_buf and column_len_r = Some column_len in
  let is_column_deleted = is_column_deleted tx table_name in
  let table_r = ref (-1) in

  let exists_f it = begin fun key -> match find_maybe tx.ks.ks_tables table_name with
        Some table ->
          Obs_datum_encoding.encode_datum_key datum_key tx.ks.ks_id
            ~table ~key ~column:"" ~timestamp:Int64.min_int;
          it_seek tx.ks it (Obs_bytea.unsafe_string datum_key) 0 (Obs_bytea.length datum_key);
          if not (IT.valid it) then
            false
          else begin
            let len = IT.fill_key it buf in
            let ok =
              Obs_datum_encoding.decode_datum_key
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
  in
    Lwt_pool.use !(tx.iter_pool) (fun it -> f (exists_f it))

type 'a fold_result =
    Continue
  | Skip_key
  | Continue_with of 'a
  | Skip_key_with of 'a
  | Finish_fold of 'a

(* advance the iterator using the supplied function until we find a new key
 * within the same table, reach the end of the data area or exceed an
 * iteration count *)
let advance_until_next_key ~reverse next it
      ks_id table_id
      ~prev_key_buf ~prev_key_len
      ~table_r ~key_buf ~key_len ~key_buf_r ~key_len_r buf =
  let finished = ref false in
  let n = ref 0 in
  let sign = if reverse then -1 else 1 in
  (* FIXME: constant should be determined based on relative speed of
   * IT.next/prev and IT.seek, which depends on DB size (num cells) *)
  let limit = 25 in
    while !n < limit && not !finished && !table_r = table_id do
      next it;
      if not (IT.valid it) then finished := true
      else begin
        let len = IT.fill_key it buf in
          if not (Obs_datum_encoding.decode_datum_key
                    ~table_r ~key_buf_r ~key_len_r
                    ~column_buf_r:None ~column_len_r:None
                    ~timestamp_buf:None !buf len) ||
             Obs_datum_encoding.get_datum_key_keyspace_id !buf <> ks_id ||
             (sign *
              Obs_string_util.cmp_substrings
                prev_key_buf 0 prev_key_len !key_buf 0 !key_len) < 0
          then
            finished := true
      end;
      incr n;
    done;
    if !n >= limit && not !finished then `Need_seek
    else `Finished

(* fold over cells whose key is in the range
 * { reverse; first = first_key; up_to = up_to_key }
 * with the semantics defined in Obs_data_model
 * *)
let fold_over_data_aux it tx table_name f acc ?(first_column="")
      ~reverse ~first_key ~up_to_key =
  let buf = ref "" in
  let table_r = ref (-1) in
  let key_buf = ref "" and key_len = ref 0 in
  let column_buf = ref "" and column_len = ref 0 in
  let key_buf_r = Some key_buf and key_len_r = Some key_len in
  let column_buf_r = Some column_buf and column_len_r = Some column_len in
  let timestamp_buf = Obs_datum_encoding.make_timestamp_buf () in
  let some_timestamp_buf = Some timestamp_buf in
  let next_key_buf = ref "" and next_key_len = ref 0 in
  let next_key_buf_r = Some next_key_buf and next_key_len_r = Some next_key_len in

  let past_limit = match reverse, up_to_key with
      _, None -> (fun () -> false)
    | false, Some k -> (* normal *)
        (fun () ->
           Obs_string_util.cmp_substrings
             !key_buf 0 !key_len
             k 0 (String.length k) >= 0)
    | true, Some k -> (* reverse *)
        (fun () ->
           Obs_string_util.cmp_substrings
             !key_buf 0 !key_len
             k 0 (String.length k) < 0) in

  let next_key () =
    let s = String.create (!key_len + 1) in
      String.blit !key_buf 0 s 0 !key_len;
      s.[!key_len] <- '\000';
      s in

  let datum_key_buf = Obs_bytea.create 13 in
  let table_id = ref (-2) in

  let rec do_fold_over_data acc =
    if not (IT.valid it) then
      acc
    else begin
      let len = IT.fill_key it buf in
        (* try to decode datum key, check if it's the same KS *)
        if not (Obs_datum_encoding.decode_datum_key
                  ~table_r
                  ~key_buf_r ~key_len_r
                  ~column_buf_r ~column_len_r
                  ~timestamp_buf:some_timestamp_buf
                  !buf len) ||
           Obs_datum_encoding.get_datum_key_keyspace_id !buf <> tx.ks.ks_id
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
                  Continue | Continue_with _ when not reverse -> it_next tx.ks it
                | Continue | Continue_with _ (* reverse *) -> it_prev tx.ks it
                | Skip_key | Skip_key_with _ when not reverse -> begin
                    (* we first move with it_next for a while, since it's much
                     * faster than it_seek *)
                    match advance_until_next_key
                            ~reverse:false
                            (it_next tx.ks) it tx.ks.ks_id !table_id
                            ~prev_key_buf:!key_buf ~prev_key_len:!key_len
                            ~table_r
                            ~key_buf:next_key_buf ~key_len:next_key_len
                            ~key_buf_r:next_key_buf_r ~key_len_r:next_key_len_r buf
                    with
                        `Finished -> ()
                      | `Need_seek ->
                        Obs_datum_encoding.encode_datum_key datum_key_buf tx.ks.ks_id
                          ~table:!table_id ~key:(next_key ())
                          ~column:"" ~timestamp:Int64.min_int;
                        it_seek tx.ks it
                          (Obs_bytea.unsafe_string datum_key_buf) 0
                          (Obs_bytea.length datum_key_buf)
                  end
                | Skip_key | Skip_key_with _ (* when reverse *) -> begin
                    match advance_until_next_key
                            ~reverse:true
                            (it_prev tx.ks) it tx.ks.ks_id !table_id
                            ~prev_key_buf:!key_buf ~prev_key_len:!key_len
                            ~table_r
                            ~key_buf:next_key_buf ~key_len:next_key_len
                            ~key_buf_r:next_key_buf_r ~key_len_r:next_key_len_r buf
                    with
                        `Finished -> ()
                      | `Need_seek ->
                          (* we jump to the first datum for the current key, then go
                           * one back *)
                          Obs_datum_encoding.encode_datum_key datum_key_buf tx.ks.ks_id
                            ~table:!table_id ~key:(String.sub !key_buf 0 !key_len)
                            ~column:"" ~timestamp:Int64.max_int;
                          it_seek tx.ks it
                            (Obs_bytea.unsafe_string datum_key_buf) 0
                            (Obs_bytea.length datum_key_buf);
                          if IT.valid it then it_prev tx.ks it
                  end
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
            Obs_datum_encoding.encode_datum_key_to_string tx.ks.ks_id ~table
              ~key:(Option.default "" first_key) ~column:first_column
              ~timestamp:Int64.min_int
          in
            it_seek tx.ks it first_datum_key 0 (String.length first_datum_key);
        end else begin
          match first_key with
              None ->
                (* we go to the first datum of the next table, then back *)
                let datum_key =
                  Obs_datum_encoding.encode_datum_key_to_string tx.ks.ks_id
                    ~table:(table + 1)
                    ~key:"" ~column:"" ~timestamp:Int64.min_int
                in
                  it_seek tx.ks it datum_key 0 (String.length datum_key);
                  if IT.valid it then it_prev tx.ks it
            | Some k ->
                (* we go to the datum for the key, then back (it's exclusive) *)
                let datum_key =
                  Obs_datum_encoding.encode_datum_key_to_string tx.ks.ks_id
                    ~table
                    ~key:k ~column:"" ~timestamp:Int64.min_int
                in
                  it_seek tx.ks it datum_key 0 (String.length datum_key);
                  if IT.valid it then it_prev tx.ks it
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
  Lwt_pool.use !(tx.iter_pool)
    (fun it -> fold_over_data_aux it tx table f acc ?first_column
                 ~reverse ~first_key ~up_to_key)

let get_keys_aux init_f map_f fold_f tx table ?(max_keys = max_int) = function
    Keys l ->
      with_exists_key tx table
        (fun exists_key ->
           detach_ks_op tx.ks
             (fun () ->
                let s = S.of_list l in
                let s = S.diff s (M.find_default S.empty table tx.deleted_keys) in
                let s =
                  S.filter
                    (fun k ->
                       S.mem k (M.find_default S.empty table tx.added_keys) ||
                       exists_key k)
                    s
                in map_f s)
             ())

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
            Obs_load_stats.record_bytes_rd tx.ks.ks_db.load_stats key_len;
            Obs_load_stats.record_cols_rd tx.ks.ks_db.load_stats 1;
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

let exist_keys tx table keys =
  with_exists_key tx table
    (fun exists_key ->
       detach_ks_op tx.ks
         (fun keys ->
            List.map
              (fun key ->
                 S.mem key (M.find_default S.empty table tx.added_keys) ||
                 (not (S.mem key (M.find_default S.empty table tx.deleted_keys)) &&
                  exists_key key))
              keys)
         keys)

let exists_key tx table key =
  exist_keys tx table [key] >>= function
    | true :: _ -> return true
    | _ -> return false

let count_keys tx table range =
  let s = ref S.empty in
  get_keys_aux
    (fun _s -> s := _s; S.cardinal _s)
    (fun s -> S.cardinal s)
    (fun n key_buf key_len ->
       if not (S.is_empty !s) && S.mem (String.sub key_buf 0 key_len) !s then n
       else (n + 1))
    tx table ~max_keys:max_int range >|= Int64.of_int

let rec rev_append l = function
    a::b::c::d::tl -> rev_append (d :: c :: b :: a :: l) tl
  | x :: tl -> rev_append (x :: l) tl
  | [] -> l

let rev_merge_rev cmp l1 l2 =
  let rec do_rev_merge_rev cmp acc l1 l2 =
    match l1, l2 with
        [], [] -> acc
      | [], x :: tl | x :: tl, [] -> do_rev_merge_rev cmp (x :: acc) [] tl
      | x1 :: tl1, x2 :: tl2 ->
          match cmp x1 x2 with
              n when n > 0 -> do_rev_merge_rev cmp (x1 :: acc) tl1 l2
            | n when n < 0 -> do_rev_merge_rev cmp (x2 :: acc) l1 tl2
            | _ -> do_rev_merge_rev cmp (x2 :: acc) tl1 tl2
  in do_rev_merge_rev cmp [] l1 l2

let rev l = rev_append [] l

let rev_merge cmp l1 l2 =
  let rec do_merge cmp acc l1 l2 =
    match l1, l2 with
        [], x | x, [] -> rev_append acc x
      | x1 :: tl1, x2 :: tl2 ->
          match cmp x1 x2 with
              n when n < 0 -> do_merge cmp (x1 :: acc) tl1 l2
            | n when n > 0 -> do_merge cmp (x2 :: acc) l1 tl2
            | _ -> do_merge cmp (x2 :: acc) tl1 tl2
  in do_merge cmp [] l1 l2

let filter_map_rev_merge cmp map merge ~limit l1 l2 =

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

let rev_filter_map f l =
  let rec do_rev_filter_map f acc = function
      [] -> acc
    | x :: tl -> match f x with
          Some y -> do_rev_filter_map f (y :: acc) tl
        | None -> do_rev_filter_map f acc tl
  in do_rev_filter_map f [] l

let simple_column_range_selector = function
  | Columns l ->
      if List.length l < 5 then (* TODO: determine threshold *)
        (fun ~buf ~len ->
           List.exists
             (fun s ->
                Obs_string_util.cmp_substrings s 0 (String.length s) buf 0 len = 0)
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
               Obs_string_util.cmp_substrings buf 0 len x 0 (String.length x) >= 0) in
      let cmp_up_to = match up_to with
          None -> (fun ~buf ~len -> true)
        | Some x ->
            (fun ~buf ~len ->
               Obs_string_util.cmp_substrings buf 0 len x 0 (String.length x) < 0)
      in (fun ~buf ~len -> cmp_first ~buf ~len && cmp_up_to ~buf ~len)

let column_range_selector = function
    All_columns -> (fun ~buf ~len -> true)
  | Column_range_union [] -> (fun ~buf ~len -> false)
  | Column_range_union [x] -> simple_column_range_selector x
  | Column_range_union l ->
        let fs = List.map simple_column_range_selector l in
          (fun ~buf ~len -> List.exists (fun f -> f ~buf ~len) fs)

let columns_needed_for_predicate = function
    None -> S.empty
  | Some (Satisfy_any preds) ->
      List.fold_left
        (fun s (Satisfy_all preds) ->
           List.fold_left
             (fun s pred -> match pred with
                  Column_val (col, _) -> S.add col s)
             s preds)
        S.empty preds

let column_range_selector_for_predicate = function
    None -> (fun ~buf ~len -> false)
  | Some _ as x ->
      simple_column_range_selector
        (Columns (S.to_list (columns_needed_for_predicate x)))

let scmp = Obs_string_util.compare

let row_predicate_func = function
    None -> (fun _ -> true)
  | Some pred ->
      let eval_col_val_pred = function
          EQ x -> (fun v -> scmp v x = 0)
        | LT x -> (fun v -> scmp v x < 0)
        | GT x -> (fun v -> scmp v x > 0)
        | LE x -> (fun v -> scmp v x <= 0)
        | GE x -> (fun v -> scmp v x >= 0)
        | Between (x, true, y, true) -> (fun v -> scmp v x >= 0 && scmp v y <= 0)
        | Between (x, false, y, true) -> (fun v -> scmp v x > 0 && scmp v y <= 0)
        | Between (x, true, y, false) -> (fun v -> scmp v x >= 0 && scmp v y < 0)
        | Between (x, false, y, false) -> (fun v -> scmp v x > 0 && scmp v y < 0)
        | Any -> (fun _ -> true) in
      let eval_simple = function
        | Column_val (name, rel) ->
            let f = eval_col_val_pred rel in
              (fun cols ->
                 try f (List.find (fun c -> c.name = name) cols).data
                 with Not_found -> false) in
      let eval_and eval (Satisfy_all preds) =
        let fs = List.map eval preds in
          (fun cols -> List.for_all (fun f -> f cols) fs) in
      let eval_or eval (Satisfy_any preds) =
        let fs = List.map eval preds in
          (fun cols -> List.exists (fun f -> f cols) fs)
      in eval_or (eval_and eval_simple) pred

let get_slice_aux_discrete
      postproc_keydata get_keydata_key ~keep_columnless_keys
      tx table
      ~max_keys ~max_columns ~decode_timestamps
      key_range ?predicate column_range =
  let column_selected = column_range_selector column_range in
  let is_column_deleted = is_column_deleted tx table in
  let column_needed_for_predicate = column_range_selector_for_predicate predicate in
  let eval_predicate = row_predicate_func predicate in
  let l = key_range in
  let l =
    List.filter
      (fun k -> not (S.mem k (M.find_default S.empty table tx.deleted_keys)))
      (List.sort String.compare l) in
  let reverse = match column_range with
      Column_range_union [Column_range { reverse; _ }] -> reverse
    | _ -> false in
  let columns_needed_for_predicate = columns_needed_for_predicate predicate in
  lwt key_data_list, _ =
    Lwt_list.fold_left_s
      (fun (key_data_list, keys_so_far) key ->
         if keys_so_far >= max_keys then return (key_data_list, keys_so_far)
         else
           let columns_selected = ref 0 in

           let fold_datum (rev_cols, rev_predicate_cols, missing_pred_cols) it
                 ~key_buf ~key_len ~column_buf ~column_len
                 ~timestamp_buf =
             (* must skip deleted columns *)
             if is_column_deleted
                  ~key_buf:key ~key_len:(String.length key)
                  ~column_buf ~column_len then
               Continue
             else begin
               let selected = column_selected column_buf column_len in
               let needed_for_pred =
                 column_needed_for_predicate column_buf column_len
               in
                 (* also skip columns not selected + not needed for predicate *)
                 if not selected && not needed_for_pred then
                   Continue
                 else begin
                   let data = IT.get_value it in
                   let name = String.sub column_buf 0 column_len in
                   let timestamp = match decode_timestamps with
                       false -> No_timestamp
                     | true -> Timestamp (Obs_datum_encoding.decode_timestamp timestamp_buf) in
                   let col = { name; data; timestamp; } in
                     let rev_cols =
                       (* we must check against if we have enough cols,
                        * because we could be collecting pred cols only
                        * by now *)
                       if selected && !columns_selected < max_columns then begin
                         incr columns_selected;
                         col :: rev_cols
                       end else rev_cols in
                     let rev_predicate_cols, missing_pred_cols =
                       (* we're careful not to overwrite the in-tx pred_col
                       * value with the one from disk, so we do the following
                       * instead of just   if needed_for_pred  *)
                       if not (S.mem name missing_pred_cols) then
                         (rev_predicate_cols, missing_pred_cols)
                       else (col :: rev_predicate_cols, S.remove name missing_pred_cols)
                     in
                       (* only early exit if got all the columns
                        * needed to evaluate the predicate *)
                       if !columns_selected >= max_columns && S.is_empty missing_pred_cols then
                         Finish_fold (rev_cols, rev_predicate_cols, missing_pred_cols)
                       else Continue_with (rev_cols, rev_predicate_cols, missing_pred_cols)
                 end
             end in

           lwt rev_cols1, rev_pred_cols1, _ =
             let first_key, up_to_key =
               if reverse then
                 (Some (key ^ "\000"), Some key)
               else
                 (Some key, Some (key ^ "\000")) in
             (* we take the columns needed for the predicate directly from the
              * tx data and inject it them into fold_over_data *)
             let cols_in_mem_map =
               M.find_default M.empty table tx.added |>
               M.find_default M.empty key in
             let pred_cols_in_mem, pred_cols_in_mem_set =
               M.fold
                 (fun name data ((l, s) as acc) ->
                    if column_needed_for_predicate name (String.length name) then
                      ({ name; data; timestamp = No_timestamp; } :: l, S.add name s)
                    else acc)
                 cols_in_mem_map
                 ([], S.empty)
             in
               fold_over_data tx table fold_datum
                  ([], pred_cols_in_mem,
                   S.diff columns_needed_for_predicate pred_cols_in_mem_set)
                  ~reverse ~first_key ~up_to_key in

           let rev_cols2 =
             M.fold
               (fun col data cols ->
                  let len = String.length col in
                  if not (column_selected col len) then
                    cols
                  else begin
                    let col =  { name = col; data; timestamp = No_timestamp } in
                      col :: cols
                  end)
               (tx.added |>
                M.find_default M.empty table |> M.find_default M.empty key)
               [] in

           (* rev_cols2 is G-to-S, want S-to-G if ~reverse *)
           let rev_cols2 = if reverse then rev rev_cols2 else rev_cols2 in

           let cmp_cols =
             if reverse then
               (fun c1 c2 -> String.compare c2.name c1.name)
             else
               (fun c1 c2 -> String.compare c1.name c2.name) in

           let pred_cols = rev_pred_cols1 in
             (* now eval predicate *)
             if eval_predicate pred_cols then begin
               let cols = rev (rev_merge_rev cmp_cols rev_cols1 rev_cols2) in

                 match postproc_keydata ~guaranteed_cols_ok:false (key, cols) with
                   None -> return (key_data_list, keys_so_far)
                 | Some x -> return (x :: key_data_list, keys_so_far + 1)
             end else begin
               return (key_data_list, keys_so_far)
             end)
      ([], 0) l in
  let last_key = match key_data_list with
      x :: _ -> Some (get_keydata_key x)
    | [] -> None
  in return (last_key, rev key_data_list)

let get_slice_aux_continuous_no_predicate
      postproc_keydata get_keydata_key ~keep_columnless_keys
      tx table
      ~max_keys
      ~max_columns
      ~decode_timestamps
      { first; up_to; reverse; } column_range =
  let column_selected = column_range_selector column_range in
  let is_column_deleted = is_column_deleted tx table in

  let first_pass = ref true in
  let keys_so_far = ref 0 in
  let prev_key = Obs_bytea.create 13 in
  let cols_in_this_key = ref 0 in
  let cols_kept = ref 0 in

  let fold_datum ((key_data_list, key_data) as acc)
        it ~key_buf ~key_len ~column_buf ~column_len ~timestamp_buf =
    let ((key_data_list, key_data) as acc') =
      if Obs_string_util.cmp_substrings
           (Obs_bytea.unsafe_string prev_key) 0 (Obs_bytea.length prev_key)
           key_buf 0 key_len = 0
      then
        acc
      else begin
        (* new key, increment count and copy the key to prev_key *)
          cols_kept := 0;
          cols_in_this_key := 0;
          let acc' =
            match key_data with
              _ :: _ ->
                incr keys_so_far;
                ((Obs_bytea.contents prev_key, key_data) :: key_data_list, [])
            | [] ->
                if not !first_pass && keep_columnless_keys then begin
                  incr keys_so_far;
                  ((Obs_bytea.contents prev_key, key_data) :: key_data_list, [])
                end else acc
          in
            Obs_bytea.clear prev_key;
            Obs_bytea.add_substring prev_key key_buf 0 key_len;
            acc'
      end

    in
      first_pass := false;
      incr cols_in_this_key;
      (* early termination w/o iterating over further keys if we
       * already have enough  *)
      if !keys_so_far >= max_keys then
        Finish_fold acc'
      (* see if we already have enough columns for this key*)
      else if !cols_kept >= max_columns then begin
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
          | true -> Timestamp (Obs_datum_encoding.decode_timestamp timestamp_buf) in
        let col_data = { name = col; data; timestamp; } in

        let key_data = col_data :: key_data in
        let acc = (key_data_list, key_data) in
          (* if we're in the last key, and have enough columns, finish *)
          if !keys_so_far >= max_keys - 1 && !cols_kept >= max_columns then
            Finish_fold acc
          else
            Continue_with acc
      end
  in

  lwt rev_key_data_list1 =
    lwt l1, d1 =
      fold_over_data tx table fold_datum ([], [])
        ~reverse ~first_key:first ~up_to_key:up_to
    in
      if !keys_so_far >= max_keys then return l1
      else match d1 with
          [] when not !first_pass && keep_columnless_keys ->
            return ((Obs_bytea.contents prev_key, d1) :: l1)
        | [] -> return l1
        | cols -> return ((Obs_bytea.contents prev_key, cols) :: l1) in

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
         in (key, if reverse then rev cols else cols) :: l)
      (let first, up_to =
         if reverse then (up_to, first) else (first, up_to)
       in M.submap ?first ?up_to (M.find_default M.empty table tx.added))
      []

  in
    match rev_key_data_list2 with
        [] -> (* fast path for common case with empty in-tx data *)
          let last_key = match rev_key_data_list1 with
              (k, _) :: _ -> Some k
            | [] -> None in
          let postproc keydata =
            postproc_keydata ~guaranteed_cols_ok:true keydata
          in return (last_key, rev_filter_map postproc rev_key_data_list1)

      | rev_key_data_list2 ->
          (* if the key range was reverse,
           * rev_key_data_list1 : (key * column list) list
           * holds the keys in normal order (smaller to greater),
           * otherwise in reverse order (greater to smaller).
           * rev_key_data_list2 is always in reverse order.
           * *)

          let key_data_list1 = rev rev_key_data_list1 in
          let key_data_list2 =
            if not reverse then rev rev_key_data_list2
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
              (fun cols1 cols2 -> rev (rev_merge cmp_cols cols1 cols2))
            else (* both are G-to-S *)
              (fun cols1 cols2 -> rev (rev_merge_rev cmp_cols cols1 cols2)) in

          let rev_key_data_list =
            filter_map_rev_merge
              cmp_keys
              (postproc_keydata ~guaranteed_cols_ok:false)
              (fun (k, rev_cols1) (_, rev_cols2) ->
                 let cols = merge rev_cols1 rev_cols2 in
                   (k, cols))
              ~limit:max_keys key_data_list1 key_data_list2 in

          let last_key = match rev_key_data_list with
              x :: _ -> Some (get_keydata_key x)
            | [] -> None
          in return (last_key, rev rev_key_data_list)

let get_slice_aux_continuous_with_predicate
      postproc_keydata get_keydata_key ~keep_columnless_keys
      tx table
      ~max_keys
      ~max_columns
      ~decode_timestamps
      { first; up_to; reverse; } ~predicate column_range =
  let predicate = Some predicate in
  let column_selected = column_range_selector column_range in
  let is_column_deleted = is_column_deleted tx table in
  let column_needed_for_predicate = column_range_selector_for_predicate predicate in
  let eval_predicate = row_predicate_func predicate in

  let columns_needed_for_predicate = columns_needed_for_predicate predicate in

  let first_pass = ref true in
  let keys_so_far = ref 0 in
  let prev_key = Obs_bytea.create 13 in
  let cols_in_this_key = ref 0 in
  let cols_kept = ref 0 in

  let fold_datum ((key_data_list, key_data, pred_cols, missing_pred_cols) as acc)
        it ~key_buf ~key_len ~column_buf ~column_len ~timestamp_buf =
    let ((key_data_list, key_data, pred_cols, missing_pred_cols) as acc') =
      if Obs_string_util.cmp_substrings
           (Obs_bytea.unsafe_string prev_key) 0 (Obs_bytea.length prev_key)
           key_buf 0 key_len = 0
      then
        acc
      else begin
        (* new key, increment count and copy the key to prev_key *)
          cols_kept := 0;
          cols_in_this_key := 0;
          (* we grab the pred_cols and missing_pred_cols for the new key
          * directly from tx data *)
          let new_key = String.sub key_buf 0 key_len in
          let cols_in_mem_map =
            M.find_default M.empty table tx.added |>
            M.find_default M.empty new_key in
          let pred_cols_in_mem, pred_cols_in_mem_set =
            M.fold
              (fun name data ((l, s) as acc) ->
                 if column_needed_for_predicate name (String.length name) then
                   ({ name; data; timestamp = No_timestamp; } :: l, S.add name s)
                 else acc)
              cols_in_mem_map
              ([], S.empty) in
          let missing_pred_cols =
            S.diff columns_needed_for_predicate pred_cols_in_mem_set in
          let acc' =
            match key_data with
              _ :: _ when eval_predicate pred_cols ->
                incr keys_so_far;
                ((Obs_bytea.contents prev_key, key_data) :: key_data_list,
                 [], pred_cols_in_mem, missing_pred_cols)
            | _ :: _ -> (key_data_list, [], pred_cols_in_mem, missing_pred_cols)
            | [] ->
                if not !first_pass && keep_columnless_keys &&
                   eval_predicate pred_cols then begin
                  incr keys_so_far;
                  ((Obs_bytea.contents prev_key, key_data) :: key_data_list,
                   [], pred_cols_in_mem, missing_pred_cols)
                end else (key_data_list, [], pred_cols_in_mem, missing_pred_cols)
          in
            Obs_bytea.clear prev_key;
            Obs_bytea.add_substring prev_key key_buf 0 key_len;
            acc'
      end

    in
      first_pass := false;
      incr cols_in_this_key;
      (* early termination w/o iterating over further keys if we
       * already have enough  *)
      if !keys_so_far >= max_keys then
        Finish_fold acc'
      (* see if we already have enough columns for this key*)
      else if !cols_kept >= max_columns && S.is_empty missing_pred_cols then begin
        (* seeking is very expensive, so we only do it when it looks
         * like the key has got a lot of columns *)
        if !cols_in_this_key - max_columns < 50 then (* FIXME: determine constant *)
          (if acc == acc' then Continue else Continue_with acc')
        else
          (if acc == acc' then Skip_key else Skip_key_with acc')
      end else if is_column_deleted ~key_buf ~key_len ~column_buf ~column_len then
        (if acc == acc' then Continue else Continue_with acc')
      else begin
        let selected = column_selected column_buf column_len in
        let needed_for_pred = column_needed_for_predicate column_buf column_len in
          if not selected && not needed_for_pred then begin
            (* skip column *)
            if acc == acc' then Continue else Continue_with acc'
          end else begin
            let data = IT.get_value it in
            let name = String.sub column_buf 0 column_len in
            let timestamp = match decode_timestamps with
                false -> No_timestamp
              | true -> Timestamp (Obs_datum_encoding.decode_timestamp timestamp_buf) in
            let col_data = { name; data; timestamp; } in

            let key_data =
              (* we must check against if we have enough cols, because we
               * could be collecting pred cols only by now *)
              if not selected || !cols_kept >= max_columns then key_data
              else begin
                incr cols_kept;
                col_data :: key_data
              end in
            let pred_cols, missing_pred_cols =
              if not (S.mem name missing_pred_cols) then pred_cols, missing_pred_cols
              else (col_data :: pred_cols, S.remove name missing_pred_cols) in
            let acc = (key_data_list, key_data, pred_cols, missing_pred_cols) in
              (* if we're in the last key, and have enough columns, finish *)
              (* only if we have all the cols we need for the predicate *)
              if !keys_so_far >= max_keys - 1 && !cols_kept >= max_columns &&
                 S.is_empty missing_pred_cols
              then
                Finish_fold acc
              else
                Continue_with acc
          end
      end
  in

  lwt rev_key_data_list1 =
    lwt l1, d1, pred_cols, _ =
      fold_over_data tx table fold_datum ([], [], [], S.empty)
        ~reverse ~first_key:first ~up_to_key:up_to
    in
      if !keys_so_far >= max_keys then return l1
      else begin
        (* take predicate columns from tx data and add them to pred_cols *)
        let last_key = Obs_bytea.contents prev_key in
        let pred_cols =
          let m = M.find_default M.empty table tx.added |>
                  M.find_default M.empty last_key
          in
            M.fold
              (fun name data l ->
                 if column_needed_for_predicate name (String.length name) then
                   { name; data; timestamp = No_timestamp; } :: l
                 else l)
              m
              pred_cols
        in
          match d1 with
            [] when not !first_pass && keep_columnless_keys && eval_predicate pred_cols ->
              return ((last_key, d1) :: l1)
          | [] -> return l1
          | cols when eval_predicate pred_cols ->
              return ((last_key, cols) :: l1)
          | _ -> return l1
      end in

  let rev_key_data_list2 =
    M.fold
      (fun key key_data_in_mem l ->
         let cols, pred_cols =
           M.fold
             (fun col data ((cols, pred_cols) as acc) ->
                let len = String.length col in
                let selected = column_selected col len in
                let needed_for_pred = column_needed_for_predicate col len in
                  if not selected && not needed_for_pred then
                    acc
                  else begin
                    let col =  { name = col; data; timestamp = No_timestamp } in
                    let cols = if selected then col :: cols else cols in
                    let pred_cols =
                      if needed_for_pred then col :: pred_cols else pred_cols in
                      (cols, pred_cols)
                  end)
             key_data_in_mem
             ([], [])
         in
           if eval_predicate pred_cols then
             (key, if reverse then rev cols else cols) :: l
           else l)
      (let first, up_to =
         if reverse then (up_to, first) else (first, up_to)
       in M.submap ?first ?up_to (M.find_default M.empty table tx.added))
      []

  in
    match rev_key_data_list2 with
        [] -> (* fast path for common case with empty in-tx data *)
          let last_key = match rev_key_data_list1 with
              (k, _) :: _ -> Some k
            | [] -> None in
          let postproc keydata =
            postproc_keydata ~guaranteed_cols_ok:true keydata
          in return (last_key, rev_filter_map postproc rev_key_data_list1)

      | rev_key_data_list2 ->
          (* if the key range was reverse,
           * rev_key_data_list1 : (key * column list) list
           * holds the keys in normal order (smaller to greater),
           * otherwise in reverse order (greater to smaller).
           * rev_key_data_list2 is always in reverse order.
           * *)

          let key_data_list1 = rev rev_key_data_list1 in
          let key_data_list2 =
            if not reverse then rev rev_key_data_list2
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
              (fun cols1 cols2 -> rev (rev_merge cmp_cols cols1 cols2))
            else (* both are G-to-S *)
              (fun cols1 cols2 -> rev (rev_merge_rev cmp_cols cols1 cols2)) in

          let rev_key_data_list =
            filter_map_rev_merge
              cmp_keys
              (postproc_keydata ~guaranteed_cols_ok:false)
              (fun (k, rev_cols1) (_, rev_cols2) ->
                 let cols = merge rev_cols1 rev_cols2 in
                   (k, cols))
              ~limit:max_keys key_data_list1 key_data_list2 in

          let last_key = match rev_key_data_list with
              x :: _ -> Some (get_keydata_key x)
            | [] -> None
          in return (last_key, rev rev_key_data_list)

let get_slice_aux
      postproc_keydata get_keydata_key ~keep_columnless_keys
      tx table
      ~max_keys ~max_columns ~decode_timestamps
      key_range ?predicate column_range =
  match predicate, key_range with
      _, Keys l -> get_slice_aux_discrete
                     postproc_keydata get_keydata_key ~keep_columnless_keys tx table
                     ~max_keys ~max_columns ~decode_timestamps
                     l ?predicate column_range

    | None, Key_range range ->
        get_slice_aux_continuous_no_predicate
          postproc_keydata get_keydata_key ~keep_columnless_keys tx table
          ~max_keys ~max_columns ~decode_timestamps
          range column_range
    | Some predicate, Key_range range ->
        get_slice_aux_continuous_with_predicate
          postproc_keydata get_keydata_key ~keep_columnless_keys tx table
          ~max_keys ~max_columns ~decode_timestamps
          range ~predicate column_range

let record_column_reads load_stats key last_column columns =
  let col_bytes =
    List.fold_left
      (fun s col ->
         s + String.length col.data + String.length col.name)
      0
      columns
  in
    Obs_load_stats.record_bytes_rd load_stats
      (String.length key + String.length last_column + col_bytes);
    Obs_load_stats.record_cols_rd load_stats (List.length columns)

let get_slice tx table
      ?(max_keys = max_int) ?max_columns
      ?(decode_timestamps = false) key_range ?predicate column_range =
  let max_columns_ = Option.default max_int max_columns in
  let postproc_keydata ~guaranteed_cols_ok (key, rev_cols) =
    match rev_cols with
      | last_candidate :: _ as l when max_columns_ > 0 ->
          if guaranteed_cols_ok then begin
            (* we have at most max_columns_ in l *)
            let last_column = last_candidate.name in
            let columns = rev l in
              record_column_reads tx.ks.ks_db.load_stats key last_column columns;
              Some ({ key; last_column; columns; })
          end else begin
            (* we might have more, so need to List.take *)
            let columns = List.take max_columns_ (rev l) in
            let last_column = (List.last columns).name in
              record_column_reads tx.ks.ks_db.load_stats key last_column columns;
              Some ({ key; last_column; columns; })
          end
      | _ -> None in

  let get_keydata_key { key; _ } = key in

  (* if we're being given a list (union) of columns, we restrict the max
   * number of columns to the cardinality of the column set *)
  let is_columns = function Columns _ -> true | Column_range _ -> false in
  let max_columns = match max_columns with
      Some m -> m
    | None -> match column_range with
        | Column_range_union l when List.for_all is_columns l ->
            let columns =
              List.fold_left
                (fun s x -> match x with
                     Columns l -> List.fold_left (fun s c -> S.add c s) s l
                   | Column_range _ -> s)
                S.empty
                l
            in S.cardinal columns
        | Column_range_union _ | All_columns -> max_int
  in
    Obs_load_stats.record_reads tx.ks.ks_db.load_stats 1;
    get_slice_aux
      postproc_keydata get_keydata_key ~keep_columnless_keys:false tx table
      ~max_keys ~max_columns ~decode_timestamps key_range ?predicate column_range

let get_slice_values tx table
      ?(max_keys = max_int) key_range columns =
  let bytes_read = ref 0 in
  let cols_read = ref 0 in
  let postproc_keydata ~guaranteed_cols_ok (key, cols) =
    let l =
      List.map
        (fun column ->
           try
             let data = (List.find (fun c -> c.name = column) cols).data in
               bytes_read := !bytes_read + String.length data;
               incr cols_read;
               Some data
           with Not_found -> None)
        columns
    in Some (key, l) in

  let get_keydata_key (key, _) = key in

  lwt ret =
    get_slice_aux
      postproc_keydata get_keydata_key ~keep_columnless_keys:true tx table
       ~max_keys ~max_columns:(List.length columns)
       ~decode_timestamps:false key_range
       (Column_range_union [Columns columns])
  in
    Obs_load_stats.record_reads tx.ks.ks_db.load_stats 1;
    Obs_load_stats.record_bytes_rd tx.ks.ks_db.load_stats !bytes_read;
    Obs_load_stats.record_cols_rd tx.ks.ks_db.load_stats !cols_read;
    return ret

let get_slice_values_with_timestamps tx table
      ?(max_keys = max_int) key_range columns =
  let bytes_read = ref 0 in
  let cols_read = ref 0 in
  let now = Int64.of_float (Unix.gettimeofday () *. 1e6) in
  let postproc_keydata ~guaranteed_cols_ok (key, cols) =
    let l =
      List.map
        (fun column ->
           try
             let c = List.find (fun c -> c.name = column) cols in
                bytes_read := !bytes_read + String.length c.data + 8;
                incr cols_read;
                Some (c.data,
                      match c.timestamp with
                        | Timestamp t -> t
                        | No_timestamp -> now)
           with Not_found -> None)
        columns
    in Some (key, l) in

  let get_keydata_key (key, _) = key in

  lwt ret =
    get_slice_aux
      postproc_keydata get_keydata_key ~keep_columnless_keys:true tx table
       ~max_keys ~max_columns:(List.length columns)
       ~decode_timestamps:true key_range
       (Column_range_union [Columns columns])
  in
    Obs_load_stats.record_reads tx.ks.ks_db.load_stats 1;
    Obs_load_stats.record_bytes_rd tx.ks.ks_db.load_stats !bytes_read;
    Obs_load_stats.record_cols_rd tx.ks.ks_db.load_stats !cols_read;
    return ret

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
    get_columns tx table key ~decode_timestamps:true ~max_columns:1
      (Column_range_union [Columns [column_name]])
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

let put_multi_columns tx table data =
  Lwt_list.iter_s
    (fun (key, columns) -> put_columns tx table key columns)
    data

let rec put_multi_columns_no_tx ks table data =
  let timestamp = Int64.of_float (Unix.gettimeofday () *. 1e6) in
  let cols_written = ref 0 in
  let bytes_written = ref 0 in
  let datum_key = put_multi_columns_no_tx_buf in
  lwt wait_sync =
    WRITEBATCH.perform ks.ks_db.writebatch begin fun b ->
      let table =
        match find_maybe ks.ks_tables table with
          Some t -> t
        | None ->
            (* must add table to keyspace table list *)
            register_new_table_and_add_to_writebatch' ks b table
      in
        (* FIXME: should save timestamp provided in
         * put_columns and use it here if it wasn't
         * Auto_timestamp *)
        List.iter
          (fun (key, columns) ->
             List.iter
               (fun c ->
                  Obs_datum_encoding.encode_datum_key datum_key ks.ks_id
                    ~table ~key ~column:c.name ~timestamp;
                  let klen = Obs_bytea.length datum_key in
                  let vlen = String.length c.data in
                    incr cols_written;
                    bytes_written := !bytes_written + klen + vlen;
                    WRITEBATCH.put_substring b
                      (Obs_bytea.unsafe_string datum_key) 0 klen
                      c.data 0 vlen)
               columns)
          data;
        return ()
      end in
  lwt () = wait_sync in
  let stats = ks.ks_db.load_stats in
    Obs_load_stats.record_writes stats 1;
    Obs_load_stats.record_bytes_wr stats !bytes_written;
    Obs_load_stats.record_cols_wr stats !cols_written;
    return ()

and put_multi_columns_no_tx_buf = Obs_bytea.create 10

let put_multi_columns_no_tx ks table data =
  (* we wrap in Miniregion.use because we don't want ks.ks_db.writebatch to
   * change under our feet (due to a concurrent trigger_raw_dump) *)
  Miniregion.use ks.ks_db.db
    (fun _ -> put_multi_columns_no_tx ks table data)

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
  let open Obs_backup in
  let max_chunk = 65536 in
  let module ENC = (val Obs_backup.encoder format : Obs_backup.ENCODER) in
  let enc = ENC.make ~buffer:tx.dump_buffer () in
  lwt pending_tables = match offset with
      Some c -> return c.bc_remaining_tables
    | None -> (* FIXME: should use the tx's iterator to list the tables *)
        match only_tables with
            None -> list_tables tx.ks
          | Some tables -> return tables in
  let curr_key, curr_col = match offset with
      Some c -> Some c.bc_key, Some c.bc_column
    | None -> None, None in

  let cols_read = ref 0 in
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
      incr cols_read;
      Continue
    end in

  let rec collect_data ~curr_key ~curr_col = function
      [] ->
        if ENC.is_empty enc then return None
        else return (Some (Obs_bytea.contents (ENC.finish enc), None))
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
            in return (Some (Obs_bytea.contents (ENC.finish enc), Some cursor))

          (* we don't have enough data yet, move to next table *)
          end else begin
            collect_data ~curr_key:None ~curr_col:None tl
          end in
  lwt ret = collect_data ~curr_key ~curr_col pending_tables in
    Obs_load_stats.record_reads tx.ks.ks_db.load_stats 1;
    Obs_load_stats.record_cols_rd tx.ks.ks_db.load_stats !cols_read;
    Option.may
      (fun (d, _) ->
         Obs_load_stats.record_bytes_rd tx.ks.ks_db.load_stats (String.length d))
      ret;
    return ret

let load tx data =
  Obs_load_stats.record_bytes_wr tx.ks.ks_db.load_stats (String.length data);
  Obs_load_stats.record_writes tx.ks.ks_db.load_stats 1;
  let cols_written = ref 0 in
  let wb = Lazy.force tx.backup_writebatch in
  let enc_datum_key datum_key ~table:table_name ~key ~column ~timestamp =
    incr cols_written;
    match find_maybe tx.ks.ks_tables table_name with
        Some table -> Obs_datum_encoding.encode_datum_key datum_key tx.ks.ks_id
                        ~table ~key ~column ~timestamp
      | None ->
          (* register table first *)
          let table =
            register_new_table_and_add_to_writebatch tx.ks wb table_name
          in Obs_datum_encoding.encode_datum_key datum_key tx.ks.ks_id
               ~table ~key ~column ~timestamp in

  let ok = Obs_backup.load enc_datum_key wb data in
    if not ok then
      return false
    else begin
      Obs_load_stats.record_cols_wr tx.ks.ks_db.load_stats !cols_written;
      tx.backup_writebatch_size <- tx.backup_writebatch_size + String.length data;
      begin
        (* we check if there's enough data in this batch, and write it if so *)
        if tx.backup_writebatch_size < 40_000_000 then
          (* FIXME: allow to set this constant somewhere *)
          return ()
        else begin
          tx.backup_writebatch <- lazy (L.Batch.make ());
          tx.backup_writebatch_size <- 0;
          Miniregion.use tx.ks.ks_db.db
            (fun lldb ->
               detach_ks_op tx.ks (L.Batch.write ~sync:true lldb) wb >>
               reset_iter_pool tx.ks.ks_db)
        end
      end >>
      return true
    end

let load_stats tx =
  return (Obs_load_stats.stats tx.ks.ks_db.load_stats)

let cursor_of_string = Obs_backup.cursor_of_string
let string_of_cursor = Obs_backup.string_of_cursor

type raw_dump = unit

let trigger_raw_dump db =
  Miniregion.update_value db.db
    (fun lldb ->
       (* flush *)
       WRITEBATCH.close db.writebatch >>
       let () = L.close lldb in
       let lldb = db.reopen () in
         begin try_lwt
           let dstdir =
             Filename.concat db.basedir
               (sprintf "dump-%f" (Unix.gettimeofday ())) in
           let is_file fname =
             let fname = Filename.concat db.basedir fname in
             let st = Unix.stat fname in
               match st.Unix.st_kind with
                   Unix.S_REG -> true
                 | _ -> false in
           let hardlink_file fname =
             let src = Filename.concat db.basedir fname in
             let dst = Filename.concat dstdir fname in
               Unix.link src dst in
           let src_files =
             Sys.readdir db.basedir |> Array.to_list |>
             List.filter is_file
           in
             Unix.mkdir dstdir 0o755;
             List.iter hardlink_file src_files;
             return ()
         with exn ->
           (* FIXME: better log *)
           eprintf "Error in trigger_raw_dump: %s\n%!" (Printexc.to_string exn);
           return ()
         end >>
         return lldb)

let with_transaction ks f =
  match Lwt.get tx_key with
      None -> read_committed_transaction ks f
    | Some tx -> f tx

let get_keys ks table ?max_keys key_range =
  with_transaction ks (fun tx -> get_keys tx table ?max_keys key_range)

let count_keys ks table key_range =
  with_transaction ks (fun tx -> count_keys tx table key_range)

let exists_key ks table key =
  with_transaction ks (fun tx -> exists_key tx table key)

let exist_keys ks table keys =
  with_transaction ks (fun tx -> exist_keys tx table keys)

let get_slice ks table ?max_keys ?max_columns ?decode_timestamps
      key_range ?predicate column_range =
  with_transaction ks
    (fun tx ->
       get_slice tx table ?max_keys ?max_columns ?decode_timestamps
         key_range ?predicate column_range)

let get_slice_values ks table ?max_keys key_range columns =
  with_transaction ks
    (fun tx -> get_slice_values tx table ?max_keys key_range columns)

let get_slice_values_with_timestamps ks table ?max_keys key_range columns =
  with_transaction ks
    (fun tx -> get_slice_values_with_timestamps tx table ?max_keys key_range columns)

let get_columns ks table ?max_columns ?decode_timestamps key column_range =
  with_transaction ks
    (fun tx -> get_columns tx table ?max_columns ?decode_timestamps key column_range)

let get_column_values ks table key columns =
  with_transaction ks
    (fun tx -> get_column_values tx table key columns)

let get_column ks table key column =
  with_transaction ks (fun tx -> get_column tx table key column)

let put_multi_columns ks table data =
  match Lwt.get tx_key with
    | None -> put_multi_columns_no_tx ks table data
    | Some tx -> put_multi_columns tx table data

let put_columns ks table key columns =
  put_multi_columns ks table [key, columns]

let delete_columns ks table key columns =
  with_transaction ks (fun tx -> delete_columns tx table key columns)

let delete_key ks table key =
  with_transaction ks (fun tx -> delete_key tx table key)

let dump ks ?format ?only_tables ?offset () =
  with_transaction ks (fun tx -> dump tx ?format ?only_tables ?offset ())

let load ks data = with_transaction ks (fun tx -> load tx data)

let load_stats ks = with_transaction ks load_stats

let read_committed_transaction ks f =
  read_committed_transaction ks (fun _ -> f ks)

let repeatable_read_transaction ks f =
  repeatable_read_transaction ks (fun _ -> f ks)
