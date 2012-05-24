(*
 * Copyright (C) 2011-2012 Mauricio Fernandez <mfp@acm.org>
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

module WeakTbl : sig
  type ('a, 'b) t

  val create : ?cmp:('a -> 'a -> int) -> unit -> ('a, 'b) t
  val get : ('a, 'b) t -> 'a -> 'b option
  val add : ('a, 'b) t -> 'a -> 'b -> unit
  val remove : ('a, 'b) t -> 'a -> unit
end =
struct
  module M = BatMap
  type ('a, 'b) t = ('a, 'b Obs_weak_ref.t) M.t ref

  let create ?(cmp = compare) () = ref (M.create cmp)

  let get t k =
    try
      Obs_weak_ref.get (M.find k !t)
    with Not_found -> None

  let add t k v =
    t := M.add k (Obs_weak_ref.make v) !t

  let remove t k = t := M.remove k !t
end

let make_iter_pool db =
  Lwt_pool.create 100 (* FIXME: which limit? *)
    (fun () -> return (L.iterator db))

module IM = Map.Make(struct
                       type t = int
                       let compare (x:int) y =
                         if x < y then -1 else if x > y then 1 else 0
                     end)

type slave =
    {
      slave_id : int;
      slave_push : (Obs_bytea.t -> [`ACK | `NACK] Lwt.u -> unit);
      mutable slave_mode : sync_mode;
      slave_mode_target : sync_mode;
      mutable slave_pending_updates : int;
    }

and sync_mode = Async | Sync

module WRITEBATCH : sig
  type t
  type tx

  val make : L.db -> L.iterator Lwt_pool.t ref -> fsync:bool -> t

  val close : t -> unit Lwt.t
  val perform : t -> (tx -> unit) -> unit Lwt.t

  (** [add_slave t ~unregister slave] *)
  val add_slave : t -> unregister:(int -> unit) -> slave -> unit

  val delete_substring : tx -> string -> int -> int -> unit
  val put_substring : tx -> string -> int -> int -> string -> int -> int -> unit
  val put : tx -> string -> string -> unit
  val timestamp : tx -> Int64.t

  (* indicate that a 'reload keyspace' record is to be added to the
   * serialized update *)
  val need_keyspace_reload : tx -> unit

  (* requested rate relative to maximum (1.0 being no throttling) *)
  val throttling : t -> float
end =
struct
  type t =
      { db : L.db;
        mutable wb : L.writebatch;
        mutable dirty : bool;
        mutable closed : bool;
        mutable sync_wait : unit Lwt.t * unit Lwt.u;
        mutable wait_last_sync : unit Lwt.t * unit Lwt.u;
        (* used to wait for last sync before close *)
        iter_pool : L.iterator Lwt_pool.t ref;
        (* will be overwritten after an update *)

        mutable serialized_update : Obs_bytea.t;

        mutable slave_tbl : ((int -> unit) * slave) IM.t;
        (* the [int -> unit] function is used to unregister a slave from an
         * external registry *)
        (* the [Lwt.u] is used to wake up a thread waiting for
         * confirmation that the update was persisted (for sync replication) *)

        fsync : bool;
        mutable need_keyspace_reload : bool;
        (* whether to add a 'reload keyspace' record to the serialized update *)

        mutable throttling : throttling;

        wait_for_dirty : unit Lwt_condition.t;
      }

  and throttling =
      No_throttling
    | Throttle of int (* started when read N L0 files  *) * float (* rate *)

  type tx = { t : t; mutable valid : bool; timestamp : Int64.t; }

  let do_push t serialized_update (slave_id, (unregister, slave)) =
    let waiter, u = Lwt.task () in
      (* see if we have to switch from async to sync (slave caught up
       * with update stream) *)
      begin match slave.slave_mode, slave.slave_mode_target with
          Sync, _ | _, Async -> ()
        | Async, Sync ->
            if slave.slave_pending_updates = 0 then
              slave.slave_mode <- Sync;
      end;
      slave.slave_pending_updates <- slave.slave_pending_updates + 1;
      let wait_for_signal () =
        try_lwt
          match_lwt waiter with
              `ACK -> return ()
            | `NACK ->
                t.slave_tbl <- IM.remove slave_id t.slave_tbl;
                unregister slave_id;
                return ()
        finally
          slave.slave_pending_updates <- slave.slave_pending_updates - 1;
          return ()
      in
        match slave.slave_mode with
            Sync -> begin
              slave.slave_push serialized_update u;
              wait_for_signal ()
            end
          | Async -> begin
              slave.slave_push serialized_update u;
              ignore begin try_lwt
                wait_for_signal ()
              with exn ->
                (* FIXME: log? *)
                return ()
              end;
              return ()
            end

  let push_to_slaves t serialized_update =
    let slaves = IM.fold (fun k v l -> (k, v) :: l) t.slave_tbl [] in
      Lwt_list.iter_p (do_push t serialized_update) slaves

  let num_l0_files db =
    int_of_string (Option.default "4" (L.get_property db "leveldb.num-files-at-level0"))

  let control_throttling t =
    match t.throttling with
        No_throttling ->
          let nl0 = num_l0_files t.db in
            if nl0 > 6 then
              t.throttling <- Throttle (nl0, 0.5 ** (float (nl0 - 6)))
      | Throttle (nl0, rate) ->
          let nl0' = num_l0_files t.db in
            if nl0' < 6 then
              t.throttling <- No_throttling
            else if nl0' > nl0 then
              t.throttling <- Throttle (nl0', 0.5 ** (float (nl0' - 6)))

  let throttling t = match t.throttling with
      No_throttling -> 1.0
    | Throttle (_, rate) -> rate

  let make db iter_pool ~fsync =
    let t =
      { db; wb = L.Batch.make (); dirty = false; closed = false;
        sync_wait = Lwt.wait ();
        wait_last_sync = Lwt.wait ();
        iter_pool;
        serialized_update = Obs_bytea.create 128;
        slave_tbl = IM.empty; fsync;
        need_keyspace_reload = false;
        throttling = No_throttling;
        wait_for_dirty = Lwt_condition.create ();
      }
    in
      ignore begin try_lwt
        let rec writebatch_loop () =
          begin if not t.dirty && not t.closed then
              Lwt_condition.wait t.wait_for_dirty
          else return ()
          end >>
          if t.closed && not t.dirty then begin
            Lwt.wakeup (snd t.wait_last_sync) ();
            return ()
          end else if not t.dirty then
            writebatch_loop ()
          else begin
            (* need a "reload keyspace" record if needed *)
            if t.need_keyspace_reload then
              Obs_bytea.add_byte t.serialized_update 0xFF;
            let wb = t.wb in
            let u = snd t.sync_wait in
            let u' = snd t.wait_last_sync in
            let serialized_update = t.serialized_update in
            let () =
              t.dirty <- false;
              t.wb <- L.Batch.make ();
              t.need_keyspace_reload <- false;
              t.sync_wait <- Lwt.wait ();
              t.wait_last_sync <- Lwt.wait ();
              t.serialized_update <- Obs_bytea.create 128 in
            let () = control_throttling t in
            lwt () =
              Lwt_preemptive.detach
                (L.Batch.write ~sync:t.fsync t.db) wb
            and () = push_to_slaves t serialized_update in
              iter_pool := make_iter_pool t.db;
              Lwt.wakeup u ();
              Lwt.wakeup u' ();
              return ()
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

  let add_slave t ~unregister slave =
    t.slave_tbl <- IM.add slave.slave_id (unregister, slave) t.slave_tbl

  let close t =
    t.closed <- true;
    Lwt_condition.signal t.wait_for_dirty ();
    if t.dirty then fst t.wait_last_sync else return ()

  let perform t f =
    if t.closed then
      failwith "WRITEBATCH.perform: closed writebatch"
    else
      let waiter = fst t.sync_wait in
      let timestamp = Int64.of_float (Unix.gettimeofday () *. 1e6) in
      let tx = { t; valid = true; timestamp; } in
        try
          f tx;
          Lwt_condition.signal t.wait_for_dirty ();
          waiter
        with exn ->
          tx.valid <- false;
          raise exn

  let timestamp tx = tx.timestamp

  let delete_substring tx s off len =
    if not tx.valid then
      failwith "WRITEBATCH.delete_substring on expired transaction";
    tx.t.dirty <- true;
    L.Batch.delete_substring tx.t.wb s off len;
    if not (IM.is_empty tx.t.slave_tbl) then begin
      let u = tx.t.serialized_update in
        Obs_bytea.add_byte u 0;
        Obs_bytea.add_int32_le u len;
        Obs_bytea.add_substring u s off len;
    end

  let put_substring tx k o1 l1 v o2 l2 =
    if not tx.valid then
      failwith "WRITEBATCH.put_substring on expired transaction";
    tx.t.dirty <- true;
    L.Batch.put_substring tx.t.wb k o1 l1 v o2 l2;
    if not (IM.is_empty tx.t.slave_tbl) then begin
      let u = tx.t.serialized_update in
        Obs_bytea.add_byte u 1;
        Obs_bytea.add_int32_le u l1;
        Obs_bytea.add_substring u k o1 l1;
        Obs_bytea.add_int32_le u l2;
        Obs_bytea.add_substring u v o2 l2;
    end

  let need_keyspace_reload tx =
    tx.t.need_keyspace_reload <- true

  let put tx k v =
    put_substring tx k 0 (String.length k) v 0 (String.length v)
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

module EXTMAP(ORD : Map.OrderedType) =
struct
  include Map.Make(ORD)

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

module M = EXTMAP(String)

let cmp_table t1 t2 =
  String.compare (string_of_table t1) (string_of_table t2)

module TM = EXTMAP(struct
                     type t = table
                     let compare = cmp_table
                   end)

module TY : sig
  type keyspace_id = private int
  val new_keyspace_id : unit -> keyspace_id
end =
struct
  type keyspace_id = int

  let new_keyspace_id =
    let n = ref 0 in
      (fun () -> incr n; !n)
end

open TY

module H = Hashtbl.Make(struct
                            type t = keyspace_id
                            let hash (n : keyspace_id) = (n :> int)
                            let equal a b = (a == b)
                          end)

module Notif_queue : sig
  type 'a t
  val empty : 'a t
  val push : 'a -> 'a t -> 'a t
  val iter : ('a -> unit) -> 'a t  -> unit
end =
struct
  type 'a t = 'a list

  let empty = []

  (* TODO: further "compression" by keeping set of previous notifications? *)
  let push x = function
      y :: _ as l when y = x -> l
    | l -> x :: l

  let iter f t = List.iter f (List.rev t)
end

type keyspace_name = string
type topic = string
type subscription_descriptor = { subs_ks : keyspace_name; subs_topic : topic }

type subs_stream = string Lwt_stream.t * (string option -> unit)
type mutable_string_set = (string, unit) Hashtbl.t

module DIRTY_FLAG : sig
  type t

  val make : unit -> t
  val hash : t -> int
  val equal : t -> t -> bool
  val set : t -> unit
  val is_set : t -> bool
end =
struct
  type t = { id : int; mutable set : bool; }

  let new_id = let n = ref 0 in (fun () -> incr n; !n)

  let make () = { id = new_id (); set = false }

  let hash t = t.id (* TODO: use integer hash function? *)
  let equal t1 t2 = t1.id = t2.id

  let set t = t.set <- true
  let is_set t = t.set
end

module DIRTY_FLAGS =
struct
  module W = Weak.Make(DIRTY_FLAG)
  type t = { mutable nelms : int; set : W.t; decr_counter : DIRTY_FLAG.t -> unit; }

  let create n =
    let rec t = { nelms = 0; set = W.create n; decr_counter; }
    and decr_counter _ = t.nelms <- t.nelms - 1
    in t

  let add t x =
    if not (W.mem t.set x) then begin
      t.nelms <- t.nelms + 1;
      Gc.finalise t.decr_counter x;
      W.add t.set x
    end

  let remove t x =
    if W.mem t.set x then begin
      t.nelms <- t.nelms - 1;
      W.remove t.set x
    end

  let iter f t = W.iter f t.set

  let is_empty t = t.nelms = 0
end

module rec TYPES : sig
  type table_idx = int

  type db =
      {
        basedir : string;
        db : L.db Miniregion.t;
        keyspaces : (string, keyspace_proto * WKS.t) Hashtbl.t;
        mutable use_thread_pool : bool;
        load_stats : Obs_load_stats.t;
        mutable writebatch : WRITEBATCH.t;
        db_iter_pool : L.iterator Lwt_pool.t ref;
        (* We use an iterator pool to reuse iterators for read committed
         * transactions whenever possible. After an update, the db_iter_pool is
         * replaced by a new one (so that only new iterators are used). *)

        reopen : ?new_slave:slave -> unit -> L.db;
        mutable slaves : slave IM.t;

        (* Mandates whether even "short requests" are to be detached when
         * use_thread_pool is set. *)
        assume_page_fault : bool;
        fsync : bool;

        mutable locks : (string, Obs_shared_mutex.t) WeakTbl.t M.t;
        (* keyspace name -> lock name -> weak ref to lock *)

        subscriptions : (subscription_descriptor, subs_stream H.t) Hashtbl.t;
      }

  and keyspace =
    {
      ks_db : db; ks_name : string; ks_id : int;
      ks_unique_id : keyspace_id;
      mutable ks_tables : (table, int) Hashtbl.t;
      mutable ks_rev_tables : (int, table) Hashtbl.t;
      ks_tx_key : transaction Lwt.key;
      ks_subs_stream : subs_stream;
      ks_subscriptions : mutable_string_set;
      ks_watches : (table_idx * key, DIRTY_FLAGS.t * (column_name, DIRTY_FLAGS.t) Hashtbl.t) Hashtbl.t;
    }

  and keyspace_proto = Proto of keyspace

  and transaction =
      {
        mutable deleted_keys : S.t TM.t; (* table -> key set *)
        mutable added_keys : S.t TM.t; (* table -> key set *)
        mutable added : string column M.t M.t TM.t; (* table -> key -> column name -> column *)
        mutable deleted : S.t M.t TM.t; (* table -> key -> column set *)
        repeatable_read : bool;
        iter_pool : L.iterator Lwt_pool.t ref;
        ks : keyspace;
        mutable backup_writebatch : L.writebatch Lazy.t;
        (* approximate size of data written in current backup_writebatch *)
        mutable backup_writebatch_size : int;
        outermost_tx : transaction;
        mutable tx_locks : Obs_shared_mutex.t M.t;
        dump_buffer : Obs_bytea.t;
        mutable tx_notifications : string Notif_queue.t;
        mutable tainted : DIRTY_FLAG.t option;
      }
end = TYPES

and WKS : Weak.S with type data = TYPES.keyspace =
  Weak.Make(struct
              type t = TYPES.keyspace
              let hash t = (t.TYPES.ks_unique_id :> int)
              let equal t1 t2 = TYPES.(t1.ks_unique_id = t2.ks_unique_id)
            end)

include TYPES

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
           | Some (_, table) -> Hashtbl.add h (table_of_string table) (int_of_string v);
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
               let ks_unique_id = new_keyspace_id () in
               let ks_name = keyspace_name in
               let ks_id = int_of_string v in
               let ks_tables = read_keyspace_tables lldb ks_name in
               let ks_rev_tables = Hashtbl.create (Hashtbl.length ks_tables) in
               let ks_tx_key = Lwt.new_key () in
               let ks_subs_stream = Lwt_stream.create () in
               let ks_subscriptions = Hashtbl.create 13 in
                 Hashtbl.iter
                   (fun k v -> Hashtbl.add ks_rev_tables v k)
                   ks_tables;
                 Hashtbl.add h keyspace_name
                   { ks_db; ks_id; ks_name; ks_tables; ks_rev_tables;
                     ks_unique_id; ks_tx_key; ks_subs_stream; ks_subscriptions;
                     ks_watches = Hashtbl.create 13;
                   };
                 true)
      lldb
      Obs_datum_encoding.keyspace_table_prefix;
    h

let reload_keyspaces t lldb =
  let new_keyspaces = read_keyspaces t lldb in
    (* we have to update the existing keyspaces in-place (otherwise new tables
     * would not appear until the client gets a new keyspace with
     * register_keyspace or get_keyspace) *)
    Hashtbl.iter
      (fun name ks ->
         try
           let (Proto old_ks, set) = Hashtbl.find t.keyspaces name in
           let ks =
             { old_ks with ks_tables = ks.ks_tables;
                           ks_rev_tables = ks.ks_rev_tables;
             }
           in
           (* replace the proto *)
             Hashtbl.replace t.keyspaces name (Proto ks, set);
             (* and then update ks_tables and ks_rev_tables in actual
              * keyspaces in use *)
             let old_kss = WKS.fold (fun ks l -> ks :: l) set [] in
               (* we must assume that ks_name and ks_id are unchanged;
                * anything else would mean very bad news *)
               List.iter
                 (fun old_ks ->
                    old_ks.ks_tables <- ks.ks_tables;
                    old_ks.ks_rev_tables <- ks.ks_rev_tables)
                 old_kss
         with Not_found ->
           (* new keyspace: register proto and new WKS set *)
           Hashtbl.add t.keyspaces name (Proto ks, WKS.create 13);
           (* also initialize the entry in the lock table *)
           t.locks <- M.add name (WeakTbl.create ()) t.locks)
      new_keyspaces

let close_db t =
  Miniregion.use t.db
    (fun lldb ->
       lwt () = WRITEBATCH.close t.writebatch in
         L.close lldb;
         return ())

let throttling t = WRITEBATCH.throttling t.writebatch

let open_db
      ?(write_buffer_size = 4 * 1024 * 1024)
      ?(block_size = 4096)
      ?(max_open_files = 1000)
      ?(assume_page_fault = false)
      ?(unsafe_mode = false)
      basedir =
  let lldb = L.open_db
             ~write_buffer_size ~block_size ~max_open_files
             ~comparator:Obs_datum_encoding.custom_comparator basedir in
  let db_iter_pool = ref (make_iter_pool lldb) in
  let fsync = not unsafe_mode in
  let writebatch =
    WRITEBATCH.make lldb db_iter_pool ~fsync
  in
    (* ensure we have a end_of_db record *)
    if not (L.mem lldb Obs_datum_encoding.end_of_db_key) then
      L.put ~sync:true lldb Obs_datum_encoding.end_of_db_key (String.make 8 '\000');
    let rec db =
      { basedir; db = Miniregion.make lldb;
        keyspaces = Hashtbl.create 13;
        use_thread_pool = false;
        load_stats = Obs_load_stats.make [1; 60; 300; 900];
        writebatch; db_iter_pool; reopen; slaves = IM.empty;
        assume_page_fault; fsync = fsync;
        locks = M.empty;
        subscriptions = Hashtbl.create 13;
      }
    and reopen ?new_slave () =
      let lldb = L.open_db
                    ~write_buffer_size ~block_size ~max_open_files
                    ~comparator:Obs_datum_encoding.custom_comparator
                    basedir in
      let unregister id =
        db.slaves <- IM.remove id db.slaves
      in
        db.db_iter_pool := make_iter_pool lldb;
        db.writebatch <- WRITEBATCH.make lldb db.db_iter_pool ~fsync:db.fsync;
        Option.may
          (fun slave -> db.slaves <- IM.add slave.slave_id slave db.slaves)
          new_slave;
        (* don't forget to register slaves in new writebatch! *)
        IM.iter
          (fun id slave -> WRITEBATCH.add_slave ~unregister db.writebatch slave)
          db.slaves;
        lldb
    in
      Gc.finalise (fun db -> ignore (close_db db)) db;
      reload_keyspaces db lldb;
      db

let reset_iter_pool t =
  Miniregion.use t.db
    (fun lldb -> t.db_iter_pool := make_iter_pool lldb; return ())

let use_thread_pool db v = db.use_thread_pool <- v

let expect_short_request_key = Lwt.new_key ()

let detach_ks_op ks f x =
  if not ks.ks_db.use_thread_pool then
    return (f x)
  else begin
    let must_detach = match Lwt.get expect_short_request_key with
        Some true -> ks.ks_db.assume_page_fault
      | _ -> true
    in if must_detach then Lwt_preemptive.detach f x else return (f x)
  end

(* FIXME: determine suitable constant *)
let max_keys_in_short_request = 128

let short_request x f =
  Lwt.with_value expect_short_request_key (Some x) f

let list_keyspaces t =
  Hashtbl.fold (fun k v l -> k :: l) t.keyspaces [] |>
  S.of_list (* removes duplicates *) |> S.to_list |> return

let get_keyspace_set t ks_name proto =
  try snd (Hashtbl.find t.keyspaces ks_name)
  with Not_found ->
    let s = WKS.create 13 in
      Hashtbl.add t.keyspaces ks_name (proto, s);
      s

let terminate_keyspace_subs ks =
  let empty_topics =
    snd ks.ks_subs_stream None;
    let subs_ks = ks.ks_name in
      Hashtbl.fold
        (fun subs_topic _ l ->
           let k =  { subs_ks; subs_topic; } in
           try
             let t = Hashtbl.find ks.ks_db.subscriptions k in
               H.remove t ks.ks_unique_id;
               if H.length t = 0 then k :: l
               else l
           with Not_found -> l)
        ks.ks_subscriptions []
  in List.iter (Hashtbl.remove ks.ks_db.subscriptions) empty_topics

let clone_keyspace (Proto proto) =
  let ks =
    { (* we copy explicitly so we get a compile-time complaint if we add a
       * new field to the record  *)
      ks_db = proto.ks_db; ks_id = proto.ks_id; ks_name = proto.ks_name;
      ks_unique_id = new_keyspace_id ();
      ks_tables = proto.ks_tables; ks_rev_tables = proto.ks_rev_tables;
      ks_tx_key = Lwt.new_key ();
      ks_subs_stream = Lwt_stream.create ();
      ks_subscriptions = Hashtbl.create 13;
      ks_watches = proto.ks_watches;
    }
  in Gc.finalise terminate_keyspace_subs ks;
     ks

let register_keyspace t ks_name =
  try
    let proto = fst (Hashtbl.find t.keyspaces ks_name) in
    (* we must create a new keyspace with new tx_key and lock table *)
    let new_ks = clone_keyspace proto in
      WKS.add (get_keyspace_set t ks_name proto) new_ks;
      return new_ks
  with Not_found ->
    let max_id = Hashtbl.fold (fun _ (Proto p, _) id -> max p.ks_id id) t.keyspaces 0 in
    let ks_id = max_id + 1 in
    let wait_sync =
      WRITEBATCH.perform t.writebatch begin fun b ->
        WRITEBATCH.put b
          (Obs_datum_encoding.keyspace_table_key ks_name)
          (string_of_int ks_id);
        WRITEBATCH.need_keyspace_reload b;
      end in
    lwt () = wait_sync in
    let proto =
      Proto
        { ks_db = t; ks_id; ks_name;
          ks_unique_id = new_keyspace_id ();
          ks_tables = Hashtbl.create 13;
          ks_rev_tables = Hashtbl.create 13;
          ks_tx_key = Lwt.new_key ();
          ks_subs_stream = Lwt_stream.create ();
          ks_subscriptions = Hashtbl.create 13;
          ks_watches = Hashtbl.create 13;
        } in
    let new_ks = clone_keyspace proto in
      WKS.add (get_keyspace_set t ks_name proto) new_ks;
      (* be careful not to overwrite t.locks entry if already present *)
      if not (M.mem ks_name t.locks) then
        t.locks <- M.add ks_name (WeakTbl.create ()) t.locks;
      return new_ks

let register_keyspace =
  (* concurrent register_keyspace ops for the same name would be problematic:
   * the keyspace could be registered more than once (with different ids)! *)
  let mutex = Lwt_mutex.create () in
    (fun t ks_name ->
       Lwt_mutex.with_lock mutex (fun () -> register_keyspace t ks_name))

let register_keyspace t ks_name =
  Miniregion.use t.db (fun lldb -> register_keyspace t ks_name)

let get_keyspace t ks_name =
  if Hashtbl.mem t.keyspaces ks_name then begin
    lwt ks = register_keyspace t ks_name in
      return (Some ks)
  end else
    return None

let keyspace_name ks = ks.ks_name

let keyspace_id ks = (ks.ks_unique_id :> int)

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
           detach_ks_op ks (collect_tables it) [] >|= List.sort cmp_table)

let table_size_on_disk ks table =
  match find_maybe ks.ks_tables table with
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

let key_range_size_on_disk ks ?first ?up_to table =
  match find_maybe ks.ks_tables table with
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

let register_new_table_and_add_to_writebatch_aux put ks wb table =
  let last = Hashtbl.fold (fun _ idx m -> max m idx) ks.ks_tables 0 in
  (* TODO: find first free one (if existent) LT last *)
  let table_id = last + 1 in
  let k = Obs_datum_encoding.Keyspace_tables.ks_table_table_key
            ks.ks_name (string_of_table table) in
  let v = string_of_int table_id in
    put wb k v;
    Hashtbl.add ks.ks_tables table table_id;
    Hashtbl.add ks.ks_rev_tables table_id table;
    table_id

let register_new_table_and_add_to_writebatch =
  register_new_table_and_add_to_writebatch_aux L.Batch.put

let register_new_table_and_add_to_writebatch' =
  register_new_table_and_add_to_writebatch_aux
    (fun wb k v ->
       WRITEBATCH.put wb k v;
       WRITEBATCH.need_keyspace_reload wb)

let find_col_watches ks table key =
  (* fast path for common case: no watches *)
  match Hashtbl.length ks.ks_watches with
      0 -> None
    | _ ->
      try
        let s, col_watches = Hashtbl.find ks.ks_watches (table, key) in
          DIRTY_FLAGS.iter DIRTY_FLAG.set s;
          if Hashtbl.length col_watches > 0 then Some col_watches
          else None
      with Not_found -> None

let set_dirty_flags col_watches column =
  match col_watches with
      None -> ()
    | Some col_watches ->
        try
          DIRTY_FLAGS.iter DIRTY_FLAG.set (Hashtbl.find col_watches column)
        with Not_found -> ()

let rec transaction_aux with_iter_pool ks f =
  match Lwt.get ks.ks_tx_key with
    | None ->
        with_iter_pool ks.ks_db None begin fun ~iter_pool ~repeatable_read ->
          let rec tx =
            { added_keys = TM.empty; deleted_keys = TM.empty;
              added = TM.empty; deleted = TM.empty;
              repeatable_read; iter_pool; ks;
              backup_writebatch = lazy (L.Batch.make ());
              backup_writebatch_size = 0;
              outermost_tx = tx; tx_locks = M.empty;
              dump_buffer = Obs_bytea.create 16;
              tx_notifications = Notif_queue.empty;
              tainted = None;
            } in
          try_lwt
            lwt y = Lwt.with_value ks.ks_tx_key (Some tx) (fun () -> f tx) in
              commit_outermost_transaction ks tx >>
              return y
          finally
            (* release tx_locks *)
            M.iter (fun name m -> Obs_shared_mutex.unlock m) tx.tx_locks;
            return ()
        end
    | Some parent_tx ->
        with_iter_pool
          ks.ks_db (Some parent_tx) begin fun ~iter_pool ~repeatable_read ->
            let tx = { parent_tx with iter_pool; repeatable_read; } in
            lwt y = Lwt.with_value ks.ks_tx_key (Some tx) (fun () -> f tx) in
              parent_tx.deleted_keys <- tx.deleted_keys;
              parent_tx.added <- tx.added;
              parent_tx.deleted <- tx.deleted;
              parent_tx.added_keys <- tx.added_keys;
              parent_tx.tx_notifications <- tx.tx_notifications;
              return y
          end

and commit_outermost_transaction ks tx =
  (* early termination for read-only or NOP txs if:
   * * backup_writebatch has not be forced (i.e., not used for bulk
   *   load)
   * * no columns have been added / deleted *)
  if not (Lazy.lazy_is_val tx.backup_writebatch) &&
     TM.is_empty tx.deleted && TM.is_empty tx.added
  then begin
    Notif_queue.iter (notify ks) tx.tx_notifications;
    return ()
  end

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
             Lwt_preemptive.detach
               (L.Batch.write ~sync:ks.ks_db.fsync lldb) b >>
             reset_iter_pool ks.ks_db >>
             return ())
    end >>

    (* write normal data and wait for group commit sync *)
    let datum_key = Obs_bytea.create 13 in
    let timestamp = Int64.of_float (Unix.gettimeofday () *. 1e6) in
    let bytes_written = ref 0 in
    let cols_written = ref 0 in

    let wait_sync =
      WRITEBATCH.perform tx.ks.ks_db.writebatch begin fun b ->
        begin match tx.tainted with
            None -> ()
          | Some flag -> if DIRTY_FLAG.is_set flag then raise Dirty_data
        end;
        TM.iter
          (fun table m ->
             match find_maybe ks.ks_tables table with
                 Some table ->
                   M.iter
                     (fun key s ->
                        let col_watches = find_col_watches ks table key in
                          S.iter
                            (fun column ->
                               set_dirty_flags col_watches column;
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
        TM.iter
          (fun table m ->
             let table = match find_maybe ks.ks_tables table with
                 Some t -> t
               | None ->
                   (* must add table to keyspace table list *)
                   register_new_table_and_add_to_writebatch' ks b table
             in M.iter
               (fun key m ->
                  let col_watches = find_col_watches ks table key in
                    M.iter
                      (fun column c ->
                         set_dirty_flags col_watches column;
                         Obs_datum_encoding.encode_datum_key datum_key ks.ks_id
                           ~table ~key ~column
                           ~timestamp:(match c.timestamp with
                                           No_timestamp -> timestamp
                                         | Timestamp x -> x);
                         let klen = Obs_bytea.length datum_key in
                         let vlen = String.length c.data in
                           incr cols_written;
                           bytes_written := !bytes_written + klen + vlen;
                           WRITEBATCH.put_substring b
                             (Obs_bytea.unsafe_string datum_key) 0 klen
                             c.data 0 vlen)
                      m)
               m)
          tx.added;
      end in
    let stats = tx.ks.ks_db.load_stats in
    lwt () = wait_sync in
      Obs_load_stats.record_writes stats 1;
      Obs_load_stats.record_bytes_wr stats !bytes_written;
      Obs_load_stats.record_cols_wr stats !cols_written;
      (* we deliver notifications _after_ actual commit
       * (otherwise, new data wouldn't be found if another client
       *  performs a query right away) *)
      Notif_queue.iter (notify tx.ks) tx.tx_notifications;
      return ()
  end

and notify ks topic =
  let k = { subs_ks = ks.ks_name; subs_topic = topic; } in
    begin try
      let subs = Hashtbl.find ks.ks_db.subscriptions k in
        H.iter (fun _ (_, pushf) -> pushf (Some topic)) subs
    with _ -> () end

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
  match Lwt.get ks.ks_tx_key with
      None -> return ()
    | Some tx ->
      (* we use tx.outermost_tx.tx_locks instead of tx.tx_locks because tx_locks are
       * always associated to the outermost transaction! *)
      if M.mem name tx.outermost_tx.tx_locks then return ()
      else
        (* try to get mutex from global (per keyspace-name) map *)
        let mutex =
          let tbl = M.find ks.ks_name ks.ks_db.locks in
            match WeakTbl.get tbl name with
                Some mutex -> mutex
              | None ->
                  (* not found (maybe GCed because no refs left to it), create
                   * a new one *)
                  let mutex = Obs_shared_mutex.create () in
                    WeakTbl.add tbl name mutex;
                    mutex
        in
          tx.outermost_tx.tx_locks <- M.add name mutex tx.outermost_tx.tx_locks;
          Obs_shared_mutex.lock ~shared mutex

let lock ks ~shared names = Lwt_list.iter_s (lock_one ks ~shared) names

let remove_watch_if_empty watches table key flag =
  try
    let k = (table, key) in
    let flags, col_watches = Hashtbl.find watches k in
      DIRTY_FLAGS.remove flags flag;
      if DIRTY_FLAGS.is_empty flags && Hashtbl.length col_watches = 0 then
        Hashtbl.remove watches k
  with Not_found -> ()

let setup_flag_key_cleanup watches table key flag =
  Gc.finalise (remove_watch_if_empty watches table key) flag

let remove_col_watch_if_empty watches table key column flag =
  try
    let k = (table, key) in
    let _, col_watches = Hashtbl.find watches k in
      begin try
        let flags = Hashtbl.find col_watches column in
          DIRTY_FLAGS.remove flags flag;
          if DIRTY_FLAGS.is_empty flags then Hashtbl.remove col_watches column;
      with Not_found -> ()
      end;
      remove_watch_if_empty watches table key flag;
  with Not_found -> ()

let setup_flag_column_cleanup watches table key column flag =
  Gc.finalise (remove_col_watch_if_empty watches table key column) flag

let get_tainted_flag tx =
  match tx.tainted with
      Some w -> w
    | None -> let x = DIRTY_FLAG.make () in
                tx.tainted <- Some x;
                x

let watch_key tx table key =
  match find_maybe tx.ks.ks_tables table with
      None -> ()
    | Some table ->
        let flag = get_tainted_flag tx in
        let flags, _ =
          try
            Hashtbl.find tx.ks.ks_watches (table, key)
          with Not_found ->
            let x = (DIRTY_FLAGS.create 2, Hashtbl.create 2) in
              Hashtbl.add tx.ks.ks_watches (table, key) x;
              x
        in DIRTY_FLAGS.add flags flag;
           setup_flag_key_cleanup tx.ks.ks_watches table key flag

let watch_keys ks table keys =
  match Lwt.get ks.ks_tx_key with
      None -> return ()
    | Some tx -> List.iter (watch_key tx table) keys; return ()

let watch_column tx table key column =
  match find_maybe tx.ks.ks_tables table with
      None -> ()
    | Some table ->
        let flag = get_tainted_flag tx in
        let _, col_watches =
          try
            Hashtbl.find tx.ks.ks_watches (table, key)
          with Not_found ->
            let x = (DIRTY_FLAGS.create 2, Hashtbl.create 2) in
              Hashtbl.add tx.ks.ks_watches (table, key) x;
              x in
        let flags =
          try
            Hashtbl.find col_watches column
          with Not_found ->
            let x = DIRTY_FLAGS.create 2 in
              Hashtbl.add col_watches column x;
              x
        in
          DIRTY_FLAGS.add flags flag;
          setup_flag_column_cleanup tx.ks.ks_watches table key column flag

let watch_columns ks table l =
  match Lwt.get ks.ks_tx_key with
      None -> return ()
    | Some tx ->
        List.iter (fun (k, cols) -> List.iter (watch_column tx table k) cols) l;
        return ()

(* string -> string ref -> int -> bool *)
let is_same_value v buf len =
  String.length v = !len && Obs_string_util.strneq v 0 !buf 0 !len

let is_column_deleted tx table =
  let deleted_col_table = tx.deleted |> TM.find_default M.empty table in
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
let with_exists_key tx table f =
  let datum_key = Obs_bytea.create 13 in
  let buf = ref "" in
  let key_buf = ref "" and key_len = ref 0 in
  let column_buf = ref "" and column_len = ref 0 in
  let key_buf_r = Some key_buf and key_len_r = Some key_len in
  let column_buf_r = Some column_buf and column_len_r = Some column_len in
  let is_column_deleted = is_column_deleted tx table in
  let table_r = ref (-1) in

  let exists_f it = begin fun key ->
    match find_maybe tx.ks.ks_tables table with
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
          (* we couldn't translate the table to a table_id *)
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
let fold_over_data_no_detach it tx table_name f acc ?first_column
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
              ~key:(Option.default "" first_key)
              ~column:(Option.default "" first_column)
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
                (* we go to the datum for the key+col, then back (it's exclusive) *)
                let datum_key =
                  Obs_datum_encoding.encode_datum_key_to_string tx.ks.ks_id
                    ~table
                    ~key:k ~column:(Option.default "" first_column)
                    ~timestamp:Int64.min_int
                in
                  it_seek tx.ks it datum_key 0 (String.length datum_key);
                  if IT.valid it then it_prev tx.ks it
        end;
        do_fold_over_data acc
    | None -> (* unknown table *) acc

let fold_over_data_no_detach = fold_over_data_no_detach

let fold_over_data tx table f ?first_column acc ~reverse ~first_key ~up_to_key =
  Lwt_pool.use !(tx.iter_pool)
    (fun it ->
       detach_ks_op tx.ks
         (fun () ->
            fold_over_data_no_detach it tx table f acc ?first_column
                 ~reverse ~first_key ~up_to_key)
         ())

let expand_key_range = function
    `Discrete _ | `Continuous _ as x -> x
  | `All -> `Continuous { first = None; up_to = None; reverse = false; }

let get_keys_aux init_f map_f fold_f tx table ?(max_keys = max_int) = function
    `Discrete l ->
      with_exists_key tx table
        (fun exists_key ->
           detach_ks_op tx.ks
             (fun () ->
                let s = S.of_list l in
                let s = S.diff s (TM.find_default S.empty table tx.deleted_keys) in
                let s =
                  S.filter
                    (fun k ->
                       S.mem k (TM.find_default S.empty table tx.added_keys) ||
                       exists_key k)
                    s
                in map_f s)
             ())

  | `Continuous { first; up_to; reverse; } ->
      (* we recover all the keys added in the transaction *)
      let s = TM.find_default S.empty table tx.added_keys in
      let s =
        let first, up_to = if reverse then (up_to, first) else (first, up_to) in
          S.subset ?first ?up_to s in
      (* now s contains the added keys in the wanted range *)

      let is_key_deleted =
        let s = TM.find_default S.empty table tx.deleted_keys in
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
  let range = expand_key_range range in
  lwt keys_in_lexicographic_order =
    get_keys_aux
      (fun s -> s)
      (fun s -> s)
      (fun s key_buf key_len -> S.add (String.sub key_buf 0 key_len) s)
      tx table ~max_keys range >|= S.to_list in
  let all_keys_in_wanted_order =
    match range with
        `Continuous { reverse = true; } -> List.rev keys_in_lexicographic_order
      | _ -> keys_in_lexicographic_order
  in return (List.take max_keys all_keys_in_wanted_order)

let exist_keys tx table keys =
  with_exists_key tx table
    (fun exists_key ->
       detach_ks_op tx.ks
         (fun keys ->
            List.map
              (fun key ->
                 S.mem key (TM.find_default S.empty table tx.added_keys) ||
                 (not (S.mem key (TM.find_default S.empty table tx.deleted_keys)) &&
                  exists_key key))
              keys)
         keys)

let exists_key tx table key =
  short_request true
    (fun () ->
       exist_keys tx table [key] >>= function
         | true :: _ -> return true
         | _ -> return false)

let count_keys tx table range =
  let range = expand_key_range range in
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
  | `Discrete l ->
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
  | `Continuous { first; up_to; reverse; } ->
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
    `All -> (fun ~buf ~len -> true)
  | `Union [] -> (fun ~buf ~len -> false)
  | `Union [x] -> simple_column_range_selector x
  | (`Discrete _ | `Continuous _) as x -> simple_column_range_selector x
  | `Union l ->
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
        (`Discrete (S.to_list (columns_needed_for_predicate x)))

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

(* return the ?first_column to give to fold_over_data based on the wanted
 * predicate columns and column range *)
let rec column_range_first_column ~reverse ~pred_columns_wanted_from_disk
      (column_range : column_range) : string option =
  let pick = if reverse then max else min in
  let bound =
    match column_range with
        `Discrete (c :: cols) ->
          Some (List.fold_left (fun c1 c2 -> pick c1 c2) c cols)
      | `Discrete [] -> None
      | `All -> None
      | `Continuous { first; up_to; reverse = col_rev; } ->
          begin
            match reverse, col_rev with
                false, false -> first
              | false, true -> up_to
              | true, false -> up_to
              | true, true -> first
          end
      | `Union r ->
          match
            List.map
              (fun r ->
                 column_range_first_column
                   ~reverse ~pred_columns_wanted_from_disk
                   (r :> column_range))
              r
          with
            | [] | None :: _ -> None
            | (Some _ as fst_col) :: fst_cols ->
                List.fold_left
                  (fun prev v -> match prev, v with
                       None, _ | _, None -> None
                     | Some prev, Some v -> Some (pick prev v))
                  fst_col fst_cols
  in
    if S.is_empty pred_columns_wanted_from_disk then begin
      if not reverse then bound
      else Option.map (fun s -> s ^ "\x00") bound
    end else match reverse, bound with
        _, None -> None
      | false, Some v ->
          Some (min v (S.min_elt pred_columns_wanted_from_disk))
      | true, Some v ->
          Some (max v (S.max_elt pred_columns_wanted_from_disk) ^ "\x00")

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
      (fun k -> not (S.mem k (TM.find_default S.empty table tx.deleted_keys)))
      (List.sort String.compare l) in
  let reverse = match column_range with
      `Union [`Continuous { reverse; _ }] | `Continuous { reverse; _ } -> reverse
    | `Discrete _ | `Union _ | `All -> false in
  let columns_needed_for_predicate = columns_needed_for_predicate predicate in

  let fold_over_keys it (key_data_list, keys_so_far) key =
    if keys_so_far >= max_keys then (key_data_list, keys_so_far)
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

      let rev_cols1, rev_pred_cols1, _ =
        let first_key, up_to_key =
          if reverse then
            (Some (key ^ "\000"), Some key)
          else
            (Some key, Some (key ^ "\000")) in
        (* we take the columns needed for the predicate directly from the
         * tx data and inject it them into fold_over_data *)
        let cols_in_mem_map =
          TM.find_default M.empty table tx.added |>
          M.find_default M.empty key in
        let pred_cols_in_mem, pred_cols_in_mem_set =
          M.fold
            (fun name col ((l, s) as acc) ->
               if column_needed_for_predicate name (String.length name) then
                 (col :: l, S.add name s)
               else acc)
            cols_in_mem_map
            ([], S.empty) in
        let pred_columns_wanted_from_disk =
          S.diff columns_needed_for_predicate pred_cols_in_mem_set
        in
          fold_over_data_no_detach it tx table fold_datum
             ([], pred_cols_in_mem, pred_columns_wanted_from_disk)
             ~reverse ~first_key ~up_to_key
             ?first_column:(column_range_first_column
                              ~reverse
                              ~pred_columns_wanted_from_disk
                              column_range) in

      let rev_cols2 =
        M.fold
          (fun colname col cols ->
             let len = String.length colname in
             if not (column_selected colname len) then cols
             else col :: cols)
          (tx.added |>
           TM.find_default M.empty table |> M.find_default M.empty key)
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
              None -> (key_data_list, keys_so_far)
            | Some x -> (x :: key_data_list, keys_so_far + 1)
        end else begin
          (key_data_list, keys_so_far)
        end in

  lwt key_data_list, _ =
    Lwt_pool.use !(tx.iter_pool)
      (fun it -> detach_ks_op tx.ks (List.fold_left (fold_over_keys it) ([], 0)) l) in
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
             (fun colname col l ->
                if column_selected colname (String.length colname) then col :: l
                else l)
             key_data_in_mem
             []
         in (key, if reverse then rev cols else cols) :: l)
      (let first, up_to =
         if reverse then (up_to, first) else (first, up_to)
       in M.submap ?first ?up_to (TM.find_default M.empty table tx.added))
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
            TM.find_default M.empty table tx.added |>
            M.find_default M.empty new_key in
          let pred_cols_in_mem, pred_cols_in_mem_set =
            M.fold
              (fun colname col ((l, s) as acc) ->
                 if column_needed_for_predicate colname (String.length colname) then
                   (col :: l, S.add colname s)
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
          let m = TM.find_default M.empty table tx.added |>
                  M.find_default M.empty last_key
          in
            M.fold
              (fun colname col l ->
                 if column_needed_for_predicate colname (String.length colname) then
                   col :: l
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
             (fun colname col ((cols, pred_cols) as acc) ->
                let len = String.length colname in
                let selected = column_selected colname len in
                let needed_for_pred = column_needed_for_predicate colname len in
                  if not selected && not needed_for_pred then
                    acc
                  else begin
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
       in M.submap ?first ?up_to (TM.find_default M.empty table tx.added))
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
      _, `Discrete l ->
        short_request
          (max_keys < max_keys_in_short_request ||
           List.length l < max_keys_in_short_request)
          (fun () ->
             get_slice_aux_discrete
               postproc_keydata get_keydata_key ~keep_columnless_keys tx table
               ~max_keys ~max_columns ~decode_timestamps
               l ?predicate column_range)
    | None, `Continuous range ->
        get_slice_aux_continuous_no_predicate
          postproc_keydata get_keydata_key ~keep_columnless_keys tx table
          ~max_keys ~max_columns ~decode_timestamps
          range column_range
    | Some predicate, `Continuous range ->
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
  let key_range = expand_key_range key_range in
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
  let is_columns = function `Discrete _ -> true | `Continuous _ -> false in
  let max_columns = match max_columns with
      Some m -> m
    | None -> match column_range with
        | `Union l when List.for_all is_columns l ->
            let columns =
              List.fold_left
                (fun s x -> match x with
                     `Discrete l -> List.fold_left (fun s c -> S.add c s) s l
                   | `Continuous _ -> s)
                S.empty
                l
            in S.cardinal columns
        | `Discrete l ->
            let columns = List.fold_left (fun s c -> S.add c s) S.empty l in
              S.cardinal columns
        | `Continuous _ | `Union _ | `All -> max_int
  in
    Obs_load_stats.record_reads tx.ks.ks_db.load_stats 1;
    get_slice_aux
      postproc_keydata get_keydata_key ~keep_columnless_keys:false tx table
      ~max_keys ~max_columns ~decode_timestamps key_range ?predicate column_range

let get_slice_values tx table
      ?(max_keys = max_int) key_range columns =
  let key_range = expand_key_range key_range in
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
       (`Union [`Discrete columns])
  in
    Obs_load_stats.record_reads tx.ks.ks_db.load_stats 1;
    Obs_load_stats.record_bytes_rd tx.ks.ks_db.load_stats !bytes_read;
    Obs_load_stats.record_cols_rd tx.ks.ks_db.load_stats !cols_read;
    return ret

let get_slice_values_with_timestamps tx table
      ?(max_keys = max_int) key_range columns =
  let key_range = expand_key_range key_range in
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
       (`Union [`Discrete columns])
  in
    Obs_load_stats.record_reads tx.ks.ks_db.load_stats 1;
    Obs_load_stats.record_bytes_rd tx.ks.ks_db.load_stats !bytes_read;
    Obs_load_stats.record_cols_rd tx.ks.ks_db.load_stats !cols_read;
    return ret

let get_columns tx table ?(max_columns = max_int) ?decode_timestamps
                key column_range =
  match_lwt
    get_slice tx table ~max_columns ?decode_timestamps
      (`Discrete [key]) column_range
  with
    | (_, { last_column = last_column;
            columns = ((_ :: _ as columns)) } :: _ ) ->
        return (Some (last_column, columns))
    | _ -> return None

let get_column_values tx table key columns =
  match_lwt get_slice_values tx table (`Discrete [key]) columns with
    | (_, (_, l):: _) -> return l
    | _ -> assert false

let get_column tx table key column_name =
  match_lwt
    get_columns tx table key ~decode_timestamps:true ~max_columns:1
      (`Union [`Discrete [column_name]])
  with
      Some (_, c :: _) -> return (Some (c.data, c.timestamp))
    | _ -> return None

let put_columns tx table key columns =
  tx.added_keys <- TM.modify (S.add key) S.empty table tx.added_keys;
  tx.deleted_keys <- TM.modify_if_found (S.remove key) table tx.deleted_keys;
  tx.deleted <-
    TM.modify_if_found
      (fun m ->
         if M.is_empty m then m
         else
           M.modify_if_found
             (fun s ->
                List.fold_left (fun s c -> S.remove c.name s) s columns)
             key m)
      table tx.deleted;
  tx.added <-
    TM.modify
      (fun m ->
         (M.modify
            (fun m ->
               List.fold_left
                 (fun m c -> M.add c.name c m)
                 m columns)
            M.empty key m))
      M.empty table tx.added;
  return ()

let put_multi_columns tx table data =
  Lwt_list.iter_s
    (fun (key, columns) -> put_columns tx table key columns)
    data

let rec put_multi_columns_no_tx ks table data =
  let cols_written = ref 0 in
  let bytes_written = ref 0 in
  let datum_key = put_multi_columns_no_tx_buf in
  let wait_sync =
    WRITEBATCH.perform ks.ks_db.writebatch begin fun b ->
      let table =
        match find_maybe ks.ks_tables table with
          Some t -> t
        | None ->
            (* must add table to keyspace table list *)
            register_new_table_and_add_to_writebatch' ks b table
      in
        List.iter
          (fun (key, columns) ->
             let col_watches = find_col_watches ks table key in
               List.iter
                 (fun c ->
                    set_dirty_flags col_watches c.name;
                    Obs_datum_encoding.encode_datum_key datum_key ks.ks_id
                      ~table ~key ~column:c.name
                      ~timestamp:(match c.timestamp with
                                      No_timestamp -> WRITEBATCH.timestamp b
                                    | Timestamp x -> x);
                    let klen = Obs_bytea.length datum_key in
                    let vlen = String.length c.data in
                      incr cols_written;
                      bytes_written := !bytes_written + klen + vlen;
                      WRITEBATCH.put_substring b
                        (Obs_bytea.unsafe_string datum_key) 0 klen
                        c.data 0 vlen)
                 columns)
          data;
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
   * change under our feet (due to a concurrent Raw_dump.dump) *)
  Miniregion.use ks.ks_db.db
    (fun _ -> put_multi_columns_no_tx ks table data)

let delete_columns tx table key cols =
  tx.added <-
    TM.modify_if_found
      (fun m ->
         (M.modify
             (fun m -> List.fold_left (fun m c -> M.remove c m) m cols)
             M.empty key m))
      table tx.added;
  if M.is_empty (TM.find_default M.empty table tx.added |>
                 M.find_default M.empty key) then
    tx.added_keys <- TM.modify_if_found (S.remove key) table tx.added_keys;
  tx.deleted <-
    TM.modify
      (fun m ->
         M.modify
           (fun s -> List.fold_left (fun s c -> S.add c s) s cols)
           S.empty key m)
      M.empty table tx.deleted;
  return ()

let delete_key tx table key =
  match_lwt
    get_columns tx table ~max_columns:max_int ~decode_timestamps:false
      key `All
  with
      None -> return ()
    | Some (_, columns) ->
        lwt () = delete_columns tx table key
                   (List.map (fun c -> c.name) columns)
        in
          tx.deleted_keys <- TM.modify (S.add key) S.empty table tx.deleted_keys;
          return ()

let find_or_add find add h k =
  try find h k with Not_found -> add h k

let listen ks topic =
  let ks_name = ks.ks_name in
  let k = { subs_ks = ks_name; subs_topic = topic} in
  let tbl =
    find_or_add
      Hashtbl.find
      (fun h k -> let v = H.create 13 in Hashtbl.add h k v; v)
      ks.ks_db.subscriptions k
  in
    if not (H.mem tbl ks.ks_unique_id) then begin
      Hashtbl.replace ks.ks_subscriptions topic ();
      H.add tbl ks.ks_unique_id ks.ks_subs_stream;
    end;
    return ()

let unlisten ks topic =
  let ks_name = ks.ks_name in
  let k = { subs_ks = ks_name; subs_topic = topic} in
    begin try
      let tbl = Hashtbl.find ks.ks_db.subscriptions k in
        H.remove tbl ks.ks_unique_id;
        (* if there are no more listeners, remove the empty
         * subscriber table *)
        if H.length tbl = 0 then Hashtbl.remove ks.ks_db.subscriptions k;
        Hashtbl.remove ks.ks_subscriptions ks_name;
    with Not_found -> () end;
    return ()

let notify ks topic =
  match Lwt.get ks.ks_tx_key with
    | None -> notify ks topic; return ()
    | Some tx ->
        tx.tx_notifications <- Notif_queue.push topic tx.tx_notifications;
        return ()

let await_notifications ks =
  let stream = fst ks.ks_subs_stream in
  (* we use get_available_up_to to limit the stack footprint
   * (get_available is not tail-recursive) *)
  match Lwt_stream.get_available_up_to 500 stream with
      [] -> begin
        match_lwt Lwt_stream.get stream with
            None -> return []
          | Some x -> return [x]
      end
    | l -> return l

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
        Some table ->
          let col_watches = find_col_watches tx.ks table key in
            set_dirty_flags col_watches column;
            Obs_datum_encoding.encode_datum_key datum_key tx.ks.ks_id
              ~table ~key ~column ~timestamp
      | None ->
          (* register table first *)
          let table =
            register_new_table_and_add_to_writebatch tx.ks wb table_name in
          let col_watches = find_col_watches tx.ks table key in
            set_dirty_flags col_watches column;
            Obs_datum_encoding.encode_datum_key datum_key tx.ks.ks_id
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
               detach_ks_op tx.ks
                 (L.Batch.write ~sync:tx.ks.ks_db.fsync lldb) wb >>
               reset_iter_pool tx.ks.ks_db)
        end
      end >>
      return true
    end

let load_stats tx =
  return (Obs_load_stats.stats tx.ks.ks_db.load_stats)

let cursor_of_string = Obs_backup.cursor_of_string
let string_of_cursor = Obs_backup.string_of_cursor


type update =
    { up_buf : string; up_off : int; up_len : int;
      up_signal_ack : [`ACK | `NACK] Lwt.u option;
    }

type update_stream = update Lwt_stream.t

module Raw_dump =
struct
  type raw_dump =
      {
        id : int;
        timestamp : Int64.t;
        directory : string;
        update_stream : update Lwt_stream.t;
        push_update : update option -> unit;
      }

  let is_file fname =
    try
        match (Unix.stat fname).Unix.st_kind with
            Unix.S_REG -> true
          | _ -> false
    with Unix.Unix_error _ -> false

  let new_dump_id =
    let n = ref 0 in
      (fun () -> incr n; !n)

  let dump db =
    let dstdir = ref "" in
    let timestamp = ref 0L in
    let id = new_dump_id () in
    let update_stream, push_update = Lwt_stream.create () in
    let push_update_bytea b u =
      push_update
        (Some { up_buf = Obs_bytea.unsafe_string b;
                up_off = 0; up_len = Obs_bytea.length b;
                up_signal_ack = Some u; })
    in
      Miniregion.update_value db.db
        (fun lldb ->
           (* flush *)
           WRITEBATCH.close db.writebatch >>
           let () = L.close lldb in
           let new_slave =
             { slave_id = id; slave_push = push_update_bytea;
               slave_mode = Async; slave_mode_target = Sync;
               slave_pending_updates = 0;
             } in
           let lldb = db.reopen ~new_slave () in
             begin try_lwt
               timestamp := Int64.of_float (Unix.gettimeofday () *. 1e6);
               dstdir := Filename.concat db.basedir (sprintf "dump-%Ld" !timestamp);
               let is_file fname = is_file (Filename.concat db.basedir fname) in
               let hardlink_file fname =
                 let src = Filename.concat db.basedir fname in
                 let dst = Filename.concat !dstdir fname in
                   Unix.link src dst in
               let src_files =
                 Sys.readdir db.basedir |> Array.to_list |>
                 List.filter is_file
               in
                 Unix.mkdir !dstdir 0o755;
                 List.iter hardlink_file src_files;
                 return ()
             with exn ->
               (* FIXME: better log *)
               eprintf "Error in Raw_dump.dump: %s\n%!" (Printexc.to_string exn);
               return ()
             end >>
             return lldb) >>
    let ret = { id; directory = !dstdir; timestamp = !timestamp;
                update_stream; push_update; }
    in return ret

  let timestamp d = return d.timestamp

  let list_files d =
    Sys.readdir d.directory |> Array.to_list |>
    List.filter_map
      (fun fname ->
         let fullname = Filename.concat d.directory fname in
         let open Unix.LargeFile in
         let stat = stat fullname in
           match stat.st_kind with
               Unix.S_REG -> Some (fname, stat.st_size)
             | _ -> None)
    |> return

  let size d =
    list_files d >|=
    List.fold_left (fun s (_, fs) -> Int64.add s fs) 0L

  let md5digest_of_file fname =
    try_lwt
      let ic = open_in fname in
        try_lwt
          lwt digest =
            Lwt_preemptive.detach
              (fun () -> Cryptokit.hash_channel (Cryptokit.Hash.md5 ()) ic)
              ()
          in return (Some digest)
        finally
          close_in ic;
          return ()
    with _ -> return None

  let file_digest d fname =
    md5digest_of_file (Filename.concat d.directory fname)

  let open_file d ?offset fname =
    let fname = Filename.concat d.directory fname in
      try_lwt
        lwt ic = Lwt_io.open_file ~mode:Lwt_io.input fname in
          match offset with
              None -> return (Some ic)
            | Some off -> Lwt_io.set_position ic off >> return (Some ic)
      with _ -> return None

  let ign_unix_error f x =
    try f x with Unix.Unix_error _ -> ()

  let release d =
    Array.iter
      (fun fname -> ign_unix_error Unix.unlink (Filename.concat d.directory fname))
      (Sys.readdir d.directory);
    ign_unix_error Unix.rmdir d.directory;
    return ()
end

module Replication =
struct
  type _update = update
  type update = _update
  type _update_stream = update_stream
  type update_stream = _update_stream

  let get_update_stream d = return d.Raw_dump.update_stream
  let get_update = Lwt_stream.get

  let signal u x =
    (try Option.may (fun u -> Lwt.wakeup u x) u.up_signal_ack with _ -> ());
    return ()

  let ack_update u = signal u `ACK
  let nack_update u = signal u `NACK

  let is_sync_update u = (* FIXME *) return false

  let get_update_data u = return (u.up_buf, u.up_off, u.up_len)

  let update_of_string s off len =
    Some { up_buf = s; up_off = off; up_len = len; up_signal_ack = None; }

  let read_int32_le s n =
    let a = Char.code s.[n] in
    let b = Char.code s.[n+1] in
    let c = Char.code s.[n+2] in
    let d = Char.code s.[n+3] in
      a + (b lsl 8) + (c lsl 16) + (d lsl 24)

  let apply_update db update =
    Miniregion.use db.db begin fun lldb ->
      let must_reload = ref false in
      let wait_sync =
         WRITEBATCH.perform db.writebatch begin fun b ->
           let rec apply_update s n max =
             if n >= max then ()
             else begin
               match s.[n] with
                 | '\x01' -> (* add *)
                   let klen = read_int32_le s (n + 1) in
                   let vlen = read_int32_le s (n + 1 + 4 + klen) in
                     (* TODO: instead of writing as usual and letting
                      * WRITEBATCH serialize the change again (in case we have
                      * slaves), write locally (need some ?push:bool option in
                      * WRITEBATCH.put_substring and delete_substring) and
                      * forward the entire update as is *)
                     WRITEBATCH.put_substring b
                       s (n + 1 + 4) klen
                       s (n + 1 + 4 + klen + 4) vlen;
                     apply_update s (n + 1 + 4 + klen + 4 + vlen) max
                 | '\x00' -> (* delete *)
                   let klen = read_int32_le s (n + 1) in
                     WRITEBATCH.delete_substring b s (n + 1 + 4) klen;
                     apply_update s (n + 1 + 4 + klen) max
                 | '\xFF' -> (* reload keyspaces *)
                     must_reload := true;
                     apply_update s (n + 1) max
                 | _ -> (* unrecognized record, abort processing *)
                     (* FIXME: error out or just log? *)
                     ()
             end
           in apply_update
                update.up_buf update.up_off
                (update.up_off + update.up_len)
         end in
      lwt () = wait_sync in
        if !must_reload then reload_keyspaces db lldb;
        return ()
    end
end

let with_transaction ks f =
  match Lwt.get ks.ks_tx_key with
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
  match Lwt.get ks.ks_tx_key with
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

let get_property t property =
  Miniregion.use t.db (fun lldb -> return (L.get_property lldb property))

module RAW =
struct
  type keyspace_ = keyspace
  type keyspace = keyspace_
  let get_slice = get_slice
  let get_slice_values = get_slice_values
  let get_slice_values_with_timestamps = get_slice_values_with_timestamps
  let get_columns = get_columns
  let get_column_values = get_column_values
  let get_column = get_column
  let put_columns = put_columns
  let put_multi_columns = put_multi_columns
end

include (Obs_structured.Make(RAW) :
           Obs_structured.STRUCTURED with type keyspace := keyspace)
