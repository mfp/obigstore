(** Lwt mutexes with shared/exclusing locking semantics. *)
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

(** Type of mutexes with shared/exclusive locking semantics. Locks are
  * obtained in the order in which they were requested, i.e., a process
  * trying to acquire an exclusive lock will not wait for an unbound amount of
  * time if other processes try to acquire shared locks in the meantime. *)
type t

(** Create a new mutex. *)
val create : unit -> t

(** [lock ?shared m] acquires a lock on the mutex [m]. [shared], defaulting to
  * [true], indicates whether the lock is shared. *)
val lock : ?shared:bool -> t -> unit Lwt.t

(** Unlock a mutex. *)
val unlock : t -> unit

(** Run the supplied function with the mutex locked. If an exception is
  * raised, the mutex will be unlocked and the exception will be re-raised.
  * @param shared whether the lock is shared (default: [true]) *)
val with_lock : ?shared:bool -> t -> (unit -> 'a Lwt.t) -> 'a Lwt.t

(** Returns whether the mutex is locked. *)
val is_locked : t -> bool

(** Returns whether the mutex is locked in shared mode. *)
val is_locked_shared : t -> bool

(** Returns whether the mutex is locked in exclusive mode. *)
val is_locked_exclusive : t -> bool

(** Returns [true] if there is no thread waiting to acquire a lock on the
  * mutex. *)
val is_empty : t -> bool

