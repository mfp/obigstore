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

open Obigstore_core

include Data_model.S
include Data_model.BACKUP_SUPPORT with type backup_cursor := backup_cursor

(** Open or create a database in the given directory.
  * @param group_commit_period period for group commit in seconds
  *        (default 0.002, minimal value: 0.001) *)
val open_db : ?group_commit_period:float -> string -> db

(** Flush transactions waiting for group commit and close the DB.  All further
  * actions on it will raise an error. *)
val close_db : db -> unit Lwt.t

(** [use_thread_pool db x] indicates whether blocking operations should be
  * performed in a separate thread so as to avoid blocking. By default, all
  * operations are performed in the main thread. *)
val use_thread_pool : db -> bool -> unit
