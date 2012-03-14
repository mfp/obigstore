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


include Obs_data_model.S
include Obs_data_model.BACKUP_SUPPORT with type backup_cursor := backup_cursor

module Replication :
sig
  include Obs_replication.REPLICATION_SERVER
    with type db := db and type raw_dump := Raw_dump.raw_dump

  include Obs_replication.REPLICATION_CLIENT
    with type db := db and type raw_dump := Raw_dump.raw_dump
     and type update := update
end

(** Open or create a database in the given directory.
  *
  * @param group_commit_period period for group commit in seconds
  *        (default 0.002, minimal value: 0.001)
  *
  * @param assume_page_fault when true, even "short" requests are run in a
  * separate thread when use_thread_pool is set (default: false)
  *
  * @param unsafe_mode when true, do not fsync after writes (default: false)
  *
  * @param write_buffer_size
  * "Amount of data to build up in memory (backed by an unsorted log
  * on disk) before converting to a sorted on-disk file.
  * Larger values increase performance, especially during bulk loads.
  * Up to two write buffers may be held in memory at the same time,
  * so you may wish to adjust this parameter to control memory usage.
  * Also, a larger write buffer will result in a longer recovery time
  * the next time the database is opened. (default: 4MB)"
  *
  * @param block_size
  * "Approximate size of user data packed per block.  Note that the
  * block size specified here corresponds to uncompressed data.  The
  * actual size of the unit read from disk may be smaller if
  * compression is enabled.  This parameter can be changed dynamically.
  * (default: 4KB)"
  *
  * @param max_open_files
  * "Number of open files that can be used by the DB.  You may need to
  * increase this if your database has a large working set (budget
  * one open file per 2MB of working set). (default: 1000)"
  *)
val open_db :
  ?write_buffer_size:int ->
  ?block_size:int ->
  ?max_open_files:int ->
  ?group_commit_period:float ->
  ?assume_page_fault:bool ->
  ?unsafe_mode:bool ->
  string -> db

(** Flush transactions waiting for group commit and close the DB.  All further
  * actions on it will raise an error. *)
val close_db : db -> unit Lwt.t

(** [use_thread_pool db x] indicates whether blocking operations should be
  * performed in a separate thread so as to avoid blocking. By default, all
  * operations are performed in the main thread. *)
val use_thread_pool : db -> bool -> unit
