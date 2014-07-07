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

(** Replication. *)

module type REPLICATION_COMMON =
sig
  type db
  type raw_dump
  type update
end

module type REPLICATION_CLIENT =
sig
  include REPLICATION_COMMON

  val update_of_string : string -> int -> int -> update option
  val apply_update : db -> update -> unit Lwt.t
end

module type REPLICATION_SERVER =
sig
  include REPLICATION_COMMON

  type update_stream

  val get_update_stream : raw_dump -> update_stream Lwt.t
  val get_update : update_stream -> update option Lwt.t
  val get_updates : update_stream -> update Lwt_stream.t
  val ack_update : update -> unit Lwt.t
  val nack_update : update -> unit Lwt.t

  val is_sync_update : update -> bool Lwt.t
  val get_update_data : update -> (string * int * int) Lwt.t
end
