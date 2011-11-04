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


module Make : functor(P : Obs_protocol.PAYLOAD) ->
sig
  include Obs_data_model.S with type backup_cursor = string
  include Obs_data_model.BACKUP_SUPPORT with type backup_cursor := backup_cursor
  val make : Lwt_io.input_channel -> Lwt_io.output_channel -> db
  val close : db -> unit

  (** {3} Asynchronous notifications *)

  (** [listen ks topìc] allows to receive notifications sent to the specified
    * [topic] in the keyspace [ks]. Note that [listen] is not affected by
    * surrounding transactions, i.e., the subscription is performed even if
    * the surrounding transaction is canceled. *)
  val listen : keyspace -> string -> unit Lwt.t

  (** [unlisten ks topìc] signals that further notifications sent to the [topic]
    * in the keyspace [ks] will not be received. Notifications that were
    * already queued in the server will not be discarded, however.
    * Note that [unlisten] is not affected by surrounding transactions, i.e.,
    * the unsubscription is performed even if the surrounding transaction is
    * canceled. *)
  val unlisten : keyspace -> string -> unit Lwt.t

  (** [notify ks topic] sends a notification associated to the given [topic]
    * in keyspace [ks], which will be received by all the connections that
    * performed [listen] on the same [ks]/[topic]. [notify] honors surrounding
    * transactions, i.e., the notification will be actually performed only
    * when/if the outermost surrounding transaction is committed, and no
    * notification is sent if any of the surrounding transactions is aborted.
    * *)
  val notify : keyspace -> string -> unit Lwt.t

  (** Returned queued notifications, blocking if there is none yet.
    * An empty list will be returned when there are no more queued
    * notifications and the underlying connection is closed.
    * *)
  val await_notifications : keyspace -> string list Lwt.t
end
