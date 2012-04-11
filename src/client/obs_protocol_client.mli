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


module Make : functor(P : Obs_protocol.PAYLOAD) ->
sig
  include Obs_data_model.S with type backup_cursor = string
  include Obs_data_model.BACKUP_SUPPORT with type backup_cursor := backup_cursor

  module Replication : Obs_replication.REPLICATION_SERVER
   with type db := db and type raw_dump := Raw_dump.raw_dump

  val make :
    data_address:Unix.sockaddr ->
    Lwt_io.input_channel -> Lwt_io.output_channel -> db
  val close : db -> unit
end
