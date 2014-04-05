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

(** Utility functions for efficient database dumping. *)

(** Information returned by dump operation. *)
type dump_result =
   {
     dstdir      : string;
     all_files   : string list;
     added_files : string list;
   }

module Make : functor(D : Obs_data_model.RAW_DUMP) ->
sig
  (** @return directory the database was dumped to *)
  val dump_local : ?verbose:bool -> ?destdir:string -> D.raw_dump -> dump_result Lwt.t
end
