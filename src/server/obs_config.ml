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

open Lwt

module List = BatList

let read_auth f = try_lwt
  Lwt_io.with_file ~mode:Lwt_io.input f Lwt_io.read >|=
  Obs_config_j.roles_of_string >|=
  List.filter_map
    (fun { Obs_config_t.role; password; perms } ->
       match perms with
         | [`Replication] -> Some (role, password, `Replication)
         | l when List.mem `Full_access l -> Some (role, password, `Full_access)
         | _ -> None) >|=
  Obs_auth.make
