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

(** Misc. utility functions *)

(** [format_size x n] represents [x *. Int64.to_float n] as an amount in bytes
  * in a human-readable way. *)
val format_size : float -> Int64.t -> string

(** [format_speed ~since ~now n] represents the transfer speed
  * [n * (1. /. (now -. since))] (bytes/s) in a human-readable way. *)
val format_speed : since:float -> now:float -> Int64.t -> string

(** Readable progress report.
  * Sample usage:
  *  [Progress_report.with_progress_report Lwt_io.stderr f]
  * where [f] calls [Progress_report.update] with the current value of the
  * progress counter (e.g. file position) repeatedly.
  * *)
module Progress_report :
sig
  type t

  val update : t -> Int64.t -> unit Lwt.t

  val with_progress_report :
    ?max:Int64.t -> Lwt_io.output_channel -> (t -> 'a Lwt.t) -> 'a Lwt.t
end
