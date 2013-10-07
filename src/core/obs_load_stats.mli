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

(** Obigstore load statistics. *)

type t

type stats = {
  uptime : float;
  total_writes : Int64.t;
  total_reads : Int64.t;
  total_bytes_wr : Int64.t;
  total_bytes_rd : Int64.t;
  total_cols_wr : Int64.t;
  total_cols_rd : Int64.t;
  total_seeks : Int64.t;
  total_near_seeks : Int64.t;
  total_transactions : Int64.t;
  averages : (int * rates) list;
}

and rates = {
  writes : float;
  reads : float;
  bytes_wr : float;
  bytes_rd : float;
  cols_wr : float;
  cols_rd : float;
  seeks : float;
  near_seeks : float;
  transactions : float;
}

(* val update_avg : t -> int -> now:float -> dt:float -> rates -> rates -> rates *)
(* val update : t -> unit *)
(* val zero_rates : rates *)
(* val zero_stats : int list -> stats *)

(** [make period_list] initializes and begins to update periodically a new
  * value holding the load stats including the average rates calculated for
  * the periods given in [period_list] (in seconds). *)
val make : int list -> t

(** Return the up-to-date load statistics. *)
val stats : t -> stats

(** {2 Event recording} *)

val record_reads : t -> int -> unit
val record_writes : t -> int -> unit
val record_bytes_wr : t -> int -> unit
val record_bytes_rd : t -> int -> unit
val record_cols_wr : t -> int -> unit
val record_cols_rd : t -> int -> unit
val record_seeks : t -> int -> unit
val record_near_seeks : t -> int -> unit
val record_transaction : t -> unit
