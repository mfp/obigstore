(* Lightweight thread library for OCaml
 * http://www.ocsigen.org/lwt
 * Interface extracted from deprecated Lwt_util
 * Copyright (C) 2005-2008 Jérôme Vouillon
 * Laboratoire PPS - CNRS Université Paris Diderot
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, with linking exceptions;
 * either version 2.1 of the License, or (at your option) any later
 * version. See COPYING file for details.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA
 * 02111-1307, USA.
 *)

type region

val make_region : int -> region
      (** [make_region sz] create a region of size [sz]. *)
val resize_region : region -> int -> unit
      (** [resize_region reg sz] resize the region [reg] to size [sz]. *)
val run_in_region : region -> int -> (unit -> 'a Lwt.t) -> 'a Lwt.t
      (** [run_in_region reg size f] execute the thread produced by the
          function [f] in the region [reg]. The thread is not started
          before some room is available in the region. *)
