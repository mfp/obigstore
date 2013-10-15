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

open Lwt

type kind = [`Shared | `Exclusive]

type state = [`Free | kind]

type t =
    {
      mutable state : state;
      mutable procs : int;
      mutable waiters : (unit Lwt.t * unit Lwt.u * kind) Lwt_sequence.t;
    }

(* invariants:
 * (1) if state = `Free, procs = 0
 * (2) if state = `Exclusive, procs = 1
 * (3) if state = `Shared, the next waiter MUST be `Exclusive
 * *)

let create () = { state = `Free; procs = 0; waiters = Lwt_sequence.create () }

let rec lock kind m =
  match m.state, kind with
      `Free, _ ->
        m.procs <- m.procs + 1;
        m.state <- (kind :> state);
        Lwt.return_unit
    | `Shared, `Shared when Lwt_sequence.is_empty m.waiters ->
        (* we only allow it to enter if there are no other waiters (the first
         * of them being necessarily exclusive as per (3)) in order to ensure
         * fairness *)
        m.procs <- m.procs + 1;
        Lwt.return_unit
    | `Shared, `Shared | `Shared, `Exclusive | `Exclusive, _ ->
        let (res, w) = Lwt.task () in
        let node = Lwt_sequence.add_r (res, w, kind) m.waiters in
          Lwt.on_cancel res (fun _ -> Lwt_sequence.remove node);
          res

let unlock m =
  match m.state with
      `Free -> ()
    | `Shared ->
        m.procs <- m.procs - 1;
        if Lwt_sequence.is_empty m.waiters then begin
          if m.procs = 0 then m.state <- `Free
        end else if m.procs = 0 then begin
          let _, u, kind = Lwt_sequence.take_l m.waiters in
            m.state <- (kind :> state);
            m.procs <- 1;
            Lwt.wakeup_later u ()
        end
        (* as per invariant (3), the next waiter must be exclusive,
         * so we cannot awaken it if m.procs > 0 *)
    | `Exclusive ->
        (* as per invariant (2), m.procs is 1 *)
        assert (m.procs = 1);
        m.procs <- 0;
        if Lwt_sequence.is_empty m.waiters then
          m.state <- `Free
        else begin
          (* we try to awaken either 1 exclusive waiter or as many shared
           * waiters as possible  *)
          let rec do_wakeup ~excl_ok =
            match Lwt_sequence.take_opt_l m.waiters with
                None -> ()
              | Some (_, u, `Shared) ->
                  m.state <- `Shared;
                  m.procs <- m.procs + 1;
                  Lwt.wakeup_later u ();
                  do_wakeup ~excl_ok:false
              | Some (_, u, `Exclusive) when excl_ok ->
                  m.state <- `Exclusive;
                  m.procs <- 1;
                  Lwt.wakeup_later u ()
              | Some ((t, u, `Exclusive) as x) (* when not excl_ok *) ->
                  (* must requeue *)
                  let node = Lwt_sequence.add_l x m.waiters in
                    Lwt.on_cancel t (fun _ -> Lwt_sequence.remove node)
          in do_wakeup ~excl_ok:true
        end

let lock ?(shared = true) m = lock (if shared then `Shared else `Exclusive) m

let with_lock ?shared m f =
  lwt () = lock ?shared m in
  try_lwt
    f ()
  finally
    unlock m;
    return_unit

let is_locked m = m.state <> `Free
let is_locked_shared m = m.state = `Shared
let is_locked_exclusive m = m.state = `Exclusive

let is_empty m = Lwt_sequence.is_empty m.waiters
