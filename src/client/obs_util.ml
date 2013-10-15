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
open Printf

let format_size factor n =
  if n > 1048576L then
    sprintf "%.2f MB" (factor *. Int64.to_float n /. 1048576.)
  else if n > 1024L then
    sprintf "%.2f kB" (factor *. Int64.to_float n /. 1024.)
  else
    sprintf "%Ld B" (Int64.(of_float (factor *. to_float n)))

let format_speed ~since ~now delta =
  format_size (1. /. (now -. since)) delta ^ "/second"

module Progress_report =
struct

  type t =
      {
        och : Lwt_io.output_channel;
        max : Int64.t option;
        start_time : float;
        mutable curr : Int64.t;
        mutable delta_start_time : float;
        mutable delta_start : Int64.t;
        mutable curr_speed : string;
        mutable finished : bool;
        mutable eta_counter : int;
        mutable avg_speed : int;
      }

  let format_eta t =
    match t.max with
      | Some m when t.avg_speed > 0 ->
          sprintf "ETA: %ds" Int64.(to_int (div m (of_int t.avg_speed)))
      | _ -> ""

  let format_count t = format_size 1.0 t.curr

  let output t =
    let now = Unix.gettimeofday () in
    lwt () =
      Lwt_io.fprintf t.och
        " %6.2fs:  %10s  %16s  %s              \r"
        (Unix.gettimeofday () -. t.start_time)
        (format_count t)
        t.curr_speed
        (format_eta t)
        >>
      Lwt_io.flush t.och
    in
      if now -. t.delta_start_time > 1.0 then begin
        t.curr_speed <- format_speed t.delta_start_time now
                          Int64.(sub t.curr t.delta_start);
        t.avg_speed <- (4 * t.avg_speed +
                          Int64.(to_int (sub t.curr t.delta_start))) /
                       min 5 (1 + t.eta_counter);
        t.delta_start_time <- now;
        t.delta_start <- t.curr;
        t.eta_counter <- t.eta_counter + 1;
      end;
      return_unit

  let update t counter =
    t.curr <- counter;
    return_unit

  let make ?max och =
    let now = Unix.gettimeofday () in
    let t =
      {
        och; max;
        start_time = now; delta_start_time = now;
        curr = 0L; curr_speed = ""; delta_start = 0L; finished = false;
        eta_counter = 0; avg_speed = 0;
      }
    in
      ignore begin try_lwt
        let rec loop_output () =
          if t.finished then return_unit
          else begin
            Lwt_unix.sleep 0.05 >>
            output t >>
            loop_output ()
          end
        in loop_output ()
      with _ -> return_unit
      end;
      t

  let finished t =
    t.finished <- true;
    let now = Unix.gettimeofday () in
      Lwt_io.fprintf t.och
        " %6.2fs:  %10s  %16s            \n\n"
        (now -. t.start_time)
        (format_count t)
        (format_speed t.start_time now t.curr) >>
      Lwt_io.flush t.och

  let with_progress_report ?max och f =
    let t = make ?max och in
      try_lwt
        f t
      finally
        finished t
end

