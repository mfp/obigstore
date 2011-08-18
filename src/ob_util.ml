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
open Printf

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
      }

  let format_size factor n =
    if n > 1048576L then
      sprintf "%.2fs MB" (factor *. Int64.to_float n /. 1048576.)
    else if n > 1024L then
      sprintf "%.2fs kB" (factor *. Int64.to_float n /. 1024.)
    else
      sprintf "%Ld B" (Int64.(of_float (factor *. to_float n)))

  let format_count t = format_size 1.0 t.curr

  let format_speed since now delta =
    format_size (1. /. (now -. since)) delta ^ "/second"

  let output t =
    let now = Unix.gettimeofday () in
    lwt () =
      Lwt_io.fprintf t.och
        " %6.2fs:  %10s  %16s                \r"
        (Unix.gettimeofday () -. t.start_time)
        (format_count t)
        t.curr_speed >>
      Lwt_io.flush t.och
    in
      if now -. t.delta_start_time > 1.0 then begin
        t.curr_speed <- format_speed t.delta_start_time now
                          Int64.(sub t.curr t.delta_start);
        t.delta_start_time <- now;
        t.delta_start <- t.curr;
      end;
      return ()

  let update t counter =
    t.curr <- counter;
    return ()

  let make ?max och =
    let now = Unix.gettimeofday () in
    let t =
      {
        och; max;
        start_time = now; delta_start_time = now;
        curr = 0L; curr_speed = ""; delta_start = 0L; finished = false
      }
    in
      ignore begin try_lwt
        let rec loop_output () =
          if t.finished then return ()
          else begin
            Lwt_unix.sleep 0.05 >>
            output t >>
            loop_output ()
          end
        in loop_output ()
      with _ -> return ()
      end;
      t

  let finished t =
    t.finished <- true;
    let now = Unix.gettimeofday () in
      Lwt_io.fprintf t.och
        " %6.2fs:  %10s  %16s                \n\n"
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

