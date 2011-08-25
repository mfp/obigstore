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

type t = {
  mutable stats : stats;
  init_time : float;
  mutable t0 : float;
  mutable curr_writes : int;
  mutable curr_reads : int;
  mutable curr_bytes_wr : int;
  mutable curr_bytes_rd : int;
  mutable curr_cols_wr : int;
  mutable curr_cols_rd : int;
  mutable curr_seeks : int;
  mutable curr_near_seeks : int;
}

and stats =
  {
    uptime : float;
    total_writes : Int64.t;
    total_reads : Int64.t;
    total_bytes_wr : Int64.t;
    total_bytes_rd : Int64.t;
    total_cols_wr : Int64.t;
    total_cols_rd : Int64.t;
    total_seeks : Int64.t;
    total_near_seeks : Int64.t;
    averages : (int * rates) list;
  }

and rates =
  {
    writes : float; reads : float;
    bytes_wr : float; bytes_rd : float;
    cols_wr : float; cols_rd : float;
    seeks : float; near_seeks : float;
  }

let update_avg t period ~now ~dt prev sample =
  let period = (min (float period) (now -. t.init_time)) in
  let avg prev sample =
    max 0. (((period -. dt) *. prev +. dt *. sample) /. period)
  in
    {
      writes = avg prev.writes sample.writes;
      reads = avg prev.reads sample.reads;
      bytes_wr = avg prev.bytes_wr sample.bytes_wr;
      bytes_rd = avg prev.bytes_rd sample.bytes_rd;
      cols_wr = avg prev.cols_wr sample.cols_wr;
      cols_rd = avg prev.cols_rd sample.cols_rd;
      seeks = avg prev.seeks sample.seeks;
      near_seeks = avg prev.near_seeks sample.near_seeks;
    }

let update t =
  let now = Unix.gettimeofday () in
  let dt = now -. t.t0 in
  let avg_1s =
    { writes = float t.curr_writes /. dt;
      reads = float t.curr_reads /. dt;
      bytes_wr = float t.curr_bytes_wr /. dt;
      bytes_rd = float t.curr_bytes_rd /. dt;
      cols_wr = float t.curr_cols_wr /. dt;
      cols_rd = float t.curr_cols_rd /. dt;
      seeks = float t.curr_seeks /. dt;
      near_seeks = float t.curr_seeks /. dt;
    } in
  let prev = t.stats in
  let stats =
    {
      uptime = now -. t.init_time;
      total_writes = Int64.(add prev.total_writes (of_int t.curr_writes));
      total_reads = Int64.(add prev.total_reads (of_int t.curr_reads));
      total_bytes_wr = Int64.(add prev.total_bytes_wr (of_int t.curr_bytes_wr));
      total_bytes_rd = Int64.(add prev.total_bytes_rd (of_int t.curr_bytes_rd));
      total_cols_wr = Int64.(add prev.total_cols_wr (of_int t.curr_cols_wr));
      total_cols_rd = Int64.(add prev.total_cols_rd (of_int t.curr_cols_rd));
      total_seeks = Int64.(add prev.total_seeks (of_int t.curr_seeks));
      total_near_seeks = Int64.(add prev.total_near_seeks (of_int t.curr_near_seeks));
      averages =
        List.map
          (fun (period, prev) ->
             (period, update_avg t period ~now ~dt prev avg_1s))
          prev.averages;
    }
  in
    t.stats <- stats;
    t.curr_writes <- 0;
    t.curr_reads <- 0;
    t.curr_bytes_wr <- 0;
    t.curr_bytes_rd <- 0;
    t.curr_cols_wr <- 0;
    t.curr_cols_rd <- 0;
    t.curr_seeks <- 0;
    t.curr_near_seeks <- 0;
    t.t0 <- now

let zero_rates =
  { writes = 0.; reads = 0.;
    bytes_wr = 0.; bytes_rd = 0.;
    cols_wr = 0.; cols_rd = 0.;
    seeks = 0.; near_seeks = 0.;
  }

let zero_stats periods =
  {
    uptime = 0.;
    total_writes = 0L; total_reads = 0L;
    total_bytes_wr = 0L; total_bytes_rd = 0L;
    total_cols_wr = 0L; total_cols_rd = 0L;
    total_seeks = 0L; total_near_seeks = 0L;
    averages = List.map (fun p -> (p, zero_rates))
                 (List.filter ((<) 0) periods) (* only period > 0 *)
  }

let make avg_periods =
  let now = Unix.gettimeofday () in
  let t =
    {
      stats = zero_stats avg_periods;
      init_time = now; t0 = now;
      curr_writes = 0; curr_reads = 0; curr_bytes_wr = 0; curr_bytes_rd = 0;
      curr_cols_wr = 0; curr_cols_rd = 0; curr_seeks = 0; curr_near_seeks = 0;
    } in
  let r = Weak_ref.make t in
    ignore begin
      try_lwt
        let rec update_stats () =
          Lwt_unix.sleep 1.0 >>
          match Weak_ref.get r with
              None -> return ()
            | Some t -> update t; update_stats ()
        in update_stats ()
      with _ -> return ()
    end;
    t

let stats t = t.stats

let record_reads t n = t.curr_reads <- t.curr_reads + n
let record_writes t n = t.curr_writes <- t.curr_writes + n
let record_bytes_wr t n = t.curr_bytes_wr <- t.curr_bytes_wr + n
let record_bytes_rd t n = t.curr_bytes_rd <- t.curr_bytes_rd + n
let record_cols_wr t n = t.curr_cols_wr <- t.curr_cols_wr + n
let record_cols_rd t n = t.curr_cols_rd <- t.curr_cols_rd + n
let record_seeks t n = t.curr_seeks <- t.curr_seeks + n
let record_near_seeks t n = t.curr_near_seeks <- t.curr_near_seeks + n

