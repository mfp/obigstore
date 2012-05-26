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

open Bm_util
open Lwt
open Printf

module D = Obs_storage
module DM = Obs_data_model
module List = struct include List include BatList end
module String = struct include String include BatString end

module C = Obs_protocol_client.Make(Obs_protocol_bin.Version_0_0_0)

type mode = Normal | Random of int * int | Seq of int

let server = ref "127.0.0.1"
let port = ref 12050
let keyspace = ref "obs-benchmark"
let table = ref (DM.table_of_string "bm_write")
let concurrency = ref 2048
let multi = ref 1
let columns = ref 1
let rate = ref None
let period = ref None
let report_latencies = ref false

let mode = ref Normal

let show_usage_and_exit = ref (fun () -> exit 1)

let set_rand_mode s =
  try
    let n, m = String.split s ":" in
    let n = int_of_string n in
    let m = int_of_string m in
      mode := Random (n, m)
  with Not_found | Failure _ ->
      !show_usage_and_exit ()

let params = Arg.align
  [
   "-server", Arg.Set_string server, "ADDR Connect to server at ADDR.";
   "-port", Arg.Set_int port, "N Connect to server port N (default: 12050)";
   "-keyspace", Arg.Set_string keyspace,
     "NAME keyspace to use (default 'obs-benchmark')";
   "-table", Arg.String (fun s -> table := DM.table_of_string s),
     "NAME table to use (default 'bm_write')";
   "-columns", Arg.Set_int columns, "N Write N columns per key (default: 1).";
   "-concurrency", Arg.Set_int concurrency,
     "N maximum number of concurrent writes (default: 2048, multi: max 256)";
   "-multi", Arg.Set_int multi, "N Write in N (power of 2) batches (default: 1).";
   "-rand", Arg.String set_rand_mode,
     "N:M Write N keys in range 1..M with 32-byte 50% compressible values.";
   "-seq", Arg.Int (fun n -> mode := Seq n),
     "N Write N sequential keys with 32-byte 50% compressible values.";
   "-rate", Arg.Int (fun n -> rate := Some n), "N Limit writes to N/sec.";
   "-period", Arg.Float (fun p -> period := Some p),
     "FLOAT Report stats every FLOAT seconds.";
   "-latency", Arg.Set report_latencies, " Report latencies.";
  ]

let usage_message = "Usage: bm_write [options]"

let make_client ~address ~data_address =
  lwt fd, ich, och = Obs_conn.open_connection address in
    Lwt_unix.setsockopt fd Unix.TCP_NODELAY true;
    return (C.make ~data_address ich och)

let in_flight = ref 0
let finished = ref 0
let errors = ref 0

let prev_finished = ref 0

let exec_start_time = ref 0.
let t0 = ref 0.

let is_time_to_report = ref false

let report_delta = 64 * 1024

let must_report () =
  let r =
    !is_time_to_report ||
    BatOption.is_none !period &&
    (!finished > 16384 && !finished land (- !finished) = !finished ||
     !finished land (report_delta - 1) = 0)
  in
    is_time_to_report := false;
    r

let latencies = ref []

let measure_latency f =
  if not !report_latencies then f ()
  else begin
    let t0 = Unix.gettimeofday () in
    lwt () = f () in
    let dt = Unix.gettimeofday () -. t0 in
      latencies := dt :: !latencies;
      return ()
  end

let report_header =
  let n = ref 0 in
    (fun () ->
       if !n = 0 then begin
         printf "#%5s  %-9s  %6s  " "time" "keys" "k rate";
         if !columns > 1 then printf "%6s  " "c rate";
         if !report_latencies then begin
           printf "%8s  %8s  %8s  %8s  %8s"
             "mean" "median" "90th" "95th" "99th"
         end;
         printf "\n%!";
       end;
       incr n;
       if !n > 10 then n := 0)

let report ?(force=false) () =
  if force || must_report () then begin
    let t = Unix.gettimeofday () in
    let dt = t -. !t0 in
    let rate = float (!finished - !prev_finished) /. dt in
      report_header ();
      printf "%-6.2f  %-9d  %-6d  " (t -. !exec_start_time) !prev_finished (truncate rate);
      if !columns > 1 then printf "%-6d  " (truncate (rate *. float !columns));
      if !report_latencies then begin
        let sorted = Array.of_list !latencies in
        let () = Array.sort compare sorted in
        let len = Array.length sorted in
        let mean = Array.fold_left (fun dt s -> s +. dt) 0. sorted /. float len in
        let median = sorted.(len / 2) in
        let perc90 = sorted.(90 * len / 100) in
        let perc95 = sorted.(95 * len / 100) in
        let perc99 = sorted.(99 * len / 100) in
          printf "%.6f  %.6f  %.6f  %.6f  %.6f" mean median perc90 perc95 perc99;
          latencies := [];
      end;
      printf "\n%!";
      prev_finished := !finished;
      t0 := t
  end

let rate_limiter = ref (fun () -> return false)

let rec write_aux read_data write_data data_len ks =
  try_lwt
    while_lwt !in_flight < !concurrency do
      lwt data = read_data () in
        ignore begin
          incr in_flight;
          !rate_limiter () >>
          begin try_lwt
            lwt () = measure_latency (fun () -> write_data ks !table data) in
              finished := !finished + data_len data;
              report ();
              return ()
          with _ ->
            incr errors;
            return ()
          finally
            decr in_flight;
            return ()
          end >>
          write_aux read_data write_data data_len ks
        end;
        return ()
    done
  with End_of_file -> return ()

let read_string ic =
  lwt count = Lwt_io.LE.read_int ic in
  let s = String.create count in
    Lwt_io.read_into_exactly ic s 0 count >>
    return s

let mk_col ?(timestamp = DM.No_timestamp) ~name data =
  { DM.name; data; timestamp; }

let read_pair ch =
  lwt k = read_string ch in
  lwt v = read_string ch in
    return (k, v)

let read_key_value () = Lwt_io.atomic read_pair Lwt_io.stdin

let read_multi_values () =
  Lwt_io.atomic
    (fun ch ->
       let rec read_n_values ch acc = function
           0 -> return acc
         | n ->
             lwt k, v = read_pair ch in
               read_n_values ch ((k, [mk_col ~name:"v" v]) :: acc) (n - 1)
       in read_n_values ch [] !multi)
    Lwt_io.stdin

let rec read_columns ch ?(acc=[]) = function
    0 -> return acc
  | n ->
      lwt name, v = read_pair ch in
        read_columns ch ~acc:(mk_col ~name v :: acc) (n - 1)

let read_complex_value ch =
  lwt k = read_string ch in
  lwt cols = read_columns ch !columns in
    return (k, cols)

let read_complex_values ch =
  let rec read_complex_multi ch acc = function
      0 -> return acc
    | n ->
        lwt x = read_complex_value ch in
          read_complex_multi ch (x :: acc) (n - 1)
  in read_complex_multi ch [] !columns

let write_values =
  write_aux
    (fun () ->
       lwt k, v = read_key_value () in
         return (k, [mk_col ~name:"v" v]))
    (fun ks table (k, cols) -> C.put_columns ks table k cols)
    (fun (k, cols) -> 1)

let write_values_multi =
  write_aux read_multi_values C.put_multi_columns List.length

let write_complex_values =
  write_aux
    (fun () -> Lwt_io.atomic read_complex_value Lwt_io.stdin)
    (fun ks table (k, cols) -> C.put_columns ks table k cols)
    (fun (k, cols) -> 1)

let write_complex_values_multi =
  write_aux
    (fun () -> Lwt_io.atomic read_complex_values Lwt_io.stdin)
    C.put_multi_columns
    List.length

module Throttle = Lwt_throttle.Make(struct
                                      type t = int
                                      let hash n = n
                                      let equal = (==)
                                    end)

let zero_pad n s =
  String.make (n - String.length s) '0' ^ s

let perform_writes ks =
  match !mode with
      Normal ->
        if !columns > 1 && !multi > 1 then write_complex_values_multi ks
        else if !columns > 1 then write_complex_values ks
        else if !multi > 1 then write_values_multi ks
        else write_values ks
    | mode ->
        let prng = Cheapo_prng.make ~seed:(Random.int 0xFFFFFF) in
        let `Staged random_val = random_string_maker 0.5 prng in
        let i = ref 0 in
        let mk_datum = match mode with
            Normal -> assert false
          | Seq n ->
              (fun _ ->
                 incr i;
                 if !i > n then raise End_of_file;
                 let k = zero_pad 16 (string_of_int !i) in
                 let v = random_val 32 in
                   (k, [mk_col ~name:"v" v]))
          | Random (n, m) ->
              (fun _ ->
                 incr i;
                 if !i > n then raise End_of_file;
                 let k = zero_pad 16 (string_of_int (Random.int m)) in
                   let v = random_val 32 in
                   (k, [mk_col ~name:"v" v]))
        in
          if !multi > 1 then
            write_aux
              (fun () -> return (List.init !multi mk_datum))
              C.put_multi_columns List.length ks
          else
            write_aux
              (fun () -> return (mk_datum 0))
              (fun ks table (k, cols) -> C.put_columns ks table k cols)
              (fun (k, cols) -> 1)
              ks

let () =
  show_usage_and_exit := (fun () -> Arg.usage params usage_message; exit 1);
  Arg.parse params ignore usage_message;
  Lwt_unix.run begin
    let address = Unix.ADDR_INET (Unix.inet_addr_of_string !server, !port) in
    let data_address = Unix.ADDR_INET (Unix.inet_addr_of_string !server, !port + 1) in
    lwt db = make_client ~address ~data_address in
    lwt ks = C.register_keyspace db !keyspace in

    let rec wait_until_finished () =
      lwt () = Lwt_unix.sleep 0.01 in
        if !in_flight > 0 then wait_until_finished ()
        else return ()
    in
      t0 := Unix.gettimeofday ();
      exec_start_time := !t0;
      begin
        match !period with
            None -> ()
          | Some p ->
              let rec trigger_report () =
                lwt () = Lwt_unix.sleep p in
                  is_time_to_report := true;
                  trigger_report ()
              in ignore (trigger_report ())
      end;
      begin match !rate with
          None -> ()
        | Some rate ->
            let rate = rate / !multi in
            let limiter = Throttle.create ~rate ~max:1000000 ~n:13 in
            let wait () = Throttle.wait limiter 0 in
              rate_limiter := wait;
      end;
      if !multi > 1 then concurrency := min !concurrency 256;
      perform_writes ks >>
      lwt () = wait_until_finished () in
        report ~force:true ();
        return ()
  end
