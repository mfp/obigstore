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

module D = Obs_storage
module DM = Obs_data_model
module List = struct include List include BatList end
module String = struct include String include BatString end

module C = Obs_protocol_client.Make(Obs_protocol_payload.Version_0_0_0)

let server = ref "127.0.0.1"
let port = ref 12050
let keyspace = ref "obs-benchmark"
let table = ref "bm_write"
let concurrency = ref 2048
let multi = ref 1
let columns = ref 1

let params = Arg.align
  [
   "-server", Arg.Set_string server, "ADDR Connect to server at ADDR.";
   "-port", Arg.Set_int port, "N Connect to server port N (default: 12050)";
   "-keyspace", Arg.Set_string keyspace,
     "NAME keyspace to use (default 'obs-benchmark')";
   "-table", Arg.Set_string table, "NAME table to use (default 'bm_write')";
   "-columns", Arg.Set_int columns, "N Write N columns per key (default: 1).";
   "-concurrency", Arg.Set_int concurrency,
     "N maximum number of concurrent writes (default: 2048, multi: max 256)";
   "-multi", Arg.Set_int multi, "N Write in N (power of 2) batches (default: 1).";
  ]

let usage_message = "Usage: bm_write [options]"

let make_client ~address ~data_address =
  lwt fd, ich, och = Obs_conn.open_connection address in
    Lwt_unix.setsockopt fd Unix.TCP_NODELAY true;
    return (C.make ~data_address ich och)

let read_string ic =
  lwt count = Lwt_io.LE.read_int ic in
  let s = String.create count in
    Lwt_io.read_into_exactly ic s 0 count >>
    return s

let mk_col ?(timestamp = DM.No_timestamp) ~name data =
  { DM.name; data; timestamp; }

let in_flight = ref 0
let finished = ref 0
let errors = ref 0

let read_pair ch =
  lwt k = read_string ch in
  lwt v = read_string ch in
    return (k, v)

let read_key_value () = Lwt_io.atomic read_pair Lwt_io.stdin

let prev_finished = ref 0
let t0 = ref 0.

let report_delta = ref 16383

let one_gb = 1024 * 1024 * 1024

let report () =
  if !finished > 16384 && !finished land (- !finished) = !finished ||
     !finished land (one_gb - 1) = 0
  then begin
    let t = Unix.gettimeofday () in
    let dt = t -. !t0 in
    let rate = float (!finished - !prev_finished) /. dt in
      printf "%-9d  %-6d  " !prev_finished (truncate rate);
      if !columns > 1 then printf "%d" (truncate (rate *. float !columns));
      printf "\n%!";
      prev_finished := !finished;
      t0 := t
  end

let rec write_values ks =
  try_lwt
    while_lwt !in_flight < !concurrency do
      lwt k, v = read_key_value () in
        ignore begin
          incr in_flight;
          try_lwt
            lwt () = C.put_columns ks !table k [mk_col ~name:"v" v] in
              incr finished;
              report ();
              write_values ks
          with _ ->
            incr errors;
            return ()
          finally
            decr in_flight;
            return ()
        end;
        return ()
    done
  with End_of_file -> return ()

let wrap_some x = Some x

let try_read_pair ch =
  try_lwt read_pair ch >|= wrap_some with End_of_file -> return None

let read_multi_values () =
  Lwt_io.atomic
    (fun ch ->
       let rec read_n_values ch acc = function
           0 -> return (!multi, acc)
         | n ->
             match_lwt try_read_pair ch with
                 None -> return ((!multi - n), acc)
               | Some (k, v) ->
                   read_n_values ch ((k, [mk_col ~name:"v" v]) :: acc) (n - 1)
       in read_n_values ch [] !multi)
    Lwt_io.stdin

let rec write_values_multi ks =
  try_lwt
    while_lwt !in_flight < !concurrency do
      match_lwt read_multi_values () with
          _, [] -> raise Exit
        | num_pairs, data ->
            ignore begin
              incr in_flight;
              try_lwt
                lwt () = C.put_multi_columns ks !table data in
                  finished := !finished + num_pairs;
                  report ();
                  write_values_multi ks
              with exn ->
                incr errors;
                return ()
              finally
                decr in_flight;
                return ()
            end;
            return ()
    done
  with Exit -> return ()

let rec read_columns ch ?(acc=[]) = function
    0 -> return acc
  | n -> match_lwt try_read_pair ch with
        None -> return acc
      | Some (name, v) -> read_columns ch ~acc:(mk_col ~name v :: acc) (n - 1)

let read_complex_value ch =
  lwt k = read_string ch in
  lwt cols = read_columns ch !columns in
    return (k, cols)

let read_complex_values ch =
  let rec read_complex_multi ch acc = function
      0 -> return acc
    | n ->
        match_lwt
          try_lwt read_complex_value ch >|= wrap_some with _ -> return None
        with
            None -> return acc
          | Some x -> read_complex_multi ch (x :: acc) (n - 1)
  in read_complex_multi ch [] !columns

let rec write_complex_values ks =
  try_lwt
    while_lwt !in_flight < !concurrency do
      lwt k, cols = Lwt_io.atomic read_complex_value Lwt_io.stdin in
        ignore begin
          incr in_flight;
          try_lwt
            lwt () = C.put_columns ks !table k cols in
              incr finished;
              report ();
              write_complex_values ks
          with _ ->
            incr errors;
            return ()
          finally
            decr in_flight;
            return ()
        end;
        return ()
    done
  with End_of_file -> return ()

let rec write_complex_values_multi ks =
  try_lwt
    while_lwt !in_flight < !concurrency do
      match_lwt Lwt_io.atomic read_complex_values Lwt_io.stdin with
          [] -> raise_lwt End_of_file
        | l ->
            ignore begin
              incr in_flight;
              try_lwt
                lwt () = C.put_multi_columns ks !table l in
                  finished := !finished + List.length l;
                  report ();
                  write_complex_values_multi ks
              with _ ->
                incr errors;
                return ()
              finally
                decr in_flight;
                return ()
            end;
            return ()
    done
  with End_of_file -> return ()

let () =
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
      begin if !columns > 1 && !multi > 1 then begin
        concurrency := min !concurrency 256;
        write_complex_values_multi ks
      end else if !columns > 1 then
        write_complex_values ks
      else if !multi > 1 then begin
        report_delta := 65536 * 2 - 1;
        concurrency := min !concurrency 256;
        write_values_multi ks
      end else
        write_values ks
      end >>
      wait_until_finished ()
  end
