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
open Obigstore_core
open Obigstore_client
open Obigstore_server

module D = Storage
module DM = Data_model
module List = struct include BatList include List end

module Make
  (D : Data_model.S)
  (C : sig
     val is_remote : bool
     val make_tmp_db : unit -> D.db Lwt.t
   end) =
struct
  let mk_col ?(timestamp = DM.No_timestamp) ~name data =
    { DM.name; data; timestamp; }

  let time f =
    let t0 = Unix.gettimeofday () in
      f () >>
      return (Unix.gettimeofday () -. t0)

  let bm_put_columns ?(iters=10000) ?(batch_size=1000) make_row ks ~table =
    lwt insert_time =
      time begin fun () ->
        for_lwt i = 1 to iters / batch_size do
          let rows = List.init batch_size (fun _ -> make_row ()) in
            D.read_committed_transaction ks
              (fun tx ->
                 Lwt_list.iter_p
                   (fun (key, columns) -> D.put_columns tx table key columns)
                   rows)
        done
      end in
    lwt overhead =
      time begin fun () ->
        for_lwt i = 1 to iters / batch_size do
          let rows = List.init batch_size (fun _ -> make_row ()) in
            Lwt_list.iter_s (fun _ -> return ()) rows
        done
      end
    in return (insert_time -. overhead)

  let make_row_dummy ?(avg_cols=10) () =
    let c = if avg_cols > 1 then 1 + Random.int (avg_cols*2) else 1 in
      (sprintf "%d%d" (Random.int 0x3FFFFFF) (Random.int 0x3FFFFFF),
       List.init c
         (fun i ->
            mk_col ~name:(sprintf "col%d" i) (string_of_int (Random.int 0x3FFFFFF))))

  let row_data_size (key, cols) =
    String.length key +
    List.fold_left (fun s c -> s + String.length c.DM.data) 0 cols

  let row_data_size_with_colnames (key, cols) =
    row_data_size (key, cols) +
    List.fold_left (fun s c -> s + String.length c.DM.name) 0 cols

  let pr_separator () =
    print_endline (String.make 80 '-')

  let run_put_colums_bm ~rounds ~iterations ~batch_size ~avg_cols db =
    lwt ks = D.register_keyspace db "ks1" in
      printf "column insertion time (%d row batches, %d columns avg)\n"
        batch_size avg_cols;
      pr_separator ();
      for_lwt i = 0 to rounds - 1 do
        lwt dt =
          bm_put_columns ~iters:iterations ~batch_size
                          (make_row_dummy ~avg_cols) ks ~table:"dummy" in
        lwt size_on_disk = D.table_size_on_disk ks "dummy" in
          printf "%7d -> %7d    %8.5fs  (%.0f/s)   ~%Ld bytes\n%!"
             (i * iterations) ((i + 1) * iterations) dt (float iterations /. dt)
             size_on_disk;
          return ()
      done >>
      let compute_avg_size f =
        float
          (List.fold_left
             (fun s r -> s + f r)
             0 (List.init 1000 (fun _ -> make_row_dummy ~avg_cols ()))) /.
        1000.  in
      let avg_row_size = compute_avg_size row_data_size in
      let avg_row_size' = compute_avg_size row_data_size_with_colnames in
      lwt size_on_disk = D.table_size_on_disk ks "dummy" in
      let total_data_size = avg_row_size *. float rounds *. float iterations in
      let total_data_size' = avg_row_size' *. float rounds *. float iterations in
        printf "\nTable size: %9Ld bytes\n\
                  Data:       %9Ld bytes, ratio %4.2f  excluding colum names\n\
                 \            %9Ld bytes  ratio %4.2f  with column names\n"
          size_on_disk
          (Int64.of_float total_data_size)
          (Int64.to_float size_on_disk /. total_data_size)
          (Int64.of_float total_data_size')
          (Int64.to_float size_on_disk /. total_data_size');
        return ()

  let bm_sequential_read ?(read_committed = false) ~max_keys ?(max_columns = 10) db =
    lwt ks = D.register_keyspace db "ks1" in
    let n_keys = ref 0 in
    let n_columns = ref 0 in

    let transaction =
      if read_committed then D.read_committed_transaction
      else D.repeatable_read_transaction in

    let rec read_from tx first =
      lwt (last_key, l) =
        D.get_slice tx "dummy" ~max_keys ~max_columns
          (DM.Key_range { DM.first; up_to = None; reverse = false; })
          DM.All_columns in
      let len = List.length l in
        n_keys := !n_keys + len;
        n_columns := !n_columns +
                     List.fold_left (fun s kd -> s + List.length kd.DM.columns) 0 l;
        match last_key with
          | Some k when len = max_keys -> read_from tx (Some (k ^ "\000"))
          | _ -> return () in
    lwt dt = time (fun () -> transaction ks (fun tx -> read_from tx None)) in
      printf "Seq read %s (slice %4d): %9d keys %9d columns in %8.5fs (%d/s) (%d/s)\n%!"
        (if read_committed then "RC" else "RR")
        max_keys
        !n_keys !n_columns dt
        (truncate (float !n_keys /. dt))
        (truncate (float !n_columns /. dt));
      return ()

  let iter_p_in_region ~size f l =
    let pending = ref 0 in
    let q = Queue.create () in
    let rec iter_in_region f t = function
        [] -> t
      | x :: tl ->
          begin if !pending > size then begin
            let w = Lwt.task () in
              Queue.push (snd w) q;
              fst w
          end else
            return ()
          end >>
          let t' =
            begin
              incr pending;
              lwt () = f x in
                decr pending;
                try
                  Lwt.wakeup (Queue.pop q) ();
                  return ()
                with Queue.Empty -> return ()
            end
          in iter_in_region f (t >> t') tl
    in iter_in_region f (return ()) l

  let mk_payload payload k =
    if payload <= 32 then Digest.to_hex (Digest.string k)
    else
      let b = Buffer.create 13 in
        while Buffer.length b < payload do
          Buffer.add_string b (string_of_int (Random.int 0x3FFFFFF))
        done;
        Buffer.sub b 0 payload

  let bm_simple_write_aux mk_key ~iterations ~payload db table =
    lwt ks = D.register_keyspace db "simple" in
    let chunksize = iterations / (max 2 (1000 / payload)) in
    let chunks = iterations / chunksize in
    let nkeys = chunksize * chunks in
    let keys =
      List.init chunks
        (fun i ->
           List.init chunksize
             (fun j ->
                let k = mk_key (i * chunksize + j) in
                  (k, mk_payload payload k))) in
    lwt dt =
      time
        (fun () ->
           iter_p_in_region ~size:50
             (iter_p_in_region ~size:100
                (fun (k, v) -> D.put_columns ks table k [mk_col "value" v]))
             keys) in
    lwt dt2 =
      time
        (fun () ->
           iter_p_in_region ~size:50
             (fun l ->
                D.put_multi_columns ks (table ^ "_multi")
                  (List.map (fun (k, v) -> (k, [mk_col "value" v])) l))
             keys)
    in return (dt, dt2, nkeys)

  let bm_sequential_write ~iterations ~payload db =
    lwt (dt, dt_multi, nkeys) =
      bm_simple_write_aux string_of_int ~iterations ~payload db "sequential_write"
    in
      print_newline ();
      printf "Seq write: %d keys in %8.5fs (%d/s)\n%!"
        nkeys dt (truncate (float nkeys /. dt));
      printf "Seq write (multi): %d keys in %8.5fs (%d/s)\n%!"
        nkeys dt_multi (truncate (float nkeys /. dt_multi));
      return ()

  let bm_random_write ~iterations ~payload db =
    lwt (dt, dt_multi, nkeys) =
      bm_simple_write_aux (fun _ -> string_of_int (Random.int iterations))
        ~iterations ~payload db "random_write"
    in
      print_newline ();
      printf "Rand write: %d keys in %8.5fs (%d/s)\n%!"
        nkeys dt (truncate (float nkeys /. dt));
      printf "Rand write (multi): %d keys in %8.5fs (%d/s)\n%!"
        nkeys dt_multi (truncate (float nkeys /. dt_multi));
      return ()

  let bm_count db =
    lwt ks = D.register_keyspace db "simple" in
    let table = "sequential_write" in
    let nkeys = ref 0L in
    lwt dt =
      time (fun () ->
              lwt n = D.count_keys ks table
                        (DM.Key_range { DM.first= None; up_to = None; reverse = false; })
              in
                nkeys := n;
                return ())
    in
      print_newline ();
      printf "Key count: %Ld keys in %8.5fs (%d/s)\n%!"
        !nkeys dt (truncate (Int64.to_float !nkeys /. dt));
      return ()

  let bm_random_read ~max_key db =
    lwt ks = D.register_keyspace db "simple" in
    let table = "sequential_write" in
    let nkeys = max_key in
    let keys =
      List.init (nkeys / 100)
        (fun _ ->
           List.init 100
             (fun _ -> string_of_int (Random.int nkeys))) in

    let do_read k =
      lwt _ = D.get_column ks table k "value" in
        return () in
    let read keys =
      if C.is_remote then
        iter_p_in_region ~size:50
          (iter_p_in_region ~size:50 do_read) keys
      else
        Lwt_list.iter_s (Lwt_list.iter_s do_read) keys in
    let dt transaction = time (fun () -> transaction ks (fun _ -> read keys)) in

    lwt dt_repeatable = dt D.repeatable_read_transaction in
    lwt dt_read_committed = dt D.read_committed_transaction in
    lwt dt_no_tx = dt (fun ks f -> f ks) in
    lwt dt_slices =
      let do_read keys =
        lwt _ = D.get_slice ks table (DM.Keys keys)
                  (DM.Column_range_union [DM.Columns ["value"]])
        in
          return ()
      in
        time
          (fun () ->
             if C.is_remote then iter_p_in_region ~size:50 do_read keys
             else Lwt_list.iter_s do_read keys)
    in
      print_newline ();
      printf "Rand read (repeatable): %d columns in %8.5fs (%d/s)\n"
         nkeys dt_repeatable (truncate (float nkeys /. dt_repeatable));
      printf "Rand read (read_committed): %d columns in %8.5fs (%d/s)\n"
         nkeys dt_read_committed (truncate (float nkeys /. dt_read_committed));
      printf "Rand read (no TX): %d columns in %8.5fs (%d/s)\n"
         nkeys dt_no_tx (truncate (float nkeys /. dt_no_tx));
      printf "Rand read (no TX) using 100-key slices: %d columns in %8.5fs (%d/s)\n%!"
         nkeys dt_slices (truncate (float nkeys /. dt_slices));
      return ()

  let run_if x f = if x then f () else return ()

  let run
        ~rounds ~iterations ~avg_cols ~batch_size
        ~run_put ~run_seq_write
        ~run_seq_read ~run_read_committed ~run_rand_read
        ~seq_iterations ~seq_payload =
    lwt db = C.make_tmp_db () in
      Test_00util.keep_tmp := false;
      run_if run_put
        (fun () -> run_put_colums_bm ~rounds ~iterations ~batch_size ~avg_cols db) >>
      run_if run_seq_write
        (fun () -> bm_random_write
                     ~iterations:seq_iterations
                     ~payload:seq_payload db) >>
      run_if run_seq_write
        (fun () -> bm_sequential_write
                     ~iterations:seq_iterations
                     ~payload:seq_payload db) >>
      run_if run_rand_read (fun () -> bm_count db) >>
      run_if run_rand_read (fun () -> bm_random_read ~max_key:seq_iterations db) >>
      run_if run_seq_read
        (fun () ->
           Lwt_list.iter_s
             (fun max_columns ->
                printf "\nmax_columns: %d\n" max_columns;
                pr_separator ();
                Lwt_list.iter_s
                  (fun read_committed ->
                     Lwt_list.iter_s
                       (fun max_keys ->
                          bm_sequential_read ~max_columns ~read_committed ~max_keys db)
                       [ 1000; 500; 200; 100; 50; 20; 10; 1 ])
                  (if run_read_committed then [ true; false ] else [false]))
             [ 1; 5; 10; 100 ]) >>
      return ()

end

let server = ref "127.0.0.1"
let port = ref 12050
let remote_test = ref false
let run_put = ref true
let run_seq_read = ref true
let run_seq_write = ref true
let run_random_read = ref true
let db_dir = ref None
let rounds = ref 10
let iterations = ref 10000
let batch_size = ref 1000
let avg_cols = ref 10

let payload = ref 32
let write_iters = ref 100000

let post_separator s x = s ^ "\n\n " ^ x ^ "\n"

let params =
  Arg.align
    [
      "-dir", Arg.String (fun s -> db_dir := Some s; remote_test := false),
        "PATH Run locally against LocalDB at PATH.";
      "-remote", Arg.Set remote_test, " Benchmark against server.";
      "-server", Arg.Set_string server, "ADDR Connect to server at ADDR.";
      "-port", Arg.Set_int port,
        post_separator
          "N Connect to server port N (default: 12050)"

          "Mode:";

      "-put-only",
        Arg.Unit (fun () -> run_put := true;
                            run_seq_read := false;
                            run_random_read := false;
                            run_seq_write := true;),
        " Benchmark only put_columns (complex + simple, rand + seq).";
      "-read-only",
        Arg.Unit (fun () -> run_put := false;
                            run_seq_read := true;
                            run_random_read := true;
                            run_seq_write := false;),
        post_separator " Benchmark only sequential and random read."

          "Multi-column row insertion:";

      "-complex-write-rounds", Arg.Set_int rounds,
        "N Run N put_columns rounds (default: 10).";
      "-complex-write-iters", Arg.Set_int iterations,
        "N Insert N keys per round. (default: 10000)";
      "-complex-write-columns", Arg.Set_int avg_cols,
        "N Insert N columns in average per key. (default: 10)";
      "-complex-write-batch-size", Arg.Set_int batch_size,
        post_separator
          "N Insert N keys per batch insert. (default: 1000)"

          "Single row insertion:";

      "-simple-write-payload", Arg.Set_int payload, "N Payload size (default: 32).";
      "-simple-write-iters", Arg.Set_int write_iters,
          "N Number of values to insert (default: 100000)\n"
    ]

module CLIENT = Protocol_client.Make(Protocol_payload.Version_0_0_0)

let usage_message = "Usage: benchmark [options]\n"

module BM_Storage =
  Make(Storage)
    (struct
       let make_tmp_db () =
         let tmp_dir = match !db_dir with
             None -> Test_00util.make_temp_dir ()
           | Some d -> d in
         let db = D.open_db ~group_commit_period:0.010 tmp_dir in
           return db

       let is_remote = false
     end)

let () =
  Printexc.record_backtrace true;
  Arg.parse params ignore usage_message;
  if not !remote_test then
    Lwt_unix.run
      (BM_Storage.run
         ~rounds:!rounds ~iterations:!iterations ~avg_cols:!avg_cols
         ~batch_size:!batch_size
         ~run_put:!run_put
         ~run_seq_write:!run_seq_write
         ~run_seq_read:!run_seq_read
         ~run_rand_read:!run_random_read
         ~run_read_committed:true
         ~seq_payload:!payload
         ~seq_iterations:!write_iters)
  else begin
    let addr = Unix.ADDR_INET (Unix.inet_addr_of_string !server, !port) in
    let module C =
      struct
        let make_tmp_db () =
          lwt ich, och = Lwt_io.open_connection addr in
            return (CLIENT.make ich och)

        let is_remote = true
      end in
    let module BM = Make(CLIENT)(C) in
      Lwt_unix.run
        (BM.run
           ~rounds:!rounds ~iterations:!iterations ~avg_cols:!avg_cols
           ~batch_size:!batch_size
           ~run_put:!run_put
           ~run_seq_read:!run_seq_read
           ~run_seq_write:!run_seq_write
           ~run_rand_read:!run_random_read
           ~run_read_committed:false
           ~seq_payload:!payload
           ~seq_iterations:!write_iters)
  end
