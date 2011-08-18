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

module D = Storage
module DM = Data_model
module List = struct include BatList include List end

module Make
  (D : Data_model.S)
  (C : sig
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
          (DM.Key_range { DM.first; up_to = None })
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

  let run ~rounds ~iterations ~avg_cols ~batch_size
          ~bm_put ~bm_seq_read ~bm_read_committed =
    lwt db = C.make_tmp_db () in
    lwt () =
      if not bm_put then return ()
      else run_put_colums_bm ~rounds ~iterations ~batch_size ~avg_cols db
    in
      Test_00util.keep_tmp := false;
      if not bm_seq_read then
        return ()
      else begin
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
               (if bm_read_committed then [ true; false ] else [false]))
          [ 1; 5; 10; 100 ]
      end
end

let server = ref "127.0.0.1"
let port = ref 12050
let remote_test = ref false
let run_put = ref true
let run_seq_read = ref true
let db_dir = ref None
let rounds = ref 10
let iterations = ref 10000
let batch_size = ref 1000
let avg_cols = ref 10

let params =
  Arg.align
    [
      "-rounds", Arg.Set_int rounds,
        "N Run N put_columns rounds (default: 10).";
      "-iterations", Arg.Set_int iterations,
        "N Insert N keys per round. (default: 10000)";
      "-columns", Arg.Set_int avg_cols,
        "N Insert N columns in average per key. (default: 10)";
      "-batch-size", Arg.Set_int batch_size,
        "N Insert N keys per batch insert. (default: 1000)";
      "-put-only", Arg.Unit (fun () -> run_put := true; run_seq_read := false),
        " Benchmark only put_columns.";
      "-read-only", Arg.Unit (fun () -> run_put := false; run_seq_read := true),
        " Benchmark only sequential read.";
      "-dir", Arg.String (fun s -> db_dir := Some s; remote_test := false),
        "PATH Run locally against LocalDB at PATH.";
      "-remote", Arg.Set remote_test, " Benchmark against server.";
      "-server", Arg.Set_string server, "ADDR Connect to server at ADDR.";
      "-port", Arg.Set_int port, "N Connect to server port N (default: 12050)";
    ]

module CLIENT = Protocol_client.Make(Protocol_payload.Version_0_0_0)

let usage_message = "Usage: benchmark [options]"

module BM_Storage =
  Make(Storage)
    (struct
       let make_tmp_db () =
         let tmp_dir = match !db_dir with
             None -> Test_00util.make_temp_dir ()
           | Some d -> d in
         let db = D.open_db tmp_dir in
           return db
     end)

let () =
  Printexc.record_backtrace true;
  Arg.parse params ignore usage_message;
  if not !remote_test then
    Lwt_unix.run
      (BM_Storage.run
         ~rounds:!rounds ~iterations:!iterations ~avg_cols:!avg_cols
         ~batch_size:!batch_size
         ~bm_put:!run_put ~bm_seq_read:!run_seq_read
         ~bm_read_committed:true)
  else begin
    let addr = Unix.ADDR_INET (Unix.inet_addr_of_string !server, !port) in
    let module C =
      struct
        let make_tmp_db () =
          lwt ich, och = Lwt_io.open_connection addr in
            return (CLIENT.make ich och)
      end in
    let module BM = Make(CLIENT)(C) in
      Lwt_unix.run
        (BM.run
           ~rounds:!rounds ~iterations:!iterations ~avg_cols:!avg_cols
           ~batch_size:!batch_size
           ~bm_put:!run_put ~bm_seq_read:!run_seq_read
           ~bm_read_committed:false)
  end
