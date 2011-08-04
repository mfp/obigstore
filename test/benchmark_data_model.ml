
open Lwt
open Printf

module D = Data_model
module List = struct include BatList include List end

let mk_col ?(timestamp = D.Update.No_timestamp) ~name data =
  { D.Update.name; data; timestamp; }

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
          Lwt_list.iter_p (fun _ -> return ()) rows
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
  List.fold_left (fun s c -> s + String.length c.D.Update.data) 0 cols

let row_data_size_with_colnames (key, cols) =
  row_data_size (key, cols) +
  List.fold_left (fun s c -> s + String.length c.D.Update.name) 0 cols

let pr_separator () =
  print_endline (String.make 80 '-')

let run_put_colums_bm ~rounds ~iterations ~batch_size ~avg_cols =
  let tmp_dir = Test_00util.make_temp_dir () in
  let db = Data_model.open_db tmp_dir in
  let ks = D.register_keyspace db "ks1" in
    printf "column insertion time (%d row batches, %d columns avg)\n"
      batch_size avg_cols;
    pr_separator ();
    for i = 0 to rounds - 1 do
      let dt =
        Lwt_unix.run (bm_put_columns ~iters:iterations ~batch_size:1000
                        (make_row_dummy ~avg_cols) ks ~table:"dummy")
      in printf "%7d -> %7d    %8.5fs  (%.0f/s)   ~%Ld bytes\n%!"
           (i * iterations) ((i + 1) * iterations) dt (float iterations /. dt)
           (D.table_size_on_disk ks "dummy")
    done;
    let compute_avg_size f =
      float
        (List.fold_left
           (fun s r -> s + f r)
           0 (List.init 1000 (fun _ -> make_row_dummy ~avg_cols ()))) /.
      1000.  in
    let avg_row_size = compute_avg_size row_data_size in
    let avg_row_size' = compute_avg_size row_data_size_with_colnames in
    let size_on_disk = D.table_size_on_disk ks "dummy" in
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
      tmp_dir

let bm_sequential_read ~max_keys ?(max_columns = 10) dir =
  let db = Data_model.open_db dir in
  let ks = D.register_keyspace db "ks1" in
  let n_keys = ref 0 in
  let n_columns = ref 0 in
  let rec read_from tx first =
    lwt (last_key, l) =
      D.get_slice tx "dummy" ~max_keys ~max_columns
        (D.Data.Key_range { D.Data.first; up_to = None })
        D.Data.All_columns in
    let len = List.length l in
      n_keys := !n_keys + len;
      n_columns := !n_columns +
                   List.fold_left (fun s kd -> s + List.length kd.D.Data.columns) 0 l;
      match last_key with
        | Some k when len = max_keys -> read_from tx (Some k)
        | _ -> return () in
  let dt =
    Lwt_unix.run
      (time (fun () -> D.read_committed_transaction ks (fun tx -> read_from tx None)))
  in
    printf "Seq read (slice %4d): %9d keys %9d columns in %8.5fs (%d/s) (%d/s)\n%!"
      max_keys
      !n_keys !n_columns dt
      (truncate (float !n_keys /. dt))
      (truncate (float !n_columns /. dt))

let () =
  let db_dir =
    run_put_colums_bm ~rounds:10 ~iterations:10000 ~batch_size:1000 ~avg_cols:10
  in
    Test_00util.keep_tmp := false;
    print_endline "";
    bm_sequential_read ~max_keys:1000 db_dir;
    bm_sequential_read ~max_keys:500 db_dir;
    bm_sequential_read ~max_keys:200 db_dir;
    bm_sequential_read ~max_keys:100 db_dir;
    bm_sequential_read ~max_keys:50 db_dir;
    bm_sequential_read ~max_keys:20 db_dir
