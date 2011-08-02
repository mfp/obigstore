
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

let bm_put_columns ?(iters=10000) ?(batch_size=1000) make_row db ~keyspace ~table =
  let ks = D.register_keyspace db keyspace in
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

let make_row_dummy () =
  (sprintf "%d%d" (Random.int 0x3FFFFFF) (Random.int 0x3FFFFFF),
   List.init (Random.int 20)
     (fun i ->
        mk_col ~name:(string_of_int i) (string_of_int (Random.int 0x3FFFFFF))))

let () =
  let db = Data_model.open_db (Test_00util.make_temp_dir ()) in
    print_endline "column insertion time (100 row batches, 10 columns avg)";
    for i = 0 to 19 do
      let dt =
        Lwt_unix.run (bm_put_columns ~iters:10000 ~batch_size:1000
                        make_row_dummy db ~keyspace:"ks1" ~table:"dummy")
      in printf "%7d -> %7d    %8.5fs  (%.0f/s)\n%!"
           (i * 10000) ((i + 1) * 10000) dt (float 10000 /. dt)
    done
