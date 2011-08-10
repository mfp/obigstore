open Lwt
open Data_model

module Version_0_0_0 : Protocol.PAYLOAD =
struct

  module E =
  struct
    open Bytea

    let add_int32_le = add_int32_le
    let add_status = add_int32_le
    let add_int64_le = add_int64_le

    let add_raw_string b s = add_string b s

    let add_string b s =
      add_int32_le b (String.length s);
      add_string b s

    let add_list f b l =
      add_int32_le b (List.length l);
      List.iter (f b) l

    let add_tuple2 f g b (x, y) = f b x; g b y

    let add_option f b = function
        None -> add_int32_le b 0
      | Some x -> add_int32_le b 1;
                  f b x
  end

  module D =
  struct
    let get_int32_le = Lwt_io.LE.read_int
    let get_status = get_int32_le
    let get_int64_le = Lwt_io.LE.read_int64

    let get_string ich =
      lwt count = get_int32_le ich in
        Lwt_io.read ~count ich

    let get_list ?(do_reverse = true) f ich =
      let rec loop_read_list acc f ich = function
          n when n > 0 -> lwt x = f ich in loop_read_list (x :: acc) f ich (n-1)
        | _ (* 0 *) -> return acc in
      lwt len = get_int32_le ich in
      if do_reverse then
        (loop_read_list [] f ich len >|= List.rev)
      else
        (loop_read_list [] f ich len)

    let get_tuple2 f g ich =
      lwt x = f ich in
      lwt y = g ich in
        return (x, y)

    let get_option f ich =
      match_lwt get_int32_le ich with
          0 -> return None
        | _ -> lwt x = f ich in return (Some x)
  end

  let null_timestamp = String.make 8 '\255'

  let writer f ?(buf=Bytea.create 16) och ~request_id x =
    Bytea.clear buf;
    f buf x;
    Protocol.write_msg och request_id buf

  let reader f ich =
    let open Protocol in
    match_lwt D.get_status ich with
        0 -> f ich
      | -1 -> raise_lwt (Error Bad_request)
      | -2 -> raise_lwt (Error Unknown_keyspace)
      | n -> raise_lwt (Error (Other n))

  let bad_request =
    writer (fun b () -> E.add_status b (-1))

  let unknown_keyspace =
    writer (fun b () -> E.add_status b (-2))

  let return_keyspace =
    writer (fun b ks ->
              E.add_status b 0;
              E.add_int32_le b ks)

  let read_keyspace =
    reader D.get_int32_le

  let return_keyspace_list =
    writer (fun b l ->
              E.add_status b 0;
              E.add_list E.add_string b l)

  let read_keyspace_list = reader (D.get_list D.get_string)

  let return_table_list = return_keyspace_list

  let read_table_list = read_keyspace_list

  let return_table_size_on_disk =
    writer (fun b siz -> E.add_status b 0;
                         E.add_int64_le b siz)

  let read_table_size_on_disk = reader D.get_int64_le

  let return_key_range_size_on_disk = return_table_size_on_disk

  let read_key_range_size_on_disk = read_table_size_on_disk

  let return_keys = return_keyspace_list

  let read_keys = read_keyspace_list

  let return_key_count = return_table_size_on_disk

  let read_key_count = read_table_size_on_disk

  let add_timestamp b = function
      No_timestamp -> E.add_raw_string b null_timestamp
    | Timestamp t -> E.add_int64_le b t

  external decode_int64_le : string -> int -> Int64.t = "ostore_decode_int64_le"

  let timestamp_buf = String.create 8

  let get_timestamp ich =
    Lwt_io.read_into_exactly ich timestamp_buf 0 8 >>
      match timestamp_buf with
          x when x = null_timestamp -> return No_timestamp
        | s -> return (Timestamp (decode_int64_le s 0))

  let add_column b { name; data; timestamp; } =
    E.add_string b name;
    E.add_string b data;
    add_timestamp b timestamp

  let get_column ich =
    lwt name = D.get_string ich in
    lwt data = D.get_string ich in
    lwt timestamp = get_timestamp ich in
      return { name; data; timestamp }

  let return_slice =
    writer
      (fun b (last_key, key_data_list) ->
         let add_key_data b { key; last_column; columns; } =
           E.add_string b key;
           E.add_string b last_column;
           E.add_list add_column b columns
         in
           E.add_status b 0;
           E.add_option E.add_string b last_key;
           E.add_list add_key_data b key_data_list)

  let read_slice =
    reader
      (fun ich ->
         let read_key_data ich =
           lwt key = D.get_string ich in
           lwt last_column = D.get_string ich in
           lwt columns = D.get_list get_column ich in
             return { key; last_column; columns; } in
         lwt last_key = D.get_option D.get_string ich in
         lwt key_data_list = D.get_list read_key_data ich in
           return (last_key, key_data_list))

  let return_slice_values =
    writer
      (fun b (last_key_opt, data_list) ->
         E.add_status b 0;
         E.add_option E.add_string b last_key_opt;
         E.add_list
           (E.add_tuple2
              E.add_string
              (E.add_list (E.add_option E.add_string)))
           b data_list)

  let read_slice_values =
    reader
      (fun ich ->
         lwt last_key_opt = D.get_option D.get_string ich in
         lwt data_list =
           D.get_list
             (D.get_tuple2
                D.get_string
                (D.get_list (D.get_option D.get_string)))
             ich
         in return (last_key_opt, data_list))

  let return_columns =
    writer
      (fun b x ->
         E.add_status b 0;
         E.add_option
           (E.add_tuple2 E.add_string (E.add_list add_column)) b x)

  let read_columns =
    reader (D.get_option (D.get_tuple2 D.get_string (D.get_list get_column)))

  let return_column_values =
    writer (fun b x ->
              E.add_status b 0;
              E.add_list (E.add_option E.add_string) b x)

  let read_column_values =
    reader (D.get_list (D.get_option D.get_string))

  let return_column =
    writer (fun b x ->
              E.add_status b 0;
              E.add_option (E.add_tuple2 E.add_string add_timestamp) b x)

  let read_column =
    reader (D.get_option (D.get_tuple2 D.get_string get_timestamp))

  let return_ok = writer (fun b () -> E.add_status b 0)

  let read_ok = reader (fun ich -> return ())
end

