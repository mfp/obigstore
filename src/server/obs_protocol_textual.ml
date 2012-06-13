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
open Obs_data_model
open Obs_request
open Obs_request.Request

exception Bad_header

module Option = BatOption

module Version_0_0_0 : Obs_protocol.SERVER_FUNCTIONALITY =
struct
  let read_exactly ich n =
    let s = String.create n in
      Lwt_io.read_into_exactly ich s 0 n >>
      return s

  let drop_fst_char s = String.sub s 1 (String.length s - 1)

  let read_line_drop_fst ich = Lwt_io.read_line ich >|= drop_fst_char

  let read_arg ich =
    lwt len = read_line_drop_fst ich >|= int_of_string in
    lwt data = read_exactly ich len in
    lwt _ = Lwt_io.read_line ich in
      return data

  let read_arg_opt ich =
    lwt len = read_line_drop_fst ich >|= int_of_string in
      if len <= 0 then return None
      else begin
        lwt data = read_exactly ich len in
        lwt _ = Lwt_io.read_line ich in
          return (Some data)
      end

  let read_header ich =
    lwt id =
      lwt s = Lwt_io.read_line ich in
        if String.length s = 0 || s.[0] <> '@' then raise_lwt Bad_header else
        return (drop_fst_char s) in
    lwt nargs = read_line_drop_fst ich >|= int_of_string in
      (* FIXME: should indicate bad request and return None, then retry *)
      if nargs < 1 then raise_lwt Bad_header else
      lwt cmd = read_arg ich in
        return (id, nargs, cmd)

  let cmds = Hashtbl.create 13

  let rec discard_args nargs ich och =
    if nargs <= 0 then return ()
    else begin
      lwt _ = read_arg ich in
        discard_args (nargs - 1) ich och
    end

  let rec read_request scratch ich och =
    lwt id, nargs, cmd = read_header ich in
      match try Some (Hashtbl.find cmds cmd) with Not_found -> None with
          None -> discard_args nargs ich och >> read_request scratch ich och
        | Some (f, arity) ->
            if arity >= 0 && nargs - 1 <> arity ||
              nargs < -arity then begin
              (* FIXME: should indicate that cmd is not known *)
              discard_args nargs ich och >>
              read_request scratch ich och
            end else begin
              lwt r = f nargs ich och in
                return (Some (r, id, nargs))
            end

  let cmd name arity f = Hashtbl.add cmds name (f, arity);;

  cmd "KSREGISTER" 1 begin fun nargs ich och ->
    lwt name = read_arg ich in
      return (Register_keyspace { Register_keyspace.name })
  end;;

  cmd "KSGET" 1 begin fun nargs ich och ->
    lwt name = read_arg ich in
      return (Get_keyspace { Get_keyspace.name })
  end;;

  cmd "KSLIST" 0 begin fun nargs ich och ->
    return (List_keyspaces { List_keyspaces.prefix = "" })
  end;;

  let read_table ich = read_arg ich >|= table_of_string

  let read_ks ich och = read_arg ich >|= int_of_string;;

  cmd "TLIST" 1 begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
      return (List_tables { List_tables.keyspace })
  end;;

  cmd "TSIZE" 2 begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
    lwt table = read_table ich in
      return (Table_size_on_disk { Table_size_on_disk.keyspace; table; })
  end;;

  cmd "RSIZE" 4 begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
    lwt table = read_table ich in
    lwt first = read_arg_opt ich in
    lwt up_to = read_arg_opt ich in
    let range = { first; up_to; reverse = false; } in
      return (Key_range_size_on_disk
                { Key_range_size_on_disk.keyspace; table; range })
  end;;

  let tx_type_of_string = function
      "RR" -> Tx_type.Repeatable_read
    | "RC" -> Tx_type.Read_committed
    | _ -> failwith "bad tx_type";;

  cmd "BEGIN" 2 begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
    lwt tx_type =
      read_arg_opt ich >|=
      Option.map_default tx_type_of_string Tx_type.Read_committed
    in
      return (Begin { Begin.keyspace; tx_type; })
  end;;

  cmd "COMMIT" 1 begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
      return (Commit { Commit.keyspace; })
  end;;

  cmd "ABORT" 1 begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
      return (Abort { Abort.keyspace; })
  end;;

  let read_nargs ich n =
    let rec read_narg_loop ich acc = function
        0 -> return (List.rev acc)
      | n ->
          lwt arg = read_arg ich in
            read_narg_loop ich (arg :: acc) (n - 1)
  in read_narg_loop ich [] n

  let read_bool ich =
    match_lwt read_arg ich with
        "" | "0" | "false" | "FALSE" -> return false
      | _ -> return true;;

  cmd "LOCK" (-2) begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
    lwt shared = read_bool ich in
    lwt names = read_nargs ich (nargs - 2) in
      return (Lock { Lock.keyspace; names; shared; })
  end;;

  cmd "KEXIST" (-2) begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
    lwt table = read_table ich in
    lwt keys = read_nargs ich (nargs - 2) in
      return (Exist_keys { Exist_keys.keyspace; table; keys; })
  end;;

  let read_int ich = read_arg ich >|= int_of_string
  let read_int_opt ich = read_arg_opt ich >|= Option.map int_of_string

  let read_key_range ich =
    lwt first = read_arg_opt ich in
    lwt up_to = read_arg_opt ich in
    lwt reverse = read_bool ich in
      return (Key_range.Key_range { first; up_to; reverse })

  let read_key_range_discrete ich =
    lwt nkeys = read_int ich in
    lwt keys = read_nargs ich nkeys in
      return (Key_range.Keys keys)

  let read_col_range ich =
    lwt first = read_arg_opt ich in
    lwt up_to = read_arg_opt ich in
    lwt reverse = read_bool ich in
      return
        (Column_range.Column_range_union
           [ Simple_column_range.Column_range { first; up_to; reverse } ])

  let read_col_range_discrete ich =
    lwt ncols = read_int ich in
    lwt cols = read_nargs ich ncols in
      return (Column_range.Column_range_union [ Simple_column_range.Columns cols ]);;

  cmd "KGETRANGE" 6 begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
    lwt table = read_table ich in
    lwt max_keys = read_int_opt ich in
    lwt key_range = read_key_range ich in
      return (Get_keys { Get_keys.keyspace; table; max_keys; key_range })
  end;;

  cmd "KGET" (-3) begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
    lwt table = read_table ich in
    lwt max_keys = read_int_opt ich in
    lwt keys = read_nargs ich (nargs - 3) in
    let key_range = Key_range.Keys keys in
      return (Get_keys { Get_keys.keyspace; table; max_keys; key_range })
  end;;

  cmd "KCOUNTRANGE" 5 begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
    lwt table = read_table ich in
    lwt key_range = read_key_range ich in
      return (Count_keys { Count_keys.keyspace; table; key_range })
  end;;

  cmd "KCOUNT" (-2) begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
    lwt table = read_table ich in
    lwt keys = read_nargs ich (nargs - 3) in
    let key_range = Key_range.Keys keys in
      return (Count_keys { Count_keys.keyspace; table; key_range })
  end;;

  cmd "SGETDD" (-6) begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
    lwt table = read_table ich in
    lwt max_keys = read_int_opt ich in
    lwt max_columns = read_int_opt ich in
    lwt key_range = read_key_range_discrete ich in
    lwt column_range = read_col_range_discrete ich in
      return (Get_slice { Get_slice.keyspace; table; max_keys; max_columns;
                          decode_timestamps = true; key_range; predicate = None;
                          column_range; })
  end;;

  cmd "SGETCD" (-8) begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
    lwt table = read_table ich in
    lwt max_keys = read_int_opt ich in
    lwt max_columns = read_int_opt ich in
    lwt key_range = read_key_range ich in
    lwt column_range = read_col_range_discrete ich in
      return (Get_slice { Get_slice.keyspace; table; max_keys; max_columns;
                          decode_timestamps = true; key_range; predicate = None;
                          column_range; })
  end;;

  cmd "SGETCC" 10 begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
    lwt table = read_table ich in
    lwt max_keys = read_int_opt ich in
    lwt max_columns = read_int_opt ich in
    lwt key_range = read_key_range ich in
    lwt column_range = read_col_range ich in
      return (Get_slice { Get_slice.keyspace; table; max_keys; max_columns;
                          decode_timestamps = true; key_range; predicate = None;
                          column_range; })
  end;;

  cmd "SGETDC" (-8) begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
    lwt table = read_table ich in
    lwt max_keys = read_int_opt ich in
    lwt max_columns = read_int_opt ich in
    lwt key_range = read_key_range_discrete ich in
    lwt column_range = read_col_range ich in
      return (Get_slice { Get_slice.keyspace; table; max_keys; max_columns;
                          decode_timestamps = true; key_range; predicate = None;
                          column_range; })
  end;;

  cmd "SGETVALC" (-6) begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
    lwt table = read_table ich in
    lwt max_keys = read_int_opt ich in
    lwt key_range = read_key_range ich in
    lwt columns = read_nargs ich (nargs - 6) in
      return (Get_slice_values_timestamps
                { Get_slice_values_timestamps.keyspace; table; max_keys;
                  key_range; columns; })
  end;;

  cmd "SGETVALD" (-4) begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
    lwt table = read_table ich in
    lwt max_keys = read_int_opt ich in
    lwt nkeys = read_int ich in
    lwt key_range =
      lwt keys = read_nargs ich nkeys in
        return (Key_range.Keys keys) in
    let ncols = nargs - 4 - nkeys in
    lwt columns = read_nargs ich ncols in
      return (Get_slice_values_timestamps
                { Get_slice_values_timestamps.keyspace; table; max_keys;
                  key_range; columns; })
  end;;

  let rec read_cols ich acc = function
      0 -> return acc
    | n ->
        lwt name = read_arg ich in
        lwt data = read_arg ich in
        lwt timestamp =
          read_arg_opt ich >|=
          Option.map_default
            (fun s -> Timestamp.Timestamp (Int64.of_string s))
            Timestamp.No_timestamp
        in read_cols ich ({ Column.name; data; timestamp; } :: acc) (n - 1)

  let rec read_key_data ich acc = function
      0 -> return acc
    | n ->
        lwt key = read_arg ich in
        lwt ncols = read_int ich in
        lwt cols = read_cols ich [] ncols in
          read_key_data ich ((key, cols) :: acc) (n - 1);;

  cmd "SPUT" (-3) begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
    lwt table = read_table ich in
    lwt nkeys = read_int ich in
    lwt data = read_key_data ich [] nkeys in
      return (Put_columns { Put_columns.keyspace; table; data; })
  end;;

  cmd "CDEL" (-3) begin fun nargs ich och ->
    lwt keyspace = read_ks ich och in
    lwt table = read_table ich in
    lwt key = read_arg ich in
    lwt columns = read_nargs ich (nargs - 3) in
      return (Delete_columns { Delete_columns.keyspace; table; key; columns; })
  end;;

  cmd "PROP" 1 begin fun nargs ich och ->
    lwt property = read_arg ich in
      return (Get_property { Get_property.property; })
  end;;

  let write_line och l = Lwt_io.write och l >> Lwt_io.write och "\r\n"

  let writer f ?buf och ~request_id x =
    write_line och ("@" ^ request_id) >>
    f ?buf och x

  let write_error code desc ?buf och ~request_id () =
    write_line och request_id >>
    write_line och ("-" ^ string_of_int code)

  let bad_request = write_error 400 "bad request"
  let unknown_keyspace = write_error 400 "unknown keyspace"
  let unknown_serialization = write_error 400 "unknown serialization"
  let internal_error = write_error 500 "internal error"
  let deadlock = write_error 501 "deadlock"
  let dirty_data = write_error 502 "dirty data"

  let int64_writer =
    writer (fun ?buf och x -> write_line och (":" ^ Int64.to_string x))

  let int_writer =
    writer (fun ?buf och x -> write_line och (":" ^ string_of_int x))

  let return_keyspace = int_writer

  let write_nargs och n =
    write_line och ("*" ^ string_of_int n)

  let write_datum och s =
    write_line och ("$" ^ string_of_int (String.length s)) >>
    write_line och s

  let write_datum_opt och = function
      None -> write_line och "$-1"
    | Some s -> write_datum och s

  let write_int och n = write_datum och (string_of_int n)

  let write_bool och b = write_datum och (if b then "1" else "0")

  let return_keyspace_maybe =
    writer (fun ?buf och ks -> match ks with
                None -> write_nargs och 0
              | Some ks ->
                  write_nargs och 1 >>
                  write_datum och (string_of_int ks))

  let return_keyspace_list =
    writer (fun ?buf och l ->
              write_nargs och (List.length l) >>
              Lwt_list.iter_s (write_datum och) l)

  let return_table_list = return_keyspace_list
  let return_table_size_on_disk = int64_writer
  let return_key_range_size_on_disk = int64_writer
  let return_keys = return_keyspace_list
  let return_key_count = int64_writer

  let write_column och { name; data; timestamp } =
    write_datum och name >>
    write_datum och data >>
    write_datum_opt och
      (match timestamp with Timestamp x -> Some (Int64.to_string x) | _ -> None)

  let return_slice =
    writer (fun ?buf och (k, kd) ->
              let nargs =
                1 +
                2 * List.length kd +
                List.fold_left (fun s kd -> s + 3 * List.length kd.columns) 0 kd
              in
                write_nargs och nargs >>
                write_datum_opt och k >>
                Lwt_list.iter_s
                  (fun { key; columns; _ } ->
                     write_datum och key >>
                     write_int och (List.length columns) >>
                     Lwt_list.iter_s (write_column och) columns)
                  kd)

  let return_slice_values =
    writer (fun ?buf och (k, kd) ->
              let nargs =
                1 +
                List.length kd +
                List.fold_left (fun s (_, cs) -> s + List.length cs) 0 kd
              in
                write_nargs och nargs >>
                write_datum_opt och k >>
                Lwt_list.iter_s
                  (fun (k, cs) ->
                     write_datum och k >>
                     Lwt_list.iter_s (write_datum_opt och) cs)
                  kd)

  let write_int64 och n = write_datum och (Int64.to_string n)

  let return_slice_values_timestamps =
    writer (fun ?buf och (k, kd) ->
              let nargs =
                List.length kd +
                List.fold_left (fun s (_, cs) -> s + 2 * List.length cs) 0 kd
              in
                write_nargs och nargs >>
                write_datum_opt och k >>
                Lwt_list.iter_s
                  (fun (k, cs) ->
                     write_datum och k >>
                     Lwt_list.iter_s
                       (function
                            None -> write_datum_opt och None
                          | Some (data, ts) ->
                              write_datum och data >>
                              write_int64 och ts)
                       cs)
                  kd)

  let return_columns =
    writer (fun ?buf och -> function
                None -> write_nargs och 0
              | Some (_, l) -> write_nargs och (3 * List.length l) >>
                              Lwt_list.iter_s (write_column och) l)

  let return_column_values =
    writer (fun ?buf och l ->
                write_nargs och (List.length l) >>
                Lwt_list.iter_s (write_datum_opt och) l)

  let return_column =
    writer (fun ?buf och -> function
                None -> write_nargs och 0
              | Some (data, ts) ->
                write_nargs och 2 >>
                write_datum och data >>
                write_datum_opt och
                  (Option.map Int64.to_string
                     (match ts with No_timestamp -> None | Timestamp ts -> Some ts)))

  let return_ok =
    writer (fun ?buf och () -> write_line och "+OK")

  let return_exist_result =
    writer (fun ?buf och l ->
              write_nargs och (List.length l) >>
              Lwt_list.iter_s (write_bool och) l)

  let return_notifications = return_keyspace_list

  let return_property =
    writer (fun ?buf och -> function
                None -> write_nargs och 0
              | Some s -> write_nargs och 1 >>
                         write_datum och s)

  let not_implemented ?buf och ~request_id x = failwith "NOT IMPLEMENTED"

  let return_backup_dump = not_implemented
  let return_backup_load_result = not_implemented
  let return_load_stats = not_implemented
  let return_raw_dump_id_and_timestamp = not_implemented
  let return_raw_dump_files = not_implemented
  let return_raw_dump_file_digest = not_implemented
end
