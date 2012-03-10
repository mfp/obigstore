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

open Printf
open Lwt

let puts fmt = printf (fmt ^^ "\n%!")

let copy_stream ic oc =
  let buf = String.create 16384 in
  let rec copy_loop () =
    match_lwt Lwt_io.read_into ic buf 0 16384 with
        0 -> return ()
      | n -> Lwt_io.write_from_exactly oc buf 0 n >>
             copy_loop ()
  in copy_loop ()

let file_exists_with_size file size =
  try
    let open Unix.LargeFile in
    let st = stat file in
      match st.st_kind with
          Unix.S_REG -> st.st_size = size
        | _ -> false
  with Unix.Unix_error _ -> false

module Make(D : Obs_data_model.RAW_DUMP) =
struct
  let dump_local db dst =
    lwt dump = D.dump db in
    lwt files = D.list_files dump >|=
                List.sort (fun (n1, _) (n2, _) -> String.compare n1 n2) in
    lwt timestamp = D.timestamp dump in
    let nfiles, size =
      List.fold_left (fun (n, s) (_, fsiz) -> n + 1, Int64.add s fsiz) (0, 0L) files in
    let dstdir = match dst with
        None -> sprintf "dump-%Ld" timestamp
      | Some dst -> dst in
    let t0 = Unix.gettimeofday () in
      (try Unix.mkdir dstdir 0o750
       with Unix.Unix_error(Unix.EEXIST, _, _) -> ());
      puts "Dumping %s (%d files) to directory %s"
        (Obs_util.format_size 1.0 size) nfiles dstdir;
      Lwt_list.iter_s
        (fun (file, size) ->
           let dst = Filename.concat dstdir file in
             if file_exists_with_size dst size then begin
               puts "Skipping %s (%s)." file (Obs_util.format_size 1.0 size);
               return ()
             end else begin
               match_lwt D.open_file dump file with
                   None -> return ()
                 | Some ic ->
                       Lwt_io.with_file
                         ~mode:Lwt_io.output
                         ~flags:Unix.([O_NONBLOCK; O_CREAT; O_TRUNC; O_WRONLY])
                         dst (copy_stream ic)
             end)
        files >>
      let dt = Unix.gettimeofday () -. t0 in
        puts "Retrieved in %.2fs (%s/s)" dt (Obs_util.format_size (1.0 /. dt) size);
        D.release dump >>
        return dstdir

  let dump_local ?destdir db = dump_local db destdir
end
