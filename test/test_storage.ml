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
open Test_00util

module TEST =
  Test_data_model.Run_test
    (Obs_storage)
    (struct
       let id = "Obs_storage"

       let with_db f =
         let dir = make_temp_dir () in
         let db = Obs_storage.open_db dir in
           try_lwt
             f db
           finally
             Obs_storage.close_db db

       let with_db_pool f =
         with_db
           (fun db ->
              let pool = Lwt_pool.create 100 (fun () -> return db) in
                f pool)
       end)
