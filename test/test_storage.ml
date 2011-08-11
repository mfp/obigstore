open Test_00util

module TEST =
  Test_data_model.Run_test
    (Storage)
    (struct
       let id = "Storage"

       let with_db f =
         let dir = make_temp_dir () in
         let db = Storage.open_db dir in
           try
             Lwt_unix.run (f db)
           with e -> Storage.close_db db; raise e
     end)
