open Lwt
open Test_00util

module CLIENT = Protocol_client.Make(Protocol_payload.Version_0_0_0)

module C =
struct
  let id = "Protocol_client atop Storage server"

  module SERVER = Protocol_server.Make(Storage)(Protocol_payload.Version_0_0_0)

  let with_db f =
    let dir = make_temp_dir () in
    let db = Storage.open_db dir in
    let ch1_in, ch1_out = Lwt_io.pipe () in
    let ch2_in, ch2_out = Lwt_io.pipe () in
    let srv_client_handle = SERVER.init db ch1_in ch2_out in
    let client = CLIENT.make ch2_in ch1_out in
      Lwt_unix.run begin
        try_lwt
          Lwt.pick [f client; SERVER.service srv_client_handle]
        finally
          CLIENT.close client;
          Storage.close_db db;
          return ()
      end
end

module TEST = Test_data_model.Run_test(CLIENT)(C)
