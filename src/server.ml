open Lwt
open Printf

module Make(D : Data_model.S)(P : Protocol.PAYLOAD) =
struct
  module S = Protocol_server.Make(Storage)(Protocol_payload.Version_0_0_0)

  type conn =
      {
        addr : Lwt_unix.sockaddr;
        ich : Lwt_io.input_channel;
        och : Lwt_io.output_channel;
      }

  let rec run_server db address port =
    let rec accept_loop sock =
      begin try_lwt
        lwt (fd, addr) = Lwt_unix.accept sock in
          Lwt_unix.setsockopt fd Unix.TCP_NODELAY true;
          ignore begin try_lwt
            let ich = Lwt_io.of_fd Lwt_io.input fd in
            let och = Lwt_io.of_fd Lwt_io.output fd in
              try_lwt
                handle_connection db { ich; och; addr }
              finally
                Lwt_io.abort och
          with e ->
            try_lwt Lwt_unix.close fd with _ -> return ()
          end;
          return ()
      with e ->
        eprintf "Got toplevel exception: %s\n%!" (Printexc.to_string e);
        Printexc.print_backtrace stderr;
        Lwt_unix.sleep 0.05
      end >>
      accept_loop sock in

    let sock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
      Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
      Lwt_unix.bind sock address;
      Lwt_unix.listen sock 1024;
      accept_loop sock

  and handle_connection db conn =
    S.service (S.init db conn.ich conn.och)
end
