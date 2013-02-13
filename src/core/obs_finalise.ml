
let thread_id () = Thread.(id (self ()))

let main_thread = thread_id ()

let ensure_termination t =
  if Lwt.state t = Lwt.Sleep then begin
    let hook = Lwt_sequence.add_l (fun _ -> t) Lwt_main.exit_hooks in
    (* Remove the hook when t has terminated *)
    ignore (try_lwt t finally Lwt_sequence.remove hook; Lwt.return ())
  end

let finaliser f x =
  ensure_termination (f x)

let queue = Queue.create ()
let mutex = Mutex.create ()

let notification =
  Lwt_unix.make_notification
    (fun () ->
       Mutex.lock mutex;
       let f = Queue.take queue in
         Mutex.unlock mutex;
         ignore (f ()))

let run_in_main f =
  Mutex.lock mutex;
  Queue.add f queue;
  Mutex.unlock mutex;
  Lwt_unix.send_notification notification

let finalise_in_main_thread f x =
  let f x =
    if thread_id () = main_thread then
      ensure_termination (f x)
    else
      run_in_main (fun () -> f x)
  in
    Gc.finalise f x

