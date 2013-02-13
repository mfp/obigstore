(** Safe finalization in main thread. *)

val finalise_in_main_thread : ('a -> unit Lwt.t) -> 'a -> unit
