(** Misc. utility functions *)

(** Readable progress report.
  * Sample usage:
  *  [Progress_report.with_progress_report Lwt_io.stderr f]
  * where [f] calls [Progress_report.update] with the current value of the
  * progress counter (e.g. file position) repeatedly.
  * *)
module Progress_report :
sig
  type t

  val update : t -> Int64.t -> unit Lwt.t

  val with_progress_report :
    ?max:Int64.t -> Lwt_io.output_channel -> (t -> 'a Lwt.t) -> 'a Lwt.t
end
