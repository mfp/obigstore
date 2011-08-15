
module Make : functor(P : Protocol.PAYLOAD) ->
sig
  include Data_model.S
  include Data_model.BACKUP_SUPPORT with type backup_cursor := backup_cursor
  val make : Lwt_io.input_channel -> Lwt_io.output_channel -> db
  val close : db -> unit
end
