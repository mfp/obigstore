
include Data_model.S

(** Open or create a database in the given directory. *)
val open_db : string -> db

(** Close the DB. All further actions on it will raise an error. *)
val close_db : db -> unit
