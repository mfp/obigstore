
include Data_model.S
include Data_model.BACKUP_SUPPORT with type backup_cursor := backup_cursor

(** Open or create a database in the given directory. *)
val open_db : string -> db

(** Close the DB. All further actions on it will raise an error. *)
val close_db : db -> unit
