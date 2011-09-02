(** Weak references that don't prevent GC of the contained value *)

(** Weak reference type *)
type 'a t

(** Wrap the given value in a reference *)
val make : 'a -> 'a t

(** Return [Some value] if the value wasn't GCed *)
val get : 'a t -> 'a option
