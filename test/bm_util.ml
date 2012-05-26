

module Cheapo_prng : sig
  type t
  val make : seed:int -> t
  val next : t -> int
end =
struct
  (* TODO: replace with faster (even if worse quality) PRNG *)
  type t = unit
  let make ~seed = ()
  let next () = Random.int 0x3FFFFFF
end

let rand_char prng =
  (* ' ' to '_' *)
  Char.unsafe_chr (32 + (Cheapo_prng.next prng land 63))

let min_i (x : int) y = if x < y then x else y

let rand_string prng ~compressibility_ratio n =
  let s = String.create n in
  let off = ref (truncate (compressibility_ratio *. float n)) in
    for i = 0 to !off - 1 do
      String.unsafe_set s i (rand_char prng);
    done;
    while !off < n do
      let len = min_i !off (n - !off) in
        String.blit s 0 s !off len;
        off := !off + len
    done;
    s

let random_string_maker ~compressibility_ratio prng =
  let buf =
    let b = Buffer.create (256 * 1024) in
      for i = 1 to 16 * 1024 do
        Buffer.add_string b (rand_string prng ~compressibility_ratio 32)
      done;
      Buffer.contents b in
  let off = ref 0 in
    `Staged
      (fun n ->
         if !off + n > String.length buf then off := 0;
         let s = String.sub buf !off n in
           off := !off + n;
           s)


let cheapo_die p =
  let a = Array.init (1 lsl 13) (fun _ -> Random.float 1.0 < p) in
  let n = ref 0 in
    (fun () ->
       incr n;
       n := !n land (Array.length a - 1);
       a.(!n))

let incr_rand_char s off =
  match s.[off] with
      ' ' .. '^' as c -> s.[off] <- Char.chr (Char.code c + 1)
    | '_' -> s.[off] <- ' '
    | _ -> ()

let incr_rand_string s =
  let rec incr_loop s off =
    if off >= 0 then begin
      incr_rand_char s off;
      if s.[off] = ' ' then incr_loop s (off - 1)
    end
  in incr_loop s (String.length s - 1)

let mixed_load_gen ~hotspots ~key_size ~value_size ~p_seq ~compressibility_ratio prng =
  let `Staged random_string = random_string_maker ~compressibility_ratio prng in
  let remembered = Array.init (1 lsl hotspots) (fun _ -> random_string key_size) in
  let idx = ref 0 in
  let die = cheapo_die p_seq in
    (fun i ->
       incr idx;
       idx := !idx land (Array.length remembered - 1);
       let key =
         if die () then begin
           let s = remembered.(!idx) in
             incr_rand_string s;
             s
         end else begin
           let s = random_string key_size in
             remembered.(!idx) <- s;
             incr idx;
             s
         end in
       let v = random_string value_size in
         (key, v))

let zero_pad n s =
  let len = String.length s in
    if len >= n then s
    else String.make (n - len) '0' ^ s
