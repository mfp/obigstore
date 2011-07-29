
let chr n =
  let s = String.create 1 in
    String.unsafe_set s 0 (Char.unsafe_chr n);
    s

let rec strneq s1 o1 s2 o2 = function
    0 -> true
  | len ->
      if String.unsafe_get s1 o1 = String.unsafe_get s2 o2 then
        strneq s1 (o1 + 1) s2 (o2 + 1) (len - 1)
      else
        false

let cmp_substrings s1 o1 l1 s2 o2 l2 =
  if o1 < 0 || l1 < 0 || o1 + l1 > String.length s1 ||
     o2 < 0 || l2 < 0 || o2 + l2 > String.length s2 then
    invalid_arg "String_util.cmp_substrings";
  let rec loop_cmp s1 p1 l1 s2 p2 l2 = function
      0 -> l1 - l2
    | n ->
        let c1 = Char.code (String.unsafe_get s1 p1) in
        let c2 = Char.code (String.unsafe_get s2 p2) in
          match c1 - c2 with
              0 -> loop_cmp s1 (p1 + 1) l1 s2 (p2 + 1) l2 (n - 1)
            | n -> n
  in loop_cmp s1 o1 l1 s2 o2 l2 (if l1 < l2 then l1 else l2)
