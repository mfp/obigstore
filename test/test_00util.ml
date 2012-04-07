(*
 * Copyright (C) 2011-2012 Mauricio Fernandez <mfp@acm.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version,
 * with the special exception on linking described in file LICENSE.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *)

open Printf
open OUnit
open Lwt

module List = struct include List include BatList end
module String = struct include String include BatString end

let rounds = ref 1

let keep_tmp = ref false

let string_of_list f l = "[ " ^ String.concat "; " (List.map f l) ^ " ]"
let string_of_pair f (a, b) = sprintf "(%s, %s)" (f a) (f b)
let string_of_tuple2 f g (a, b) = sprintf "(%s, %s)" (f a) (g b)

let string_of_option f = function
    None -> "None"
  | Some x -> sprintf "Some %s" (f x)

let assert_failure_fmt fmt = Printf.ksprintf assert_failure fmt

let aeq printer = assert_equal ~printer

let aeq_int = assert_equal ~printer:(sprintf "%d")
let aeq_bool = assert_equal ~printer:(sprintf "%b")
let aeq_string = assert_equal ~printer:(sprintf "%S")
let aeq_int64 = assert_equal ~printer:(sprintf "%Ld")

let aeq_none ?msg x =
  assert_equal ?msg ~printer:(function None -> "None" | Some _ -> "Some _") None x

let cmp_tuple2 f g (a1, a2) (b1, b2) = f a1 b1 && g a2 b2

let cmp_option = function
    None -> fun o1 o2 -> compare o1 o2 = 0
  | Some f -> (fun x y -> match x, y with
                   None, Some _ | Some _, None -> false
                 | None, None -> true
                 | Some x, Some y -> f x y)

let cmp_list = function
    None -> fun l1 l2 -> compare l1 l2 = 0
  | Some f ->
      fun l1 l2 ->
        try
          List.fold_left2 (fun b x y -> b && f x y) true l1 l2
        with Invalid_argument _ -> false

let aeq_some ?msg ?cmp f x =
  assert_equal ?msg ~cmp:(cmp_option cmp) ~printer:(string_of_option f) (Some x)

let assert_not_found ?msg f =
  assert_raises ?msg Not_found (fun _ -> ignore (f ()))

let aeq_list ?cmp f =
  assert_equal ~cmp:(cmp_list cmp) ~printer:(string_of_list f)

let aeq_string_list = aeq_list (sprintf "%S")

let range min max =
  List.init (max - min + 1) (fun n -> n + min)

let to_hex s =
  String.concat " "
    (List.map (fun c -> sprintf "%02x" (Char.code c))
       (String.explode s))

let mk_chr_str n = String.make 1 (Char.chr n)

let string_of_bytes l = String.concat "" (List.map mk_chr_str l)

let shuffle l =
  let a = Array.of_list l in
  let n = ref (Array.length a) in
    while !n > 1 do
      decr n;
      let k = Random.int (!n + 1) in
      let tmp = a.(k) in
        a.(k) <- a.(!n);
        a.(!n) <- tmp
    done;
    Array.to_list a

let make_temp_file ?(prefix = "temp") ?(suffix = ".dat") () =
  let file = Filename.temp_file prefix suffix in
    at_exit (fun () -> if not !keep_tmp then Sys.remove file);
    file

let make_temp_dir ?(prefix = "temp") () =
  let path = Filename.temp_file prefix "" in
    Sys.remove path;
    Unix.mkdir path 0o755;
    at_exit
      (fun () -> if not !keep_tmp then
                   ignore (Sys.command (sprintf "rm -rf %S" path)));
    path

let random_list gen len =
  Array.to_list (Array.init len (fun _ -> gen ()))

let random_string () =
  let s = String.create (10 + Random.int 4096) in
    for i = 0 to String.length s - 1 do
      s.[i] <- Char.chr (Char.code 'a' + Random.int 26);
    done;
    s

let run_lwt f =
  Lwt_unix.run (f ())

let random_pair f g () = (f (), g ())

let all_tests = ref []

let register_tests name tests =
  let open OUnit in
    all_tests := (name >::: tests) :: !all_tests

let get_all_tests () = List.rev !all_tests

let lwt_tests
      ?(sequential_timeout = 30.0) ?(concurrent_timeout = 60.0)
      ?(sequential=[]) concurrent =
  let concurrent =
    List.map
      (fun (name, f) ->
         let t, u = Lwt.task () in
         let init_t, init_u = Lwt.task () in
         let g () =
           ignore begin try_lwt
             lwt () = init_t in
             lwt () = f () in
               Lwt.wakeup u ();
               return ()
           with e -> Lwt.wakeup_exn u e;
                     return ()
           end
         in (name, t, g, init_u))
      concurrent in

  let now () = Unix.gettimeofday () in
  let max_time = ref (now ()) in
  let launch_concurrent =
    lazy begin
      max_time := now () +. concurrent_timeout;
      List.iter (fun (_, _, g, _) -> g ()) concurrent
    end in

  let test_func t () =
    Lazy.force launch_concurrent;
    Lwt_unix.run
      (let timeout = max (!max_time -. now ()) 0.01 in
           Lwt_unix.with_timeout timeout (fun () -> t)) in

  let concurrent_test_launcher (name, _, _, u) =
    sprintf "Launch %s" name >:: (fun () -> Lwt.wakeup u ()) in
  let concurrent_test (name, t, _, _) = name >:: test_func t in

  let sequential_tests =
    List.map
      (fun (name, f) -> name >:: run_lwt ~timeout:sequential_timeout f)
      sequential
  in
    sequential_tests @
    List.map concurrent_test_launcher concurrent @
    List.map concurrent_test concurrent

