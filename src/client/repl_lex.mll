(*
 * Copyright (C) 2011 Mauricio Fernandez <mfp@acm.org>
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

{
 open Repl_gram

 let keyword_table = Hashtbl.create 13

 let keywords =
    [
      "keyspaces", KEYSPACES;
      "tables", TABLES;
      "size", SIZE;
      "begin", BEGIN;
      "commit", COMMIT;
      "abort", ABORT;
      "keys", KEYS;
      "count", COUNT;
      "get", GET;
      "put", PUT;
      "delete", DELETE;
      "lock", LOCK;
      "stats", STATS;
      "listen", LISTEN;
      "unlisten", UNLISTEN;
      "notify", NOTIFY;
      "await", AWAIT;
    ]

 let () = 
   List.iter (fun (k, v) -> Hashtbl.add keyword_table k v) keywords

let unescape_string =
  let lexer = Genlex.make_lexer [] in
  (fun s ->
     let tok_stream = lexer (Stream.of_string ("\"" ^ s ^ "\"")) in
       match Stream.peek tok_stream with
           Some (Genlex.String s) -> s
         | _ -> assert false)
}

rule token = parse
    [' ' '\t']       { token lexbuf }
  | "&&"           { AND }
  | "||"           { OR }
  | "~"            { REVRANGE }
  | ':'            { RANGE }
  | '/'            { COND }
  | '['            { LBRACKET }
  | ']'            { RBRACKET }
  | ','            { COMMA }
  | '<'            { LT }
  | "<="           { LE }
  | ">"            { GT }
  | ">="           { GE }
  | '='            { EQ }
  | '.' (['A'-'Z' 'a'-'z']+ as lxm)
                   { DIRECTIVE (String.lowercase lxm) }
  | ['0'-'9']+ as lxm { INT(int_of_string lxm) }
  | ['A'-'Z' 'a'-'z'] ['A'-'Z' 'a'-'z' '0'-'9' '_' '.'] * as id 
        { try
            Hashtbl.find keyword_table (String.lowercase id)
          with Not_found -> ID id }
  | '"' (("\\\\" | "\\" '"' | [^ '"'])* as lxm) '"'
        { ID (unescape_string lxm) }
  | "x\"" (['0'-'9' 'a'-'f' 'A'-'F']* as lxm) '"'
        { ID lxm (* FIXME hex to string *) }
  | ';'            { EOF }
  | _              { token lexbuf }
  | eof            { EOF }
