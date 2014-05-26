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

{
 open Obs_repl_gram

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
      "txid", TXID;
      "stats", STATS;
      "listen", LISTEN;
      "unlisten", UNLISTEN;
      "listenp", LISTENP;
      "unlistenp", UNLISTENP;
      "notify", NOTIFY;
      "await", AWAIT;
      "shared", SHARED;
      "dump", DUMP;
      "local", LOCAL;
      "to", TO;
      "watch", WATCH;
      "compact", COMPACT;
    ]

 let () =
   List.iter (fun (k, v) -> Hashtbl.add keyword_table k v) keywords

let num_of_hexa_char = function
    '0'..'9' as c -> Char.code c - Char.code '0'
  | 'a'..'f' as c -> Char.code c - Char.code 'a' + 10
  | x -> failwith "Invalid character"

let unescape_string s =
  let o = Obs_bytea.create 13 in
  let len = String.length s in
  let rec proc_char n =
    if n >= String.length s then Obs_bytea.contents o
    else match s.[n] with
        '\\' ->
            if n + 1 >= len then failwith "invalid string literal";
            begin match s.[n+1] with
                'x' ->
                  if n + 3 >= len then failwith "invalid string literal";
                  Obs_bytea.add_byte o (num_of_hexa_char s.[n+2] lsl 8 lor
                                        num_of_hexa_char s.[n+3]);
                  proc_char (n + 4)
              | 'n' -> Obs_bytea.add_byte o 10; proc_char (n + 2)
              | x -> Obs_bytea.add_char o x; proc_char (n + 2)
            end
      | c -> Obs_bytea.add_char o c;
             proc_char (n + 1)
  in proc_char 0

let binary_of_hex s = Cryptokit.(transform_string (Hexa.decode ()) s)

}

rule token = parse
    [' ' '\t']       { token lexbuf }
  | "&&"           { AND }
  | "||"           { OR }
  | "~"            { REVRANGE }
  | ':'            { COLON }
  | '/'            { COND }
  | '['            { LBRACKET }
  | ']'            { RBRACKET }
  | '('            { LPAREN }
  | ')'            { RPAREN }
  | '{'            { LBRACE }
  | '}'            { RBRACE }
  | ','            { COMMA }
  | '<'            { LT }
  | "<="           { LE }
  | ">"            { GT }
  | ">="           { GE }
  | '='            { EQ }
  | '+'            { PLUS }
  | '-'            { MINUS }
  | '.' ("printer" | "PRINTER")
                   { PRINTER }
  | '.' (['A'-'Z' 'a'-'z']+ as lxm)
                   { DIRECTIVE (String.lowercase lxm) }
  | (['0'-'9']+ as lxm) 'L' { INT64(Big_int.big_int_of_string lxm, lxm) }
  | ['0'-'9']+ as lxm { INT(Big_int.big_int_of_string lxm, lxm) }
  | ['0'-'9']+ '.' (['0'-'9']*) ('e' ['+' '-']? ['0'-'9']+)? as lxm { FLOAT (float_of_string lxm, lxm) }
  | ['A'-'Z' 'a'-'z' '0'-'9' '_' '.'] * as id
        { try
            Hashtbl.find keyword_table (String.lowercase id)
          with Not_found ->
            match String.lowercase id with
                "true" -> TRUE id
              | "false" -> FALSE id
              | _ -> ID id }
  | '"' (("\\\\" | "\\" '"' | [^ '"'])* as lxm) '"'
        { ID (unescape_string lxm) }
  | "x\"" (['0'-'9' 'a'-'f' 'A'-'F']* as lxm) '"'
        { ID (binary_of_hex lxm) }
  | ';'            { EOF }
  | '#' [^'\n']*   { token lexbuf }
  | _              { token lexbuf }
  | eof            { EOF }
