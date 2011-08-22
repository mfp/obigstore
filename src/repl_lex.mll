
{
 open Repl_gram

 let keyword_table = Hashtbl.create 13

 let () = 
   List.iter (fun (k, v) -> Hashtbl.add keyword_table k v)
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
     ]
     
}

rule token = parse
    [' ' '\t']       { token lexbuf }
  | "~"           { REVRANGE }
  | ':'            { RANGE }
  | '/'            { COND }
  | '['            { LBRACKET }
  | ']'            { RBRACKET }
  | ','            { COMMA }
  | '='            { EQ }
  | ['0'-'9']+ as lxm { INT(int_of_string lxm) }
  | ['A'-'Z' 'a'-'z'] ['A'-'Z' 'a'-'z' '0'-'9' '_' '.'] * as id 
        { try
            Hashtbl.find keyword_table id
          with Not_found -> ID id }
  | '"' ([^ '"']* as lxm) '"'
        { ID lxm }
  | "x\"" (['0'-'9' 'a'-'f' 'A'-'F']* as lxm) '"'
        { ID lxm (* FIXME hex to string *) }
  | ';'            { EOF }
  | _              { token lexbuf }
  | eof            { raise End_of_file }
