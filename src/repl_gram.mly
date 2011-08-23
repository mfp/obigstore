
%{

open Repl_common
open Request
module R = Request

let all_keys =
  Key_range.Key_range { Range.first = None; up_to = None; reverse = false }
%}

%token <string> ID
%token <int> INT
%token <string> DIRECTIVE
%token KEYSPACES TABLES KEYSPACE SIZE BEGIN ABORT COMMIT KEYS COUNT GET PUT DELETE
%token LBRACKET RBRACKET RANGE REVRANGE COND EQ COMMA EOF

%start input
%type <Repl_common.req> input

%%

input : phrase EOF { $1 }

phrase : /* empty */  { Nothing }
  | KEYSPACES { Command (R.List_keyspaces { R.List_keyspaces.prefix = "" }) }
  | TABLES    { with_ks
                  (fun keyspace ->
                     R.List_tables { R.List_tables.keyspace }) }
  | size      { $1 }
  | BEGIN     { with_ks (fun keyspace -> R.Begin { R.Begin.keyspace }) }
  | COMMIT    { with_ks (fun keyspace -> R.Commit { R.Commit.keyspace }) }
  | ABORT     { with_ks (fun keyspace -> R.Abort { R.Abort.keyspace }) }
  | count     { $1 }
  | get       { $1 }
  | put       { $1 }
  | delete    { $1 }
  | directive { $1 }

size : 
    SIZE ID   { with_ks
                  (fun keyspace ->
                     R.Table_size_on_disk
                       { R.Table_size_on_disk.keyspace; table = $2; }) }
  | SIZE ID range_no_max
              { with_ks
                  (fun keyspace ->
                     R.Key_range_size_on_disk
                       { R.Key_range_size_on_disk.keyspace;
                         table = $2; range = $3 }) }

range_no_max : 
    LBRACKET opt_id RANGE opt_id RBRACKET
              { { Range.first = $2; up_to = $4; reverse = false; } }

count :
    COUNT ID opt_range
              { with_ks
                  (fun keyspace ->
                     let key_range = match $3 with
                         None -> all_keys
                       | Some (Range r, _) -> Key_range.Key_range r 
                       | Some (List l, _) -> Key_range.Keys l
                     in R.Count_keys { R.Count_keys.keyspace; table = $2;
                                          key_range; }) }

get :
    GET ID range opt_range
       { with_ks
           (fun keyspace ->
              let max_columns = match $4 with
                  Some (_, x) -> x
                | None -> None in
              let key_range = match fst $3 with
                  Range r -> Key_range.Key_range r
                | List l -> Key_range.Keys l in
              let column_range = match $4 with
                  None -> Column_range.All_columns
                | Some (Range r, _) -> Column_range.Column_range r
                | Some (List l, _) -> Column_range.Columns l
              in
                R.Get_slice
                  { R.Get_slice.keyspace; table = $2;
                    max_keys = snd $3;
                    decode_timestamps = true;
                    max_columns; key_range; column_range; }) }
  | GET KEYS ID opt_range
      {
        with_ks
           (fun keyspace ->
              let key_range = match $4 with
                  Some (Range r, _) -> Key_range.Key_range r
                | Some (List l, _) -> Key_range.Keys l
                | None -> all_keys in
              let max_keys = match $4 with
                  None -> None
                | Some (_, x) -> x
              in
                R.Get_keys
                  { R.Get_keys.keyspace; table = $3; max_keys; key_range; }) }

put :
    PUT ID LBRACKET ID RBRACKET LBRACKET bindings RBRACKET
        { 
          with_ks
            (fun keyspace ->
               let columns = 
                 List.map
                   (fun (name, data) ->
                      { Column.name; data; timestamp = Timestamp.No_timestamp })
                   $7
               in R.Put_columns
                    { R.Put_columns.keyspace; table = $2; key = $4; columns; }) }

bindings :
    binding                  { [ $1 ] }
  | bindings COMMA binding   { $1 @ [ $3 ] }

binding: ID EQ ID            { ($1, $3) }

delete :
    DELETE ID LBRACKET ID RBRACKET LBRACKET id_list RBRACKET
      {
        with_ks
          (fun keyspace ->
             R.Delete_columns
               { R.Delete_columns.keyspace; table = $2;
                 key = $4; columns = $7; }) }
  | DELETE ID LBRACKET ID RBRACKET
      {
        with_ks
          (fun keyspace ->
             R.Delete_key { R.Delete_key.keyspace; table = $2; key = $4; }) }

directive : DIRECTIVE directive_params { Directive ($1, $2) }

directive_params :
    /* empty */         { [] }
  | directive_params ID { $1 @ [ $2 ] }

opt_range :
  | range        { Some $1 }
  | /* empty */  { None }

range : 
  | LBRACKET opt_id RANGE opt_id opt_cond RBRACKET
                 { (Range { Range.first = $2; up_to = $4; reverse = false; },
                         $5) }
  | LBRACKET opt_id REVRANGE opt_id opt_cond RBRACKET
                 { (Range { Range.first = $2; up_to = $4; reverse = true; },
                         $5) }
  | LBRACKET opt_cond RBRACKET
                 { (Range { Range.first = None; up_to = None; reverse = false; },
                         $2) }
  | LBRACKET id_list opt_cond RBRACKET
                 { (List $2, $3) }


opt_cond :
    /* empty */  { None }
  | COND INT     { Some $2 }

id_list :
    ID                { [$1] }
  | id_list COMMA ID  { $1 @ [$3] }

opt_id :
    /* empty */  { None }
  | ID           { Some $1 }
