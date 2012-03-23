/*
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
 */

%{

open Obs_repl_common
open Obs_request
module R = Request
module DM = Obs_data_model

let all_keys =
  Key_range.Key_range { Range.first = None; up_to = None; reverse = false }

let col_range_of_multi_range = function
    [] -> Column_range.All_columns
  | l ->
      let rev_union =
        (List.fold_left
           (fun l x -> match l, x with
              | Simple_column_range.Columns cs :: tl, `Elm e ->
                  Simple_column_range.Columns (cs @ [e]) :: tl
              | l, `Range r -> Simple_column_range.Column_range r :: l
              | l, `Elm e -> Simple_column_range.Columns [e] :: l)
        [] l)
      in Column_range.Column_range_union (List.rev rev_union)
%}

%token <string> ID
%token <Big_int.big_int> INT
%token <string> DIRECTIVE
%token KEYSPACES TABLES KEYSPACE SIZE BEGIN ABORT COMMIT KEYS COUNT GET PUT DELETE
%token LOCK SHARED STATS LISTEN UNLISTEN NOTIFY AWAIT DUMP LOCAL TO
%token LBRACKET RBRACKET RANGE REVRANGE COND EQ COMMA EOF AND OR LT LE EQ GE GT

%start input
%type <Obs_repl_common.req> input

%%

input : phrase EOF { $1 }

phrase : /* empty */  { Nothing }
  | KEYSPACES { Command (R.List_keyspaces { R.List_keyspaces.prefix = "" }, None) }
  | TABLES    { with_ks
                  (fun keyspace ->
                     R.List_tables { R.List_tables.keyspace }) }
  | size      { $1 }
  | BEGIN     { with_ks (fun keyspace ->
                           R.Begin { R.Begin.keyspace;
                                     tx_type = Tx_type.Repeatable_read; }) }
  | COMMIT    { with_ks (fun keyspace -> R.Commit { R.Commit.keyspace }) }
  | ABORT     { with_ks (fun keyspace -> R.Abort { R.Abort.keyspace }) }
  | LOCK id_list
              { with_ks (fun keyspace ->
                           R.Lock { R.Lock.keyspace; names = $2; shared = false; }) }
  | LOCK SHARED id_list
              { with_ks (fun keyspace ->
                           R.Lock { R.Lock.keyspace; names = $3; shared = true; }) }
  | STATS     { with_ks (fun keyspace -> R.Stats { R.Stats.keyspace; }) }
  | LISTEN id { with_ks (fun keyspace -> R.Listen { R.Listen.keyspace; topic = $2 }) }
  | UNLISTEN id
              { with_ks (fun keyspace ->
                           R.Unlisten { R.Unlisten.keyspace; topic = $2 }) }
  | NOTIFY id
              { with_ks (fun keyspace ->
                           R.Notify { R.Notify.keyspace; topic = $2 }) }
  | AWAIT
              { with_ks (fun keyspace -> R.Await { R.Await.keyspace; }) }
  | DUMP      { with_ks (fun keyspace -> R.Trigger_raw_dump
                                           { R.Trigger_raw_dump.record = false }) }
  | DUMP LOCAL { Dump_local None }
  | DUMP LOCAL id { Dump_local (Some $3) }
  | DUMP LOCAL TO id { Dump_local (Some $4) }
  | count     { $1 }
  | get       { $1 }
  | put       { $1 }
  | delete    { $1 }
  | directive { $1 }

size :
    SIZE table
              { with_ks
                  (fun keyspace ->
                     R.Table_size_on_disk
                       { R.Table_size_on_disk.keyspace; table = $2; }) }
  | SIZE table range_no_max
              { with_ks
                  (fun keyspace ->
                     R.Key_range_size_on_disk
                       { R.Key_range_size_on_disk.keyspace;
                         table = $2; range = $3 }) }

range_no_max :
    LBRACKET opt_id RANGE opt_id RBRACKET
              { { Range.first = $2; up_to = $4; reverse = false; } }

count :
    COUNT table opt_range
              { with_ks
                  (fun keyspace ->
                     let key_range = match $3 with
                         None -> all_keys
                       | Some (Range r, _) -> Key_range.Key_range r
                       | Some (List l, _) -> Key_range.Keys l
                     in R.Count_keys { R.Count_keys.keyspace; table = $2;
                                          key_range; }) }

get :
    GET table range opt_multi_range opt_row_predicate opt_redir
       {
         with_ks_unwrap
           (fun keyspace ->
              let column_range, max_columns = match $4 with
                  None -> Column_range.All_columns, None
                | Some x -> x in
              let key_range = match fst $3 with
                  Range r -> Key_range.Key_range r
                | List l -> Key_range.Keys l in
              let req =
                R.Get_slice
                  { R.Get_slice.keyspace; table = $2;
                    max_keys = snd $3; decode_timestamps = true;
                    max_columns; key_range; predicate = $5;
                    column_range; }
              in Command (req, $6))
       }
  | GET KEYS table opt_range opt_redir
      {
        with_ks_unwrap
          (fun keyspace ->
             let key_range = match $4 with
                 Some (Range r, _) -> Key_range.Key_range r
               | Some (List l, _) -> Key_range.Keys l
               | None -> all_keys in
             let max_keys = match $4 with
                 None -> None
               | Some (_, x) -> x in
             let req =
               R.Get_keys
                 { R.Get_keys.keyspace; table = $3; max_keys; key_range; }
             in Command (req, $5))
      }

put :
    PUT table LBRACKET id RBRACKET LBRACKET bindings RBRACKET
        {
          with_ks
            (fun keyspace ->
               let columns =
                 List.map
                   (fun (name, data) ->
                      { Column.name; data; timestamp = Timestamp.No_timestamp })
                   $7
               in R.Put_columns
                    { R.Put_columns.keyspace; table = $2; data = [$4, columns] }) }

bindings :
    binding                  { [ $1 ] }
  | bindings COMMA binding   { $1 @ [ $3 ] }

binding: id EQ id            { ($1, $3) }

delete :
    DELETE table LBRACKET id RBRACKET LBRACKET id_list RBRACKET
      {
        with_ks
          (fun keyspace ->
             R.Delete_columns
               { R.Delete_columns.keyspace; table = $2;
                 key = $4; columns = $7; }) }
  | DELETE table LBRACKET id RBRACKET
      {
        with_ks
          (fun keyspace ->
             R.Delete_key { R.Delete_key.keyspace; table = $2; key = $4; }) }

directive : DIRECTIVE directive_params { Directive ($1, $2) }

directive_params :
    /* empty */         { [] }
  | directive_params id { $1 @ [ $2 ] }

opt_range :
  | range               { Some $1 }
  | /* empty */         { None }

opt_multi_range :
  | /* empty */   { None }
  | LBRACKET opt_cond RBRACKET
                  { Some (Column_range.All_columns, $2) }
  | LBRACKET multi_range opt_cond RBRACKET
                  { Some (col_range_of_multi_range $2, $3) }

multi_range :
    multi_range_elm
                 { [$1] }
  | multi_range COMMA multi_range_elm
                 { $1 @ [ $3] }

multi_range_elm :
    id           { `Elm $1 }
  | opt_id RANGE opt_id
                 { `Range { Range.first = $1; up_to = $3; reverse = false; } }
  | opt_id REVRANGE opt_id
                 { `Range { Range.first = $1; up_to = $3; reverse = true; } }

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

opt_row_predicate :
    /* empty */        { None }
  | COND row_predicate { Some (DM.Satisfy_any $2) }

row_predicate :
    and_predicate      { [ DM.Satisfy_all $1 ] }
  | row_predicate OR and_predicate
                       { $1 @ [ DM.Satisfy_all $3 ] }

and_predicate :
    simple_predicate                   { [ $1 ] }
  | and_predicate AND simple_predicate { $1 @ [ $3 ] }


simple_predicate :
      id                { DM.Column_val ($1, DM.Any) }
  |   id LT id          { DM.Column_val ($1, DM.LT $3) }
  |   id LE id          { DM.Column_val ($1, DM.LE $3) }
  |   id EQ id          { DM.Column_val ($1, DM.EQ $3) }
  |   id GE id          { DM.Column_val ($1, DM.GE $3) }
  |   id GT id          { DM.Column_val ($1, DM.GT $3) }
  |   id LT id LT id    { DM.Column_val ($3, DM.Between ($1, false, $5, false)) }
  |   id LE id LT id    { DM.Column_val ($3, DM.Between ($1, true, $5, false)) }
  |   id LT id LE id    { DM.Column_val ($3, DM.Between ($1, false, $5, true)) }
  |   id LE id LE id    { DM.Column_val ($3, DM.Between ($1, true, $5, true)) }

opt_cond :
    /* empty */  { None }
  | COND INT     { Some (Big_int.int_of_big_int $2) }

id_list :
    id                { [$1] }
  | id_list COMMA id  { $1 @ [$3] }

opt_id :
    /* empty */  { None }
  | id           { Some $1 }

opt_redir :
    /* empty */  { None }
  | TO id        { Some $2 }

id :
    ID           { $1 }
  | INT          { Big_int.string_of_big_int $1 }

table:
    ID           { DM.table_of_string $1 }
