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
%token <int> INT
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
  | LISTEN ID { with_ks (fun keyspace -> R.Listen { R.Listen.keyspace; topic = $2 }) }
  | UNLISTEN ID
              { with_ks (fun keyspace ->
                           R.Unlisten { R.Unlisten.keyspace; topic = $2 }) }
  | NOTIFY ID
              { with_ks (fun keyspace ->
                           R.Notify { R.Notify.keyspace; topic = $2 }) }
  | AWAIT
              { with_ks (fun keyspace -> R.Await { R.Await.keyspace; }) }
  | DUMP      { with_ks (fun keyspace -> R.Trigger_raw_dump
                                           { R.Trigger_raw_dump.record = false }) }
  | DUMP LOCAL { Dump_local None }
  | DUMP LOCAL ID { Dump_local (Some $3) }
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
    GET ID range opt_multi_range opt_row_predicate opt_redir
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
  | GET KEYS ID opt_range opt_redir
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
                    { R.Put_columns.keyspace; table = $2; data = [$4, columns] }) }

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
    ID           { `Elm $1 }
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
      ID                { DM.Column_val ($1, DM.Any) }
  |   ID LT ID          { DM.Column_val ($1, DM.LT $3) }
  |   ID LE ID          { DM.Column_val ($1, DM.LE $3) }
  |   ID EQ ID          { DM.Column_val ($1, DM.EQ $3) }
  |   ID GE ID          { DM.Column_val ($1, DM.GE $3) }
  |   ID GT ID          { DM.Column_val ($1, DM.GT $3) }
  |   ID LT ID LT ID    { DM.Column_val ($3, DM.Between ($1, false, $5, false)) }
  |   ID LE ID LT ID    { DM.Column_val ($3, DM.Between ($1, true, $5, false)) }
  |   ID LT ID LE ID    { DM.Column_val ($3, DM.Between ($1, false, $5, true)) }
  |   ID LE ID LE ID    { DM.Column_val ($3, DM.Between ($1, true, $5, true)) }

opt_cond :
    /* empty */  { None }
  | COND INT     { Some $2 }

id_list :
    ID                { [$1] }
  | id_list COMMA ID  { $1 @ [$3] }

opt_id :
    /* empty */  { None }
  | ID           { Some $1 }

opt_redir :
    /* empty */  { None }
  | TO ID        { Some $2 }
