#!/bin/sh

CAML_LD_LIBRARY_PATH=../src/libs/ocaml-leveldb/src/:../src \
    rlwrap ocamldebug \
    `ocamlfind query -recursive -i-format \
       batteries oUnit lwt lwt.unix lwt.preemptive threads` \
    -I ../src ./test.run
