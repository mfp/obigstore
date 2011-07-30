#!/bin/sh

CAML_LD_LIBRARY_PATH=../src/libs/ocaml-leveldb/src/ \
    rlwrap ocamldebug \
    `ocamlfind query -recursive -i-format batteries oUnit lwt` \
    -I ../src ./test.run
