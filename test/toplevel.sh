#!/bin/sh

CAML_LD_LIBRARY_PATH=../src/libs/ocaml-leveldb/src/:../src/core \
    rlwrap ./toplevel \
    `ocamlfind query -recursive -i-format batteries oUnit lwt` \
    -I ../src/core -I ../src/client -I ../src/server -I ../src/server/util
