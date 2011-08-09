#!/bin/sh

CAML_LD_LIBRARY_PATH=../src/libs/ocaml-leveldb/src/:../src \
    rlwrap ./toplevel \
    `ocamlfind query -recursive -i-format batteries oUnit lwt` \
    -I ../src
