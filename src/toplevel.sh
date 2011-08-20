#!/bin/sh

CAML_LD_LIBRARY_PATH=libs/ocaml-leveldb/src:. \
  rlwrap ./toplevel \
    -I libs/ocaml-leveldb/src -I libs/extprot/runtime
