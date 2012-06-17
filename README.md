
obigstore: multi-dimensional DB with BigTable-like data model atop LevelDB
==========================================================================
Copyright (C) 2012 Mauricio Fernandez <mfp@acm.org>

obigstore is a database server + client library and associated tools. It
exposes a multidimensional BigTable-like data model built on top of the Google
LevelDB library, inheriting its fundamental strengths, such as fast random
writes or control over the physical data layout.  It can be used in a
client/server setting or as an embedded database.

obigstore's salient features include:

* strong data durability guarantees:
  * **fully fsync'ed writes** with group commit
  * data integrity ensured with **CRCs at the protocol level**
  * **synchronous and asynchronous replication**
  * **online backup**
* rich semi-structured data model:
  * **atomic transactions** (both read-committed and repeatable-read isolation levels)
  * optimistic and pessimistic **concurrency control**
  * asynchronous notifications
  * limited support for complex documents (BSON serialized)
  * support for composite keys (REPL and client lib)
* performance:
  * **fast random writes**
  * **efficient range queries** thanks to **spatial locality**
  * cross-record **redundancy reduction** at the page level (snappy compression)
  * fast recovery (independent of dataset size)

obigstore currently includes:

* the standalone database server
* the embeddable database library
* the client library
* a friendly REPL for interactive data manipulation
* DB dump/restore tools
* a number of benchmarking tools

Limitations
-----------
* transactions must fit in memory
* sharding on key ranges is not built-in at this point
* no automatic failover yet

Requirements
------------
(Debian package names parenthesized)

* OCaml >= 3.12.0 (ocaml-nox)
* omake to build (omake)
* snappy (libsnappy-dev)
* GCC with C++ frontend (g++)
* OCaml Batteries (libbatteries-ocaml-dev)
* Cryptokit (libcryptokit-ocaml-dev)
* Lwt (liblwt-ocaml-dev)
* oUnit for the unit tests  (libounit-ocaml-dev)
* extlib (libextlib-ocaml-dev)

obigstore's tree includes a number of git submodules that will be
automatically fetched if you run  git clone  with the  --recursive  option,
including:

* extprot
* ocaml-leveldb
* leveldb

Building
--------
Just 

    $ omake    # -j 4  for instance to parallelize the build

should do. This will build the server, client programs and libraries.
You can run the tests with

    $ omake test

You can then install at once obigstore, extprot and ocaml-leveldb with:

    $ omake install

The generated executables are:

* obigstore: the DB server
* ob_repl: the REPL
* ob_dump: tool to dump a DB to a file
* ob_load: tool to load ob_dump's output into a DB

They are standalone (modulo the snappy and tcmalloc dependencies) and can be
used simply by placing them somewhere in the PATH.
