
Getting the source code
-----------------------

The easiest way to get obigstore and the some of the libraries it requires is
to clone the git repository along with the associated submodules:

    git clone --recursive git://github.com/mfp/obigstore.git


Building
--------

obigstore also requires a working C++ environment and other programs and
libraries that can be found in package form (Debian package names
parenthesized):

* OCaml >= 3.12.0 (ocaml-nox)
* findlib (ocaml-findlib)
* omake to build (omake)
* snappy (libsnappy-dev)
* GCC with C++ frontend (g++)
* OCaml Batteries (libbatteries-ocaml-dev)
* Cryptokit (libcryptokit-ocaml-dev)
* Lwt (liblwt-ocaml-dev)
* oUnit for the unit tests  (libounit-ocaml-dev)
* extlib (libextlib-ocaml-dev)

If the above OCaml libraries are not available in packaged form for your
system, you can install them with [odb](https://github.com/thelema/odb) by
doing

    ocaml odb.ml batteries cryptokit extlib lwt ounit

Once you have the required dependencies, you can build everything at once with

    omake -j 4   # parallelize build

Launching the server
--------------------
The server only needs the path to the directory that will hold the database;
the remaining (optional) configuration can be supplied with command-line
arguments (run `obigstore -help` for more info).

    ./obigstore /tmp/test-db

Using the REPL
--------------
You can connect and create a keyspace at once with:

    ./ob_repl -keyspace test

If you prefer readline's line editing and have  `rlwrap` installed, you can
run instead:

    rlwrap ./ob_repl -keyspace test -simple

`ob_repl` exposes essentially the whole data model, allowing you to write and
delete columns, perform (predicated) range queries, transactions, etc. (enter
.help for a quick reference).
Refer to the [API documentation](doc.html) in order to use obigstore
programmatically.

    ob_repl
    Enter ".help" for instructions.
    Switching to keyspace "test"
    "test" (0) > put mytable[foo][a:1, b:"some stuff"]
    wrote 1 keys, wrote 2 columns in  0.00434s (cpu  0.00000s)
    wrote 460 columns/second
    "test" (0) > get mytable[foo][a]
    ------------------------------------------------------------------------------
    {
     foo:
       { a: 1 }
    }
    ------------------------------------------------------------------------------
    Last key: foo
    read 1 keys, read 1 columns in  0.00031s (cpu  0.00000s)
    read 3236 columns/second
    "test" (0) > get mytable[foo]
    ------------------------------------------------------------------------------
    {
     foo:
       { a: 1, b: "some stuff" }
    }
    ------------------------------------------------------------------------------
    Last key: foo
    read 1 keys, read 2 columns in  0.00026s (cpu  0.00000s)
    read 7550 columns/second
    "test" (0) > put mytable[bar][a:0, b:"whatever"]
    wrote 1 keys, wrote 2 columns in  0.00454s (cpu  0.00000s)
    wrote 440 columns/second
    "test" (0) > get mytable[][b] / a = 1
    ------------------------------------------------------------------------------
    {
     foo:
       { b: "some stuff" }
    }
    ------------------------------------------------------------------------------
    Last key: foo
    read 1 keys, read 1 columns in  0.00036s (cpu  0.00000s)
    read 2746 columns/second

