
Introduction
------------

obigstore can be useful if some of these hold:

* you have a moderate amount of data (say, in the TB range).
* you don't want to be forced to compromise on data durability.
* you don't want to have to use sharding and to bother managing clusters once
  the amount of data exceeds available memory,
* yet you don't need infinite horizontal scalability out of the box.
* you don't need the relational model exposed by RDBMS,
* yet your data maps better to a model more structured than basic key-value,
  you don't want to handle eventual consistency and can make good use of ACID
  transactions in your applications.
* you need range queries.
* you need good random write performance.
* you need good performance with cold data through physical layout control and can
  benefit from cross-record redundancy reduction.
* you need not perform huge transactions that don't fit in memory

obigstore features a semi-structured BigTable-like [data model](data-model.html).
For more information on the operations it supports, you can refer to the [data
model in the OCaml
library](https://github.com/mfp/obigstore/blob/master/src/core/obs_data_model.mli)
and the [list of operations supported by the simplified text-based
protocol](operations.html) (the latter is a subset of the former).

Getting started
---------------

The [getting started guide](getting-started.html) indicates how to get the
source, build the server, run it and use the REPL.

API
---

obigstore is implemented in [OCaml](http://www.ocaml-lang.org/) and includes
an [OCaml API](https://github.com/mfp/obigstore/blob/master/src/core/obs_data_model.mli)
that exposes the full data model. Client code written in other languages can
communicate to the server using a simple text-based protocol that resembles
redis' unified request protocol.

<span class="label label-info">Help wanted</span>
Contributions in the form of bindings for other languages are very welcome.
I will try to provide support in the form of documentation and implementation
guides.

#### OCaml API

Refer to the .mli files. A client will have to do something like:

    module C = Obs_protocol_client.Make(Obs_protocol_bin.Version_0_0_0)

    ...
    let addr = Unix.ADDR_INET (Unix.addr_of_string server, port) in
    let data_address = Unix.ADDR_INET (Unix.addr_of_string server, port + 1) in
    lwt ich, och = Lwt_io.open_connection addr in
    lwt db = C.make ~data_address ich och ~role ~password in
      ...
      perform operations on db as documented in Obs_data_model.S
      ....

#### Simplified text-based protocol

* [simplified obigstore textual protocol specification](protocol.html)
* [operations](operations.html): operations exposed in the text-based
  protocol
* [implementation guide](binding-impl-guide.html): resources for developers
  interested in using obigstore via the text-based protocol from other
  languages

Durability
----------
* [single-node durability](sync.html): write ACK after fsync() with group
  commits
* [replication](replication.html): how replication works
* [online backup](backup.html) with minimal disruption to operation

Benchmarks
----------

You can find a [number of benchmarks here](benchmarks.html).

<!-- vim: set ft=markdown: -->
