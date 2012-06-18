
obigstore supports both synchronous and asynchronous replication. Any number
of slaves can be attached to a master, and slaves can themselves become the
masters of other nodes, allowing to create replication topologies.

Replication mechanism
---------------------

Replication works as follows:

1. the slave connects to the master and requests a database dump ("attaching")
2. once the database has been dumped to local storage, updates performed in
   the master are streamed to the slave ("streaming replication")

When the slave already contains a previous version of the database, (1) is
sped up considerably as the amount of data to be copied will be `O(m log n)`
(*not* `O(n)`) where `m` is the amount of changes since streaming replication
was interrupted and `n` the size of the database. Data is transferred in bulk
at a speed mainly determined by the sequential read/write speeds of the data
storage and the network link, so as a rule of thumb it can be considered
around 100 MB/s for commodity HDs and gigabit links.

In order to minimize the amount of data transferred during the initial
database dump, the slave should use the same configuration (`-block-size` and
`-write-buffer-size`) as the master. This way they will write exactly the same
data on disk and re-attaching will remain fast; otherwise, the amount of data
to be sent when re-attaching the slave would be proportional to the amount of
data written since it was attached for the last time (instead of `O(m log n)`
as documented above).

Asynchronous replication
------------------------

The master can choose whether to wait for the slave to indicate that the
update has been persisted to disk (synchronous replication) or not
(asynchronous, with `-await-recv`). It is possible to use different sync
settings (`-no-fsync`) in the server and the slave(s).

<!-- vim: set ft=markdown: -->
