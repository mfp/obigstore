
You can back up a running obigstore database and get a consistent snapshot of
the database with minimal disruption to operations easily via `ob_repl` as
follows:

    DUMP LOCAL TO "/destination/dir"

If there is already data from a previous dump available in the destination
directory, only the differences since that dump will have to be retrieved in
order to recover an up-to-date snapshot of the database (the underlying
mechanism is the same one used in the [initial replication
phase](replication.html)). With commodity hardware (cheap spinning HDs and
gigabit ethernet), you can expect the dump to proceed at a rate around
100 MB/s.

Partial database dumping
------------------------
You can perform a partial DB dump of a set of tables (or all of them) within a
keyspace with `ob_dump`, and reload it with `ob_load`. Note that, unlike
`DUMP LOCAL`, `ob_dump` is not incremental and will always dump *all* the
requested data; it is also less efficient than `DUMP LOCAL` because the data
is decoded as it is dumped (so the speed is effectively determined by the
sequential scan speed, which will typically be below 500000 columns/s).
`DUMP LOCAL` is therefore preferrable in most cases.

<!-- vim: set ft=markdown: -->
