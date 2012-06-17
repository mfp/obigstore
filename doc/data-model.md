
obigstore's data model resembles BitTable's, modulo minor terminology
differences.

A database holds one or more _keyspaces_ (these would be equivalent to databases
in RDBMS engines). Within a keyspace, data is organized in _tables_.
Each table holds a number of _columns_ associated to a _key_.

In other words, data within a keyspace is stored in a 3-dimensional associative
array indexed by the table, the key and the column name:

     datum = table[key][column]

Any arbitrary (non-zero) number of columns can be associated to a key;
different keys in the same table can have different columns.

#### Structured columns

Columns whose name begins with `@` hold BSON-encoded values by convention.
The REPL will automatically decode them and additional structure-aware
operations on them (such as indexing or querying) might be added at a later
point.

Transactions and concurrency control
------------------------------------
obigstore supports arbitrarily nestable transactions with two isolation
levels:

* repeatable read
* read committed

Both optimistic (CAS) and pessimistic (locks) concurrency control are available.

Physical layout
---------------
Given a key, its columns are stored on disk in lexicographic order.
Such groups are themselves stored in lexicographic key order.

For instance, if we have the following data

    users     uid1        name   J. Random
                          email  random@example.com

              uid2        name   Tófi Thorvald
                          email  thor@example.com

the physical layout will be like

    ...
    [users-uid1-email:random@example.com][users-uid1-name: J.Random]
    [users-uid2-email:thor@example.com][users-uid2-name: Tófi Thorvald]
    ...

Redundancy reduction
--------------------
Internal keys, which are conceptually 4-tuples `(keyspace, table, key, column)`,
are encoded in a way that allows effective prefix encoding
(so that for instance the `(keyspace, table, key)` prefix common to all the
columns for `key` is not stored repeatedly).

Moreover, data is saved in compressed blocks whose size can be specified with
`-blocksize N`, allowing for important size savings when there is significant
cross-record (key or column) redundancy.

<!-- vim: set ft=markdown: -->
