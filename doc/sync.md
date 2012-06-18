
By default, obigstore ensures that each update has been pushed all the way to
persistent storage before acknowledging it to the client (on POSIX systems,
this means calling fsync()/fdatasync() or similar). This ensures that there is
no data loss in case of system crash as long as the storage survives.

obigstore amortizes the cost of fsync() over several updates by performing
group commits of concurrent updates. This way, given enough concurrency, write
rates exceeding the disk seek rate by up to 3 orders of magnitude can be
reached. So as to further support this, obigstore allows to perform several
concurrent operations on a single connection.

It is possible to instruct obigstore to perform asynchronous writes (i.e.,
disable fsync()ing) by using the `-no-fsync` option. This can lead to data
loss (data buffered by the OS by not pushed to disk yet) in case of system
crash, but not to data corruption.

<!-- vim: set ft=markdown: -->
