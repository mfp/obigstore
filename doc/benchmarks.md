

The following benchmarks are meant to give a ball park approximation of
the performance to be expected out of the box, without special tuning, on
commodity hardware.

Test conditions
---------------
Except where indicated, the benchmarks are run on an EC2 m1.large instance
with 7.5GB memory and two cores, running Ubuntu 12.04. The database is placed
on ephemeral storage, and the original filesystem is used as-is with no extra
tuning (this means EXT3 without any extra mount option).  In practice, better
performance can be expected given some tuning and for instance a RAID10 setup
(the tests below are largely disk-bound).

A separate instance is used as the load generator.

Some statistics relative to the disk and overall performance of the instance
(redis random reads and writes) can be found at the end of this page.

obigstore is run with the following options:

* `-write-buffer-size 16_000_000`: set the buffer size to ~16MB
* `-max-open-files 40000` to allow the benchmark to run (LevelDB's
  documentation states that about around 1 file per 2MB is required).

Random hotspot writes
---------------------
In this benchmark, 2^13 "hotspots" are chosen randomly amongst all 2^128
16-byte keys. Writes are performed in these hotspots as follows:

* with probability 50%, a new key is added semi-sequentially
* with probability 50%, a random key from this hotspot is overwritten.

The payload is a random 32-byte string with a ~50% redundancy.
Up to 64 concurrent operations are allowed; each performs 20 writes.

The load is generated and recorded with

    ./bm_makedata -hotspot 0.99999:13 1_000_000_000 | ./bm_write -latency -server SERVER_IP -concurrency 64 -multi 20 

The above models quite closely scenarios such as these:

* mostly append-only writes to mailboxes, message queues, etc.
* secondary indexes with non-uniform distribution or limited cardinality

#### Fsync'ed writes

By default, obigstore performs group commits and always calls fsync() before
acknowledging each individual write to the client.

The dispersion observable in the insertion rate is due to obigstore's
throttling mechanism, which limits the insertion rate to avoid write stalls.
It would be possible to change the throttling algorithm to keep the rate
as constant as desired (i.e., disallowing bursts) as long as it is below the
sustainable one, as shown in another benchmark below.

![random hotspot](imgs/LOG.1000M.hotspot.rate.png)

The latencies are relatively high due to the large sustained insertion rate
and concurrency factor:

![random hotspot latency](imgs/LOG.1000M.hotspot.latency.png)

#### Non-fsync()'ed writes

It is possible to run obigstore in `-no-fsync` mode (which can cause the
loss of up to `write-buffer-size` data in the worst case, but not data
corruption since LevelDB always fsyncs when it moves to a new file). This
allows to assess the cost of fsync()ing:

![random hotspot no fsync](imgs/LOG.1000M.hotspot2.rate.png)

Sequential insertion
--------------------
In this benchmark, 200 million sequential 16-byte keys with random 32-byte
payloads (50% redundancy) are written. The load is generated with

    ./bm_write -period 5.0 -latency -server SERVER_IP -concurrency 200 -multi 16 -seq 1_000_000_000

This approaches the following scenarios:

* time-series data
* insertion of records using time-based UUIDs

#### Fsync'ed writes

![sequential insertion rate](imgs/LOG.S200M.rate.png)

With 3200 concurrent writes on the wire at any point in time, the best
worst-case latency we can aim for is `3200 / 1.4e3 =~ 22ms` in addition to the
fsync() delay:

![sequential insertion latency](imgs/LOG.S200M.latency.png)

#### Non-fsync'ed writes

![sequential insertion rate, no fsync](imgs/LOG.S1000M.rate.png)

![sequential insertion latency, no fsync](imgs/LOG.S1000M.latency.png)

#### Large payloads

The benchmark is repeated with 20M pairs having 20KB payloads; the rate is
limited to 1000/s:

![large payloads](imgs/LOG.S20K-nofsync.latency.png)

Random uniform insertion
------------------------
This is the most stringent test as it maximizes the amount of work performed
by the compactor; in most real scenarios, the actual load will be easier to
handle thanks to factors such as:

* updates: they generate less compactor work as data is coalesced
* non-uniform distribution
* semi-sequential insertion with time-based UUIDs
* temporal locality in updates to a given region (approaching the "hotspot"
  scenario shown above)

Several benchmarks are performed:

1. 1 billion random, uniformly distributed 16-byte keys with 32-byte random
   (50% redundancy) payloads are inserted
2. after letting the compactor work until done, an extra 100 million pairs
   are inserted to measure the write burst rate on the 1-billion key dataset
3. an extra 10 million pairs are inserted at a rate below the sustainable one
in order to measure latency

The first benchmark completes in 36825 seconds:

![random insertion](imgs/LOG.1000M.rate.png)

The final size of the database is 42 GB.
The total work performed by the compactor is

                                       Compactions
        Level  Files Size(MB) Time(sec) Read(MB) Write(MB)
        --------------------------------------------------
          0        0        0      1442        0     40365
          1        4        8      4224    55168     54867
          2      116       98      6909   144270    143906
          3      647      998     15284   217853    217197
          4     5112     9999      9811   128297    127939
          5    14070    27582         0        0         0

which amounts to around 15MB read and 16MB written per second.

The CPU usage was

        User time (seconds): 22117.70
        System time (seconds): 4957.57

which represents a ~75% CPU usage --- the task is clearly IO-bound. Note
that obigstore uses three threads when writing: one for request handling, one
to perform the actual write and another for compaction.

![random 100M insertion](imgs/LOG.1000M-100M.rate.png)

Latency measurements are performed with a concurrency level of 50 (2-writes
per request), and a rate approaching 15000 writes/s:

![latency in 1 billion pair dataset (rate)](imgs/LOG.1000M-10M-r10k-b.rate.png)

![latency in 1 billion pair dataset](imgs/LOG.1000M-10M-r10k-b.latency.png)

Reads
=====

Range reads
-----------
1e8 pairs with 16-byte sequential keys and random 32-byte (50% redundancy) are
inserted in a database. 512-key ranges are requested with `bm_read -range`.
Ranges are read at a speed exceeding

     295000 columns/s

Random reads
------------
1e9 pairs with 16-byte sequential keys and random 32-byte (50% redundancy) are
inserted in a database. bm\_read performs lookups for keys distributed randomly
in ranges of increasing size, using up to 5 concurrent requests each of them
performing 20 lookups (this corresponds to client-side joins). obigstore is
run with the `-assume-page-fault` option, which instructs it to run each read
request in a separate thread so as to avoid blocking reads.

The lookup rate as a function of the working set size (in keys) is shown below:

![random read vs working set size](imgs/read.png)

Three terms contribute to the lookup time:

1. request processing overhead
2. data retrieval from FS buffers or disk
3. block decompression

(3) is determined by the block size (`-block-size`), as larger blocks entail
higher latency. LevelDB (and thus obigstore) uses snappy to compress
data block-wise, and decompression speeds typically exceed several hundred
MB/s. The decrease in speed shown in the above graph between 100000 and 1e6
keys is a consequence of the working set no longer fitting in LevelDB's block
cache (set to 8MB by default and unchanged).

The lookup rate falls abruptly when the working set no longer fits in memory,
at which point it quickly becomes seek-bound and (2) dominates.

### Machine stats for reference

#### Disk
    
    ubuntu@domU-12-31-39-15-2C-66:/mnt/bunch-of-2MB-files$ cat * | pipebench > /dev/null
    Summary:                                                                       
    Piped    6.05 GB in 00h04m00.49s:   25.79 MB/second

    ubuntu@domU-12-31-39-15-2C-66:~$ bonnie++ -d /mnt/bonnietmp/ | tee bonnie.log
    [...]
    Version  1.96       ------Sequential Output------ --Sequential Input- --Random-
    Concurrency   1     -Per Chr- --Block-- -Rewrite- -Per Chr- --Block-- --Seeks--
    Machine        Size K/sec %CP K/sec %CP K/sec %CP K/sec %CP K/sec %CP  /sec %CP
    domU-12-31-39-1 15G   261  98 28939   8 38231  10   790  97 100838  13 297.9  14
    Latency             87759us     601ms    2193ms   67826us   62013us     707ms
    Version  1.96       ------Sequential Create------ --------Random Create--------
    domU-12-31-39-15-2C -Create-- --Read--- -Delete-- -Create-- --Read--- -Delete--
                  files  /sec %CP  /sec %CP  /sec %CP  /sec %CP  /sec %CP  /sec %CP
                     16 24868  43 +++++ +++ +++++ +++ +++++ +++ +++++ +++ +++++ +++
    Latency             14368us     468us   60050us   60496us      11us   60079us

#### Redis performance

For the sake of concision, the latency data was summarized to show only the
median, 90th, 95th and 99th percentiles.

##### Writes with AOF, `appendfsync always`

        ubuntu@ip-10-108-183-105:~/redis-2.4.14/src$ ./redis-benchmark -n 1000000 -c 200 -P 16 -h 10.207.47.148 -t set -d 32 -r 100000000
        ====== SET ======
          1000000 requests completed in 13.21 seconds
          200 parallel clients
          32 bytes payload
          keep alive: 1
        
        49.61% <= 39 milliseconds
        89.64% <= 59 milliseconds
        95.41% <= 68 milliseconds
        98.94% <= 112 milliseconds
        75711.69 requests per second


##### Writes with AOF `appendonly yes` 

        ubuntu@ip-10-108-183-105:~/redis-2.4.14/src$ ./redis-benchmark -n 10000000 -c 200 -P 16 -h 10.207.47.148 -t set -d 32 -r 100000000
        ====== SET ======
          10000000 requests completed in 90.38 seconds
          200 parallel clients
          32 bytes payload
          keep alive: 1
         
        49.84% <= 19 milliseconds
        54.97% <= 20 milliseconds
        90.52% <= 78 milliseconds
        95.24% <= 84 milliseconds
        99.00% <= 107 milliseconds
        110640.27 requests per second

##### Writes with AOF `appendonly no`  (no persistence, mem only)

        ubuntu@ip-10-108-183-105:~/redis-2.4.14/src$ ./redis-benchmark -n 10000000 -c 200 -P 16 -h 10.207.47.148 -t set -d 32 -r 100000000
        ====== SET ======
          10000000 requests completed in 74.15 seconds
          200 parallel clients
          32 bytes payload
          keep alive: 1
             
        47.09% <= 15 milliseconds
        57.11% <= 16 milliseconds
        90.00% <= 42 milliseconds
        95.26% <= 77 milliseconds
        99.06% <= 89 milliseconds
        134867.22 requests per second

##### Read performance

        ubuntu@ip-10-108-183-105:~/redis-2.4.14/src$ ./redis-benchmark -n 1000000 -c 200 -P 16 -h 10.207.47.148 -t get -d 32 -r 100000000
        ====== GET ======
          1000000 requests completed in 5.14 seconds
          200 parallel clients
          32 bytes payload
          keep alive: 1

        56.65% <= 12 milliseconds
        92.10% <= 18 milliseconds
        95.81% <= 71 milliseconds
        99.19% <= 76 milliseconds
        194741.95 requests per second

<!-- vim: set ft=markdown: -->
