
LOCK
====
Acquire locks with specified names. May block until the lock is available.
Exclusive locks can easily lead to deadlocks, so if possible use optimistic
concurrency control instead.

Request arguments
-----------------

* keyspace handle
* boolean indicating whether the locks are shared (non-exclusive): "0" or "1"
* N lock names

Response
--------
Single-line OK or error (errcode 501 on deadlock).

Example
-------

Acquire shared locks `lock1` and `lock2`.

    REQUEST                               RESPONSE
     
    @12345678                             @12345678    
    *5                                    +OK    
    $4                                    
    LOCK                              or  @12345678
    $2                                    -501
    42                                  
    $1    
    1    
    $5    
    lock1    
    $5    
    lock2    

