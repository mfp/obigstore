COMMIT
======
Commit transaction (NOP if not in a transaction).

Request arguments
-----------------

* keyspace handle

Response
--------
Single-line OK or dirty data error 502 if using optimistic
concurrency control.

Example
-------

Commit current transaction on handle 42:

    REQUEST                               RESPONSE
     
    @12345678                             @12345678    
    *2                                    +OK    
    $6                                    
    COMMIT                                or
    $2                                    
    42                                    @12345678    
                                          -502    
