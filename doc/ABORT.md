
ABORT
=====
Abort current transaction (NOP if not in a transaction).

Request arguments
-----------------

* keyspace handle

Response
--------
Single-line OK.

Example
-------

Abort current transaction on handle 42:

    REQUEST                               RESPONSE
     
    @12345678                             @12345678    
    *2                                    +OK    
    $5                                    
    ABORT                                
    $2                                    
    42                                  
