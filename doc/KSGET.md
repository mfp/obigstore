
KSGET
=====
Attempt to get a handle for the keyspace with the specified name.

Request arguments
-----------------

* keyspace name

Response
--------
Multi-argument response with 1 argument if the keyspace exists, or 0 if not.

Example
-------

    REQUEST                               RESPONSE
    
    @12345678                             @12345678    
    *2                                    *1    
    $5                                    $2    
    KSGET                                 42    
    $10                                 or
    mykeyspace                            @12345678
                                          *0
