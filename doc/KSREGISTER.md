KSREGISTER
==========

Registers a keyspace with the given name (NOP if already existent) and returns
a handle to it.

Request arguments
-----------------

* keyspace name

Response
--------
Single-line integer handler identifier.

Example
-------

    REQUEST                               RESPONSE
    
    @12345678                             @12345678    
    *2                                    :42    
    $10                                   
    KSREGISTER    
    $10    
    mykeyspace    

