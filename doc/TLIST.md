
TLIST
=====
List tables for the given keyspace handle.

Request arguments
-----------------

* keyspace handle

Response
--------
Multi-argument response with 1 argument per existing table, carrying its
name.

Example
-------

List tables in keyspace with handle `42` (returns: `sometable`, `someother`).

    REQUEST                               RESPONSE
    
    @12345678                             @12345678    
    *2                                    *2    
    $5                                    $9    
    TLIST                                 sometable    
    $2                                    $9    
    42                                    someother    
