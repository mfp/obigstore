TSIZE
=====
Return approximate size on disk of specified table.

Request arguments
-----------------

* keyspace handle
* table name

Response
--------
Size in bytes as single-line integer.

Example
-------

Get size of table `sometable` using keyspace handle 42 (returns: 435321
bytes).

    REQUEST                               RESPONSE
     
    @12345678                             @12345678    
    *3                                    :435321    
    $5                                    
    TSIZE                                
    $2                                  
    42                                 
    $9    
    sometable    
