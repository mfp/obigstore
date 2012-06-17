
RSIZE
=====
Return approximate size on disk of key range in specified table.

Request arguments
-----------------

* keyspace handle
* table name
* first key in range (nil argument if unspecified)
* last key in range (nil argument if unspecified)

Response
--------
Size in bytes as single-line integer.

Example
-------

Get size of range `[:bar]` (all keys up to "bar") in table `sometable` using
keyspace handle 42 (returns: 4351 bytes).

    REQUEST                               RESPONSE
     
    @12345678                             @12345678    
    *5                                    :4351    
    $5                                    
    RSIZE                                
    $2                                  
    42                                 
    $9    
    sometable    
    $-1    
    $3    
    bar    
