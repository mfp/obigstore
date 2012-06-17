
KEXIST
======
Return list of booleans indicating whether the specified keys exist in the
indicated tables.


Request arguments
-----------------

* keyspace handle
* table name
* N keys

Response
--------
N boolean arguments indicating whether each of the keys exist.

Example
-------

Verify existence of keys `a` and `b` in table `mytable`
(returns: `[false, true`]).

    REQUEST                               RESPONSE
     
    @12345678                             @12345678    
    *5                                    *2    
    $6                                    $1    
    KEXIST                                0    
    $2                                    $1    
    42                                    1    
    $7                                    
    mytable                               
    $1    
    a    
    $1                                    
    b    

