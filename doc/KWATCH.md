
KWATCH
======
Watch specified keys. If any of them is modified and the current
transaction tries to write/delete data, an error will be signalled on `COMMIT`.

Request arguments
-----------------

* keyspace handle
* table name
* N keys

Response
--------
Single-line OK.

Example
-------

Watch keys `k1` and `k2` in table `mytable`.

    REQUEST                               RESPONSE
     
    @12345678                             @12345678    
    *5                                    +OK    
    $6                                    
    CWATCH                              
    $2                               
    42                                  
    $7
    mytable
    $2    
    k1
    $2
    k2

