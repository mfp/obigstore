
CDEL
====

Delete columns for specified key.

Request arguments
-----------------
* keyspace handle
* table name
* key
* N column names

Response
--------
Single-line OK.

Example
-------

Delete columns `c1` and `c2` from key `k1` in table `mytable`:

    REQUEST                               RESPONSE
     
    @12345678                             @12345678    
    *6                                    +OK
    $4                                    
    CDEL                                 
    $2
    42
    $7                                  
    mytable                            
    $2
    k1
    $2
    c1
    $2
    c2
