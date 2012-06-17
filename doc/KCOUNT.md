
KCOUNT
======
Count existing keys amongst those given in the request.

Request arguments
-----------------

* keyspace handle
* table name
* N keys

Response
--------
Single-line int with number of keys that exist.

Example
-------

Get how many keys in the set `{a, b}` exist in table `mytable` (returns: 1).

    REQUEST                               RESPONSE
    @12345678                             @12345678    
    *4                                    :1    
    $6                                    
    KCOUNT                                
    $7                                    
    mytable                               
    $1    
    a    
    $1                                    
    b    

