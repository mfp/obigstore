
KGET
====
Return existing keys amongst those given in the request.

Request arguments
-----------------

* keyspace handle
* table name
* N keys

Response
--------
M arguments with the existing keys

Example
-------

Get existing keys in the set `{a, b}` from table `mytable` (returns: [`b`]).

    REQUEST                               RESPONSE
     
    @12345678                             @12345678    
    *4                                    *1    
    $4                                    $1    
    KGET                                  b    
    $7                                    
    mytable                               
    $1    
    a    
    $1                                    
    b    
