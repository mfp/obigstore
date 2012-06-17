
SPUT
====

Write data to the specified table.

Request arguments
-----------------
* keyspace handle
* table name
* number of keys (base-10 integer)
* for each key:
  * key name
  * number of columns (base-10 integer)
  * for each column:
     * name
     * data
     * timestamp: base-10 int64 or nil

Response
--------
Single-line OK.

Example
-------

Write ` { k1 : { c1 : x, c2 : y }, k2 : { c3 : z } ` to table `mytable`.

    REQUEST                               RESPONSE
    @12345678                             @12345678    
    *16                                   +OK
    $4                                    
    SPUT                                 
    $2
    42
    $7                                  
    mytable                            
    $1
    2                                
    $2                              
    k1                             
    $1                            
    2                                     
    $2                                   
    c1
    $1
    x
    $-1
    $2
    c2
    y
    $-1
    $2
    k2
    $1
    1
    $2
    c3
    $1
    z
    $-1

