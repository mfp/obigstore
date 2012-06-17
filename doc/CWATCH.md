
CWATCH
======
Watch specified columns. If any of them is modified and the current
transaction tries to write/delete data, an error will be signalled on `COMMIT`.

Request arguments
-----------------

* keyspace handle
* table name
* number of keys (base-10 int)
* for each key:
   * key
   * number of columns (N as base-10 int)
   * N column names

Response
--------
Single-line OK.

Example
-------

Watch columns `c1` and `c2` from key `k` in table `mytable`.

    REQUEST                               RESPONSE
     
    @12345678                             @12345678    
    *8                                    +OK    
    $6                                    
    CWATCH                              
    $2                               
    42                                  
    $7
    mytable
    $1    
    1    
    $1    
    k
    $1
    2
    $2
    c1
    $2
    c2

