
SGETDD
======

Get slice for a discrete key range and discrete column range:

Request arguments
-----------------
* keyspace handle
* table name
* max-keys (base-10 integer; nil argument if unlimited)
* max-columns (base-10 integer; nil argument if unlimited)
* discrete key range:
  * number of keys (N)
  * N keys
* discrete column range:
  * number of columns (M)
  * M columns

Response
--------
Slice:

* last key processed (nil if none)
* for each key:
   * key
   * number of columns (base-10 int)
   * for each column:
      * column name
      * column data
      * column timestamp (base-10 int64) or nil (if uncommitted)

Example
-------

Get columns `a` and `b` for keys `k1` and `k2` in table `mytable` (returns:
last processed key `k1`, `{ k1: { b : ["column value", nil] } }`).

    REQUEST                               RESPONSE
    
    @12345678                             @12345678    
    *11                                   *6    
    $6                                    $2    
    SGETDD                                k1    
    $2                                    $2    
    42                                    k1    
    $7                                    $1    
    mytable                               1    
    $-1                                   $1    
    $-1                                   b    
    $1                                    $12    
    2                                     column value    
    $2                                    $-1
    k1       
    $2       
    k2    
    $1    
    2    
    $1    
    a    
    $1    
    b        


