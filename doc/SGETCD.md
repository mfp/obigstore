
SGETCD
======

Get slice for a continuous key range and discrete column range:

Request arguments
-----------------
* keyspace handle
* table name
* max-keys (base-10 integer; nil argument if unlimited)
* max-columns (base-10 integer; nil argument if unlimited)
* continuous key range:
  * first key (inclusive if range not reverse, exclusive otherwise)
  * up_to key (exclusive if range not reverse, inclusive otherwise)
  * boolean indicating if the range is reverse
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

Get columns `a` and `b` for all the keys in range `[:bar]` (up to `bar`,
excluded) in table `mytable` (returns: last key processed `babar`, 
` { babar: { b : ["column value", nil]} } `).

    REQUEST                               RESPONSE
     
    @12345678                             @12345678    
    *11                                   *6   
    $6                                    $5    
    SGETCD                                babar   
    $2                                    $5
    42                                    babar
    $7                                    $1    
    mytable                               1    
    $-1                                   $1    
    $-1                                   b    
    $-1                                   $12    
    $3                                    column value    
    bar                                   $-1
    $1                                    
    0                                     
    $1    
    2    
    $1    
    a    
    $1    
    b    

