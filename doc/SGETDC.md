
SGETDC
======

Get slice for a discrete key range and continuous column range.

Request arguments
-----------------
* keyspace handle
* table name
* max-keys (base-10 integer; nil argument if unlimited)
* max-columns (base-10 integer; nil argument if unlimited)
* discrete key range:
  * number of keys (N)
  * N keys
* continuous column range:
  * first column (inclusive if range not reverse, exclusive otherwise)
  * up_to column (exclusive if range not reverse, inclusive otherwise)
  * boolean indicating if the range is reverse

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

Get all columns for the keys `a` and `b` (returns: last key processed `b`, 
`{ b : { c1 : ["column value", nil] } }`.

    REQUEST                               RESPONSE
     
    @12345678                             @12345678    
    *11                                   *5
    $6                                    $1
    SGETDC                                b
    $2                                    $1    
    42                                    b    
    $7                                    $1    
    mytable                               1    
    $-1                                   $2    
    $-1                                   c1    
    $1                                    $12    
    2                                     column value    
    $1                                    $-1    
    a      
    $1
    b     
    $-1    
    $-1     
    $1    
    0      
