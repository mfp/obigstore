    
SGETCC
======

Get slice for a continuous key range and continuous column range.

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

Get all columns for all the keys between `a` (included) and `b` (excluded)
(returns: last key processed `a`, `{a : { c1 : ["column value", nil] } }`).

    REQUEST                               RESPONSE
     
    @12345678                             @12345678    
    *11                                   *6    
    $6                                    $1
    SGETCC                                a
    $2                                    $1    
    42                                    a    
    $7                                    $1    
    mytable                               2    
    $-1                                   $2    
    $-1                                   c1    
    $1                                    $12    
    a                                     column value    
    $1                                    $-1    
    b      
    $1     
    0     
    $-1    
    $-1    
    $1     
    0      

