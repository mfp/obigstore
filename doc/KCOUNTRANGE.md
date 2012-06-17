
KCOUNTRANGE
===========
Return number of keys in specified range.

Request arguments
-----------------

* keyspace handle
* table name
* range:
  * first key (inclusive if range not reverse, exclusive otherwise)
  * up_to key (exclusive if range not reverse, inclusive otherwise)
  * boolean indicating if the range is reverse

Response
--------
Single-line int with number of keys that exist.

Example
-------

Get number of keys in the range `[:bar]` (i.e. all up `bar`, excluded) in
`mytable` (returns: 1).

    REQUEST                               RESPONSE
     
    @12345678                             @12345678    
    *5                                    :1    
    $11                                   
    KCOUNTRANGE                           
    $7                                    
    mytable                               
    $-1    
    $3                                    
    bar    
    $1    
    0    

