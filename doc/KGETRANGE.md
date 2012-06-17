
KGETRANGE
=========
Return keys in specified range.

Request arguments
-----------------

* keyspace handle
* table name
* max-keys (base-10 integer; nil argument if unlimited)
* range:
  * first key (inclusive if range not reverse, exclusive otherwise; nil if
    unspecified)
  * up_to key (exclusive if range not reverse, inclusive otherwise; nil if
    unspecified)
  * boolean indicating if the range is reverse

Response
--------
M arguments with the existing keys

Example
-------

Get keys in the range `[:bar]` (i.e. all up `bar`, excluded) in `mytable`
(returns: [`b`]):

    REQUEST                               RESPONSE
     
    @12345678                             @12345678    
    *6                                    *1    
    $4                                    $1    
    KGETRANGE                             b    
    $7                                    
    mytable                               
    $-1    
    $-1    
    $3                                    
    bar    
    $1    
    0    

