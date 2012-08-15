BEGIN
=====
Begin transaction. A transaction is associated to a keyspace handle.
Transactions in different keyspace handles are independent.
Transactions can be nested arbitrarily.
Nested transactions can be executed concurrently, but the server may choose to
serialize their execution.

Request arguments
-----------------

* keyspace handle
* isolation level: "RR" for repeatable read, "RC" for read-committed

Response
--------
Single-line integer transaction handler identifier.

Example
-------

Begin repeatable read transaction:

    REQUEST                               RESPONSE
     
    @12345678                             @12345678    
    *3                                    +OK    
    $5                                    
    BEGIN                                
    $2                                  
    42                                 
    $2    
    RR    
