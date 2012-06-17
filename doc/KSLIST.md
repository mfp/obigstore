
KSLIST
======
List available keyspaces.

Request arguments
-----------------

none

Response
--------
Multi-argument response with 1 argument per existing keyspace, carrying its
name.

Example
-------

    REQUEST                               RESPONSE
    
    @12345678                             @12345678    
    *1                                    *2    
    $6                                    $10    
    KSLIST                                mykeyspace    
                                          $13    
                                          otherkeyspace    
