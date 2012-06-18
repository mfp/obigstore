
The [simplified text-based protocol](protocol.html) is easy to implement and debug.
If there is a redis client available for the language you're targetting, most
of the code relative to frame reading/writing can be reused, as the only
difference is that in obigstore's protocol frames are prefixed by a request
id.

It is not necessary to expose all of obigstore's functionality at once, and
indeed it is easier to start with basic operations among [all those
supported](operations) and add features one step at a time. These are some of
the simplifications that can be made at the beginning:

* synchronous requests: obigstore supports concurrent, asynchronous requests,
  but the client needs not actually generate them. A basic client can send a
  requests synchronously, one at a time, and can therefore reuse a single same
  request id.
* one keyspace handle per connection: the protocol allows to multiplex
  requests for multiple keyspace handles in a single connection (in
  particular, this allows to perform concurrent transactions in the same
  keyspace with a single connection). A basic client can acquire a single
  keyspace handle per connection and perform all operations on it.

Combined, these two simplications make transactions much easier to implement.

<!-- vim: set ft=markdown: -->
