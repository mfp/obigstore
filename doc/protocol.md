
The obigstore server supports two protocols:

* a compact binary protocol that uses CRCs to detect transmission error (used
  by the REPL, load/dump tools and in the OCaml library)
* a simple textual protocol modelled after [redis' unified request
  protocol](http://redis.io/topics/protocol), documented below

Both protocols support out-of-order replies, allowing to perform multiple
requests on the same connection concurrently.

The client chooses which protocol to use (binary or textual) in the protocol
negotiation phase that takes place during the initial handshake.

Handshake
=========

In the initial handshake, the client authenticates itself and then chooses a
protocol amongst those supported by the server.


    CLIENT>  <role>\r\n
    SERVER>  <challenge>\r\n
    CLIENT>  <response>\r\n
    SERVER>  +OK\r\n               or    -ERR\r\n    and closes connection
    SERVER>  <latest bin proto version>\r\n
    SERVER>  <latest text proto version>\r\n
    CLIENT>  TXT\r\n<version>\r\n  or    BIN\r\n<version>\r\n
    SERVER>  <version>\r\n         or    -ERR       if cannot find matching protocol

`<role>` is the role requested by the client; `<challenge>` is a nonce, and
`<response>` is the result of performing `to_hex(HMAC-SHA1(password, challenge))`.
(As of 2012-06-12, the server defaults to promiscuous mode and accepts any response
to the challenge.)

Here's a sample handshake, where the client authenticates as 'guest' (password
'guest') and chooses the textual protocol:

    CLIENT> guest\r\n
    SERVER> d1dd48e26450c8537adb1ceedfda8dbc\r\n
    CLIENT> 9cbb024e6af614b8adc8c3a5a7299b3ab4c3f828\r\n
    SERVER> +OK\r\n
    SERVER> 0.0.1\r\n
    SERVER> 0.0.1\r\n
    CLIENT> TXT\r\n
    CLIENT> 0.0.1\r\n
    SERVER> 0.0.1\r\n

Frame format
============

Requests and responses are sent as frames of the form

    @<request id (8-byte)\r\n
    <payload>

The client must guarantee that the request id is unique (per-connection;
different connections have independent request id namespaces), i.e., not the
same as in any other request that hasn't been responded to yet.  If requests
are performed sequentially (one being sent after the response to the previous
is read), the same request id can be reused.

There are four kinds of payloads:

1. multi-argument
2. single-line error code
3. single-line OK response
4. single-line integer

Multi-argument payload
----------------------

All requests and most responses are of the first kind (multi-argument), which
is of the form:

    *<num args>\r\n
    $<length in bytes of arg 1 as base-10 string>\r\n
    <arg 1>\r\n
    $<length in bytes of arg 2 as base-10 string>\r\n
    <arg 2>\r\n

where first argument is the name of the command. Nil arguments are encoded as
having length -1; e.g., the sequence of arguments `["foo", nil, "bar"]` is
encoded as:

    $3\r\n
    foo\r\n
    $-1\r\n
    $3\r\n
    bar\r\n

Single-line payload
-------------------

Some responses may carry a single-line payload like

    +OK\r\n       success
    -501\r\n      error 501
    :12345\r\n    integer 12345

<!-- vim: set ft=markdown: -->
