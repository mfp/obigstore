

<h3>
obigstore is an open-source database that exposes a multidimensional
BigTable-like data model. It inherits Google LevelDB's fundamental strengths,
such as fast random writes or control over the physical data layout. It can be
used in a client/server setting or as an embedded database.
</h3>

<a href="doc.html">Read more →</a>

------------------

<div class="main-content row">

<div class="span4"</div>

<h4> strong data durability guarantees </h4>

<ul>
<li><strong>fully fsync'ed writes</strong> with group commit</li>
<li>data integrity ensured with <strong>CRCs at the protocol
level</strong></li>
<li>synchronous and asynchronous <strong>replication</strong></li>
<li><strong>online backup</strong></li>
</div>

<div class="span4"</div>

<h4>rich semi-structured data model</h4>
<ul>
<li><strong>atomic transactions</strong> (both read-committed and repeatable-read isolation levels)</li>
<li>optimistic and pessimistic <strong>concurrency control</strong></li>
<li>asynchronous notifications</li>
<li>limited support for complex documents (BSON serialized)</li>
<li>support for composite keys (REPL and client lib)</li>
</ul>
</div>

<div class="span4"</div>
<h4>performance</a></h4>
<ul>
<li>fast <strong>random writes</strong></li>
<li><strong>efficient range queries</strong> thanks to <strong>spatial
locality</strong></li>
<li>cross-record <strong>redundancy reduction</strong> at the page level (snappy
compression)</li>
<li>fast recovery (independent of dataset size)</li>
</ul>
<a href="benchmarks.html">Read more →</a>
</div>

</div>

<div class="row">
<div class="span6">
<a href="benchmarks.html"><img alt="1 billion writes on 1 EC2 node, sequential
no fsync" src="write-seq.png" /></a>
</div>
<div class="span6">
<a href="benchmarks.html"><img alt="1 billion writes on 1 EC2 node, random hotspot, fsync()ed" src="write-hotspot.png" /></a>
</div>
</div>

<!-- vim: set ft=markdown: -->
