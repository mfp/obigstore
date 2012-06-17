

You can find below the operations available in the [simplified text-based
protocol](protocol.html).  Refer to the [`RAW_S` module type in
obs\_data\_model.mli](https://github.com/mfp/obigstore/blob/master/src/core/obs_data_model.mli)
for further information on the semantics of each operation and for extra
operations not exposed via the simplified protocol.

------

<div class="row">

<div class="span3">
<h4>Keyspace and table operations</h4>
<dl>
<dt><a href="KSREGISTER.html">KSREGISTER</a></dt>
<dd>Register keyspace</dd>
<dt><a href="KSGET.html">KSGET</a></dt>
<dd>Get existent keyspace</dd>
<dt><a href="KSLIST.html">KSLIST</a></dt>
<dd>List existent keyspaces</dd>
<dt><a href="TLIST.html">TLIST</a></dt>
<dd>List tables in keyspace</dd>
<dt><a href="TSIZE.html">TSIZE</a></dt>
<dd>Return table size on disk</dd>
</dl>
</div>

<div class="span3">
<h4>Transactions</h4>
<dl>
<dt><a href="BEGIN.html">BEGIN</a></dt>
<dd>Begin transaction</dd>
<dt><a href="ABORT.html">ABORT</a></dt>
<dd>Abort transaction</dd>
<dt><a href="COMMIT.html">COMMIT</a></dt>
<dd>Commit transaction</dd>
<dt><a href="CWATCH.html">CWATCH</a></dt>
<dd>Watch for changes in columns</dd>
<dt><a href="KWATCH.html">KWATCH</a></dt>
<dd>Watch for changes under key</dd>
<dt><a href="LOCK.html">LOCK</a></dt>
<dd>Acquire lock</dd>
</dl>
</div>

<div class="span3">
<h4>Read operations</h4>

<h5>Keys</h5>
<dl>
<dt><a href="KCOUNT.html">KCOUNT</a></dt>
<dd>Count keys</dd>
<dt><a href="KCOUNTRANGE.html">KCOUNTRANGE</a></dt>
<dd>Count keys in range</dd>
<dt><a href="KEXIST.html">KEXIST</a></dt>
<dd>Determine key existence</dd>
<dt><a href="KGET.html">KGET</a></dt>
<dd>Return existing keys from discrete set</dd>
<dt><a href="KGETRANGE.html">KGETRANGE</a></dt>
<dd>Return existing keys in range</dd>
<dt><a href="RSIZE.html">RSIZE</a></dt>
<dd>Return range size on disk</dd>
</dl>

<h5>Columns</h5>
<dl>
<dt><a href="SGETCC.html">SGETCC</a></dt>
<dd>Get continuous column range over continuous key range</dd>
<dt><a href="SGETCD.html">SGETCD</a></dt>
<dd>Get discrete column range over continuous key range</dd>
<dt><a href="SGETDC.html">SGETDC</a></dt>
<dd>Get continuous column range over discrete key range</dd>
<dt><a href="SGETDD.html">SGETDD</a></dt>
<dd>Get discrete column range over discrete key range</dd>
</dl>
</div>


<div class="span3">
<h4>Write operations</h4>
<dl>
<dt><a href="SPUT.html">SPUT</a></dt>
<dd>Write (or update) columns</dd>
<dt><a href="CDEL.html">CDEL</a></dt>
<dd>Delete columns</dd>
</dl>
</div>

</div>
