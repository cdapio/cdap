.. :Author: John Jackson
   :Description: Advanced Reactor Features

=====================================
Advanced Continuuity Reactor Features
=====================================

------------------------------
Building Big Data Applications
------------------------------

.. reST Editor: section-numbering::

.. reST Editor: contents::


Flow System
===========
Flows are user-implemented real-time stream processors. They are comprised of one or more Flowlets that are wired together into a DAG. Flowlets pass data between one another; each Flowlet is able to perform custom logic and execute data operations for each individual data object it processes.

A Flowlet processes the data objects from its input one by one. If a Flowlet has multiple inputs, they are consumed in a round-robin fashion. When processing a single input object, all operations, including the removal of the object from the input, and emission of data to the outputs, are executed in a transaction. This provides us with Atomicity, Consistency, Isolation, and Durability (ACID) properties, and helps assure a unique and core property of the Flow system: it guarantees atomic and "exactly-once" processing of each input object by each Flowlet in the DAG.

Batch Execution
---------------
By default, a Flowlet processes a single data object at a time within a single transaction. To increase throughput, you can also process a batch of data objects within the same transaction::

	@Batch(100)
	@ProcessInput
	public void process(Iterator<String> words) {
	  ...

For the batch example above, up to 100 data objects can be read from the input and processed at one time.

Flows and Instances
-------------------
You can have one or more instances of any given Flowlet, each consuming a disjoint partition of each input. You can control the number of instances programmatically via the REST interfaces or via the Dashboard. This enables you to scale your application to meet capacity at runtime.

In the Local Reactor, multiple Flowlet instances are run in threads, so in some cases actual performance may not be affected. However, in the Hosted and Enterprise Reactors each Flowlet instance runs in its own Java Virtual Machine (JVM) with independent compute resources, so scaling the number of Flowlets can improve performance, and depending on your implementation, this can have a major impact.

Partitioning Strategies
-----------------------
As mentioned earlier, if you have multiple instances of a Flowlet the input queue is partitioned among the Flowlets. The partitioning can occur in different ways, and each Flowlet can specify one of these three partitioning strategies:

- **First-in first-out (FIFO)**: Default mode. In this mode, every Flowlet instance receives the next available data object in the queue. However, since multiple consumers may compete for the same data object, access to the queue must be synchronized. This may not always be the most efficient strategy.

- **Round-robin**: With this strategy, the number of items is distributed evenly among the instances. Round-robin is, in general, the most efficient partitioning. Though more efficient than FIFO, it is not always ideal, such as in cases where the application needs to group objects into buckets according to business logic. In these cases, hash-based partitioning is preferable.

- **Hash-based**: If the emitting Flowlet annotates each data object with a hash key, this partitioning ensures that all objects of a given key are received by the same consumer instance. This can be useful for aggregating by key, and can help reduce write conflicts.

Suppose we have a Flowlet that counts words::

	public class Counter extends AbstractFlowlet {
	
	  @UseDataSet("wordCounts")
	  private KeyValueTable wordCountsTable;
	
	  @ProcessInput("wordOut")
	  public void process(String word) {
	    this.wordCountsTable.increment(Bytes.toBytes(word), 1L);
	  }
	}

This Flowlet uses the default strategy of FIFO. To increase the throughput when this Flowlet has many instances, we can specify round-robin partitioning::

	@RoundRobin @ProcessInput("wordOut")
	public void process(String word) {
	  this.wordCountsTable.increment(Bytes.toBytes(word), 1L);
	}

Now, if we have three instances of this Flowlet, every instance will receive every third word. For example, for the sequence of words in the sentence, “I scream, you scream, we all scream for ice cream”:

- The first instance receives the words: *I scream scream cream*
- The second instance receives the words: *scream we for*

The potential problem with this is that both instances might attempt to increment the counter for the word *scream* at the same time, and that may lead to a write conflict. To avoid conflicts we can use hash-based partitioning::

	@HashPartition("wordHash")
	@ProcessInput("wordOut")
	public void process(String word) {
	  this.wordCountsTable.increment(Bytes.toBytes(word), 1L);
	}

Now only one of the Flowlet instances will receive the word *scream*, and there can be no more write conflicts. Note that in order to use hash-based partitioning, the emitting Flowlet must annotate each data object with the partitioning key::

	@Output("wordOut")
	private OutputEmitter<String> wordOutput;
	...
	public void process(StreamEvent event) {
	  ...
	  // emit the word with the partitioning key name "wordHash"
	  wordOutput.emit(word, "wordHash", word.hashCode());
	}

Note that the emitter must use the same name ("wordHash") for the key that the consuming Flowlet specifies as the partitioning key. If the output is connected to more than one Flowlet, you can also annotate a data object with multiple hash keys – each consuming Flowlet can then use different partitioning. This is useful if you want to aggregate by multiple keys, such as counting purchases by product ID as well as by customer ID.

Partitioning can be combined with batch execution::

	@Batch(100)
	@HashPartition("wordHash") @ProcessInput("wordOut")
	public void process(Iterator<String> words) {
	   ...


DataSet System
==============
DataSets are your interface to the data. Instead of having to manipulate data with low-level APIs, DataSets provide higher level abstractions and generic, reusable Java implementations of common data patterns. A DataSet represents both the API and the actual data itself. In other words, a DataSet class is a reusable, generic Java implementation of a common data pattern. A DataSet instance is a named collection of data with associated metadata, and it is manipulated through a DataSet class.

Types of DataSets
-----------------
A DataSet is a Java class that extends the abstract DataSet class with its own, custom methods. The implementation of a DataSet typically relies on one or more underlying (embedded) DataSets. For example, the ``IndexedTable`` DataSet can be implemented by two underlying Table DataSets – one holding the data and one holding the index. 

We distinguish three categories of DataSets: *core*, *system*, and *custom* DataSets:

- The **core** DataSet of the Reactor is a Table. Its implementation is
  hidden from developers and it may use private DataSet interfaces that are not available to you.

- A **system** DataSet is bundled with the Reactor and is built around
  one or more underlying core or system DataSets to implement a specific data pattern.

- A **custom** DataSet is implemented by you and can have arbitrary code and methods.
  It is typically built around one or more Tables (or other DataSets)
  to implement a specific data pattern. A custom DataSet can only manipulate data
  through its underlying DataSets.

.. - A **system** DataSet is bundled with the Reactor but implemented
.. in the same way as a custom DataSet, relying on one or more underlying core or system DataSets.

Each DataSet instance has exactly one DataSet class to manipulate it - think of the class as the type or the interface of the DataSet. Every instance of a DataSet has a unique name (unique within the account that it belongs to), and some metadata that defines its behavior. For example, every IndexedTable has a name and indexes a particular column of its primary table: the name of that column is a metadata property of each instance.

Every application must declare all DataSets that it uses in its application specification. The specification of the DataSet must include its name and all of its metadata, including the specifications of its underlying DataSets. This creates the DataSet - if it does not exist yet - and stores its metadata at the time of deployment of the application. Application code (for example, a flow or procedure) can then use a DataSet by giving only its name and type - the runtime system can use the stored metadata to create an instance of the DataSet class with all required metadata.

Core DataSets
-------------
Tables are the only core DataSets, and all other DataSets are built using one or more core Tables. These Tables are similar to tables in a relational database with a few key differences:

- Tables have no fixed schema. Unlike relational database tables where every
  row has the same schema, every row of a Table can have a different set of columns.

- Because the set of columns is not known ahead of time, the columns of
  a row do not have a rich type. All column values are byte arrays and
  it is up to the application to convert them to and from rich types.
  The column names and the row key are also byte arrays.

- When reading from a Table, one need not know the names of the columns:
  The read operation returns a map from column name to column value.
  It is, however, possible to specify exactly which columns to read.

- Tables are organized in a way that the columns of a row can be read
  and written independently of other columns, and columns are ordered
  in byte-lexicographic order. They are also known as *Ordered Columnar Tables*.


Table API
---------
The table API provides basic methods to perform read, write and delete operations, plus special atomic increment and compare-and-swap operations::

	// Read
	public Row get(Get get)
	public Row get(byte[] row)
	public byte[] get(byte[] row, byte[] column)
	public Row get(byte[] row, byte[][] columns)
	public Row get(byte[] row, byte[] startColumn,
	               byte[] stopColumn, int limit)

	// Scan
	public Scanner scan(byte[] startRow, byte[] stopRow)

	// Write
	public void put(Put put)
	public void put(byte[] row, byte[] column, byte[] value)
	public void put(byte[] row, byte[][] columns, byte[][] values)

	// Compare And Swap
	public boolean compareAndSwap(byte[] row, byte[] column,
	                              byte[] expectedValue, byte[] newValue)

	// Increment
	public Row increment(Increment increment)
	public long increment(byte[] row, byte[] column, long amount)
	public Row increment(byte[] row, byte[][] columns, long[] amounts)

	// Delete
	public void delete(Delete delete)
	public void delete(byte[] row)
	public void delete(byte[] row, byte[] column)
	public void delete(byte[] row, byte[][] columns)

Every basic operation has a method that takes operation type object as a parameter and also handy methods for working with byte arrays directly. If your application code already deals with byte arrays you can use the latter ones to save on conversion. Otherwise methods with parameters of specialized type could be more convenient to use as they provide reach API to work with different types.

Read
....
A get operation reads all columns or selection of columns of a single row::

	Table t;
	byte[] rowKey1;
	byte[] columnX;
	byte[] columnY;

	// read all columns of a row
	Row row = t.get(new Get("rowKey1"));

	// read specified columns from the row
	Row rowSelection = t.get(new Get("rowKey1").add("column1").add("column2"));

	// reads a column range from x to y, with a limit of n return values
	rowSelection = t.get(rowKey1, columnX, columnY);

	// read only one column in one row byte[]
	value = t.get(rowKey1, columnX);

The Row object provides access to the Row data including its columns. If only a selection of a row columns is requested, the returned Row object will contain only these columns. Row object provides rich API for accessing returned column values::

	// get column value as byte array
	byte[] value = row.get("column1");

	// get column value of specific type
	String valueAsString = row.getString("column1");
	Integer valueAsInteger = row.getInt("column1");

When requested, value of a column is converted to specific type automatically. If column is absent in a Row, the returned value is null. To return primitive type correspondent methods accept default value to be returned when column is absent::

	// get column value of primitive type or 0 if column is absent
	long valueAsLong = row.getLong("column1", 0);

Scan
....
A scan operation fetches a subset of rows or all rows of a table::

	byte[] startRow;
	byte[] stopRow;
	Row row;
	
	// Scan all rows from startRow (inclusive) to stopRow (exclusive)
	Scanner scanner = t.scan(startRow, stopRow);
	try {
	  while ((row = scanner.next()) != null) {
	  LOG.info("column1: " + row.getString("column1"));
	  }
	} finally {
	  scanner.close();
	}

To scan a set of rows not bounded by startRow and/or stopRow you can pass null as their value::

	byte[] startRow;
	// scan all rows of a table
	Scanner allRows = t.scan(null, null);
	// scan all columns up to stopRow (exclusive)
	Scanner headRows = t.scan(null, stopRow);
	// scan all columns starting from startRow (inclusive)
	Scanner tailRows = t.scan(startRow, null);

Write
.....
A put operation writes data into a row::

	// write set of columns with their values
	t.put(new Put("rowKey1").add("column1", "value1").add("column2", 55L));


Compare and Swap
................
A swap operation compares the existing value of a column with an expected value,
and if it matches, replaces it with a new value.
The operation returns true if it succeeds and false otherwise::

	byte[] expectedCurrentValue;
	byte[] newValue;
	if (!t.compareAndSwap(rowKey1, columnX, expectedCurrentValue, newValue)) {
	  LOG.info("Current value was different from expected");
	}

Increment
.........
An increment operation increments a long value of one or more columns. If a column doesn’t exist, it is created and it is assumed the value before the increment was 0::

	// write long value to a column of a row
	t.put(new Put("rowKey1").add("column1", 55L));
	// increment values of several columns in a row
	t.increment(new Increment("rowKey1").add("column1", 1L).add("column2", 23L));

If the existing value of the column cannot be converted to long,
a ``NumberFormatException`` will be thrown.

Delete
......
A delete operation removes a whole row or subset of its columns::

	// delete the whole row
	t.delete(new Delete("rowKey1"));
	// delete a set of columns from the row
	t.delete(new Delete("rowKey1").add("column1").add("column2"));

Note that specifying a set of columns helps to perform delete operation faster. Thus, when you know all columns of a row you want to delete, passing them will make deletion faster.

System DataSets
---------------
The Continuuity Reactor comes with several system-defined DataSets, including key/value Tables, indexed Tables and time series. Each of them is defined with the help of one or more embedded Tables, but defines its own interface. For example:

- The ``KeyValueTable`` implements a key/value store as a Table with a single column.

- The ``IndexedTable`` implements a Table with a secondary key using two embedded Tables,
  one for the data and one for the secondary index.

- The ``TimeseriesTable`` uses a Table to store keyed data over time
  and allows querying that data over ranges of time.

See the Javadocs for of these classes to learn more about these DataSets.

Custom DataSets
---------------
You can define your own DataSet classes to implement common data patterns specific to your code. For example, suppose you want to define a counter table that in addition to counting words also counts how many unique words it has seen. The DataSet will be built on top two underlying DataSets, a KeyValueTable to count all the words and a core table for the unique count::

	public class UniqueCountTable extends DataSet {

	  private Table uniqueCountTable;
	  private Table entryCountTable;

Custom DataSets can also optionally implement ``configure()`` and ``initialize()`` methods. The ``configure()`` method returns a specification which we can use to save metadata about the DataSet (such as configuration parameters). The ``initialize()`` method is called at execution time. It should be noted that any operations on the data of this DataSet are prohibited in ``initialize()``.

Now we can begin with the implementation of the DataSet logic. We start with a few constants::

	// Row and column name used for storing the unique count.
	private static final byte [] UNIQUE_COUNT = Bytes.toBytes("unique");
	// Column name used for storing count of each entry.
	private static final byte[] ENTRY_COUNT = Bytes.toBytes("count");

The DataSet stores a counter for each word in its own row of the word count table. For every word the counter is incremented. If the result of increment is 1, then this was the first time we encountered the word, hence we have new unique word and we increment the unique counter::

	public void updateUniqueCount(String entry) {
	  long newCount = entryCountTable.increment(Bytes.toBytes(entry), ENTRY_COUNT, 1L);
	  if (newCount == 1L) {
	    uniqueCountTable.increment(UNIQUE_COUNT, UNIQUE_COUNT, 1L);
	  }
	}

Finally, we write a method to retrieve the number of unique words seen::

	public Long readUniqueCount() {
	  return uniqueCountTable.get(new Get(UNIQUE_COUNT, UNIQUE_COUNT))
	                         .getLong(UNIQUE_COUNT, 0);
	}

.. Example Application with Custom DataSet
.. ```````````````````````````````````````
.. [DOCNOTE: FIXME!] Insert WordCount Application

DataSets & MapReduce
--------------------

A MapReduce job can interact with a DataSet by using it as an input or an output. The DataSet should implement specific interfaces to support this.

When you run a MapReduce job, you can configure it to read its input from a DataSet. The destination DataSet must implement the BatchReadable interface, which requires two methods::

	public interface BatchReadable<KEY, VALUE> {
	  List<Split> getSplits();
	  SplitReader<KEY, VALUE> createSplitReader(Split split);
	}

These two methods complement each other: ``getSplits()`` must return all splits of the DataSet that the MapReduce job will read; ``createSplitReader()`` is then called in every mapper to read one of the splits. Note that the ``KEY`` and ``VALUE`` type parameters of the split reader must match the input key and value type parameters of the mapper.

Because ``getSplits()`` has no arguments, it will typically create splits that cover the entire DataSet. If you want to use a custom selection of the input data, you can define another method in your DataSet that takes additional parameters, and explicitly set the input in the ``beforeSubmit()`` method. 

For example, the system DataSet ``KeyValueTable`` implements ``BatchReadable<byte[], byte[]>`` with an extra method that allows to specify the number of splits and a range of keys::

	public class KeyValueTable extends DataSet
	                           implements BatchReadable<byte[], byte[]> {
	  ...
	  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop);
	}

To read only a range of keys and give a hint that you want to get 16 splits, write::

	@Override
	@UseDataSet("myTable")
	KeyValueTable kvTable;
	...
	public void beforeSubmit(MapReduceContext context) throws Exception {
	  ...
	  context.setInput(kvTable, kvTable.getSplits(16, startKey, stopKey);
	}

Similarly to reading input from a DataSet, you have the option to write to a DataSet as the output destination of a MapReduce job—if that DataSet implements the ``BatchWritable`` interface::

	public interface BatchWritable<KEY, VALUE> {
	  void write(KEY key, VALUE value);
	}

The ``write()`` method is used to redirect all writes performed by a reducer to the DataSet.
Again, the ``KEY`` and ``VALUE`` type parameters must match the output key and value type parameters of the reducer.


Getting Data into Continuuity Reactor
=====================================
.. [DOCNOTE: FIXME!] Rewrite this section

Input data can be pushed to a Flow using Streams or pulled from within a Flow using a Flowlet.

- A Stream is passively receiving events from outside (remember that streams exist outside the scope of a flow).
  To consume a Stream, connect the Stream to a Flowlet that implements a process method for ``StreamEvent``.
  This is useful when your events come from an external system that can push data using REST calls.
  It is also useful when you’re developing and testing your application, because your test driver
  can send mock data to the Stream that covers all your test cases.

- A Flowlet method with an ``@Tick`` annotation can be used to actively generate data or retrieve
  it from an external data source. For instance, it can pull data from the Twitter "firehose".


Transaction System
==================

Need for Transactions
---------------------

A Flowlet processes the data objects from its inputs one at a time. While processing a single input object, all operations, including the removal of the data from the input, and emission of data to the outputs, are executed in a transaction. This provides us with ACID
(atomicity, consistency, isolation, and durability) properties:

- The process method runs under read isolation to ensure that it does not see dirty writes
  (uncommitted writes from concurrent processing) in any of its reads. 
  It does see, however, its own writes.

- A failed attempt to process an input object leaves the data in a consistent state,
  that is, it does not leave partial writes behind.

- All writes and emission of data are committed atomically, that is,
  either all of them or none of them are persisted.

- After processing completes successfully, all its writes are persisted in a durable way.

In case of failure, the state of the data is unchanged and therefore, processing of the input
object can be reattempted. This ensures "exactly-once" processing of each object.

OCC: Optimistic Concurrency Control
-----------------------------------

The Reactor uses *Optimistic Concurrency Control* (OCC) to implement transactions. Unlike most relational databases that use locks to prevent conflicting operations between transactions, under OCC we allow these conflicting writes to happen. When the transaction is committed, we can detect whether it has any conflicts: namely if during the lifetime of this transaction, another transaction committed a write for one the same keys that this transaction has written. In that case, the transaction is aborted and all of its writes are rolled back.

In other words: If two overlapping transactions modify the same row, then the transaction that commits first will succeed, but the transaction that commits last is rolled back due to a write conflict.

Optimistic Concurrency Control is lockless and therefore avoids problems such as idle processes waiting for locks, or even worse, deadlocks. However, it comes at the cost of rollback in case of write conflicts. We can only achieve high throughput with OCC if the number of conflicts is small. It is therefore a good practice to reduce the probability of conflicts where possible:

- Keep transactions short. The Reactor attempts to delay the beginning of each
  transaction as long as possible. For instance, if your Flowlet only performs write
  operations, but no read operations, then all writes are deferred until the process
  method returns. They are then performed and transacted, together with the
  removal of the processed object from the input, in a single batch execution.
  This minimizes the duration of the transaction.

- However, if your Flowlet performs a read, then the transaction must
  begin at the time of the read. If your Flowlet performs long-running
  computations after that read, then the transaction runs longer, too,
  and the risk of conflicts increases. It is therefore a good practice
  to perform reads as late in the process method as possible.

- There are two ways to perform an increment: As a write operation that
  returns nothing, or as a read-write operation that returns the incremented
  value. If you perform the read-write operation, then that forces the
  transaction to begin, and the chance of conflict increases. Unless you
  depend on that return value, you should always perform an increment as a write operation.

- Use hash-based partitioning for the inputs of highly concurrent Flowlets
  that perform writes. This helps reduce concurrent writes to the same
  key from different instances of the Flowlet.

Keeping these guidelines in mind will help you write more efficient code.

Transactions in Flows
---------------------
[DOCNOTE: FIXME!] missing information

Need for Disabling Transactions
...............................
[DOCNOTE: FIXME!] missing information

Disabling Transactions
......................
Transaction can be disabled for a Flow by annotating the Flow class with the @DisableTransaction annotation. While this may speed up performance, if a Flowlet fails, for example, the system would not be able to roll back to its previous state::

	@DisableTransaction
	class MyExampleFlow implements Flow {
	  ...
	}


Transactions in MapReduce
-------------------------
When you run a MapReduce that interacts with DataSets, the system creates a long-running transaction. Similar to the transaction of a Flowlet or a Procedure:

- Reads can only see the writes of other transactions that were committed 
  at the time the long-running transaction was started.

- All writes of the long-running transaction are committed atomically,
  and only become visible to others after they are committed.

- The long-running transaction can read its own writes.

However, there is a key difference: Long-running transactions do not participate in conflict detection. If another transaction overlaps with the long-running transaction and writes to the same row, it will not cause a conflict but simply overwrite it. It is not efficient to fail the long running job based on a single conflict. Because of this, it is not recommended to write to the same DataSets from both real-time and MapReduce programs. It is better to use different DataSets, or at least ensure that the real-time processing writes to a disjoint set of columns.

Important to note that MapReduce framework will reattempt a task (mapper or reducer) if it fails. If the task is writing to a DataSet, the reattempt of the task will most likely repeat the writes that were already performed in the failed attempt. Therefore it is highly advisable that all writes performed by MapReduce programs be idempotent.


.. include:: includes/footer.rst
