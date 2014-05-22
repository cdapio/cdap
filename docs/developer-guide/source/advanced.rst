.. :Author: Continuuity, Inc.
   :Description: Advanced Reactor Features

=====================================
Advanced Continuuity Reactor Features
=====================================

Building Big Data Applications

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::


Flow System
===========
**Flows** are user-implemented real-time stream processors. They are comprised of one or more **Flowlets** that are wired together into a directed acyclic graph or DAG. Flowlets pass data between one another; each Flowlet is able to perform custom logic and execute data operations for each individual data object it processes.

A Flowlet processes the data objects from its input one by one. If a Flowlet has multiple inputs, they are consumed in a round-robin fashion. When processing a single input object, all operations, including the removal of the object from the input, and emission of data to the outputs, are executed in a transaction. This provides us with Atomicity, Consistency, Isolation, and Durability (ACID) properties, and helps assure a unique and core property of the Flow system: it guarantees atomic and "exactly-once" processing of each input object by each Flowlet in the DAG.

Batch Execution
---------------
By default, a Flowlet processes a single data object at a time within a single transaction. To increase throughput, you can also process a batch of data objects within the same transaction::

	@Batch(100)
	@ProcessInput
	public void process(Iterator<String> words) {
	  ...

For the batch example above, up to 100 data objects can be read from the input and processed at one time.

Flowlets and Instances
----------------------
You can have one or more instances of any given Flowlet, each consuming a disjoint partition of each input. You can control the number of instances programmatically via the
`REST interfaces <rest.html>`__ or via the Continuuity Reactor Dashboard. This enables you to scale your application to meet capacity at runtime.

In the Local Reactor, multiple Flowlet instances are run in threads, so in some cases actual performance may not be improved. However, in the Hosted and Enterprise Reactors each Flowlet instance runs in its own Java Virtual Machine (JVM) with independent compute resources. Scaling the number of Flowlets can improve performance and have a major impact depending on your implementation.

Partitioning Strategies
-----------------------
As mentioned above, if you have multiple instances of a Flowlet the input queue is partitioned among the Flowlets. The partitioning can occur in different ways, and each Flowlet can specify one of these three partitioning strategies:

- **First-in first-out (FIFO):** Default mode. In this mode, every Flowlet instance receives the next available data object in the queue. However, since multiple consumers may compete for the same data object, access to the queue must be synchronized. This may not always be the most efficient strategy.

- **Round-robin:** With this strategy, the number of items is distributed evenly among the instances. In general, round-robin is the most efficient partitioning. Though more efficient than FIFO, it is not ideal when the application needs to group objects into buckets according to business logic. In those cases, hash-based partitioning is preferable.

- **Hash-based:** If the emitting Flowlet annotates each data object with a hash key, this partitioning ensures that all objects of a given key are received by the same consumer instance. This can be useful for aggregating by key, and can help reduce write conflicts.

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

	@RoundRobin
	@ProcessInput("wordOut")
	public void process(String word) {
	  this.wordCountsTable.increment(Bytes.toBytes(word), 1L);
	}

Now, if we have three instances of this Flowlet, every instance will receive every third word. For example, for the sequence of words in the sentence, “I scream, you scream, we all scream for ice cream”:

- The first instance receives the words: *I scream scream cream*
- The second instance receives the words: *scream we for*
- The third instance receives the words: *you all ice*

The potential problem with this is that the first two instances might
both attempt to increment the counter for the word *scream* at the same time,
leading to a write conflict. To avoid conflicts, we can use hash-based partitioning::

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

Note that the emitter must use the same name ("wordHash") for the key that the consuming Flowlet specifies as the partitioning key. If the output is connected to more than one Flowlet, you can also annotate a data object with multiple hash keys—each consuming Flowlet can then use different partitioning. This is useful if you want to aggregate by multiple keys, such as counting purchases by product ID as well as by customer ID.

Partitioning can be combined with batch execution::

	@Batch(100)
	@HashPartition("wordHash")
	@ProcessInput("wordOut")
	public void process(Iterator<String> words) {
	   ...


DataSet System
==============
**DataSets** are your interface to the data. Instead of having to manipulate data with low-level APIs, DataSets provide higher level abstractions and generic, reusable Java implementations of common data patterns. A DataSet represents both the API and the actual data itself; it is a named collection of data with associated metadata, and it is manipulated through a DataSet class.

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

Each DataSet instance has exactly one DataSet class to manipulate it—think of the class
as the type or the interface of the DataSet. Every instance of a DataSet has a unique name
(unique within the account that it belongs to) and metadata that defines its behavior.
For example, every ``IndexedTable`` has a name and indexes a particular column of its primary table: the name of that column is a metadata property of each instance.

Every Application must declare all DataSets that it uses in its application specification. 
The specification of the DataSet must include its name and all of its metadata, including
the specifications of its underlying DataSets. This creates the DataSet—if it does not
exist yet—and stores its metadata at the time of deployment of the application.
Application code (a Flow or Procedure) can then use a DataSet by giving only its name and
type—the runtime system uses the stored metadata to create an instance of the DataSet
class with all required metadata.

Core DataSets
-------------
**Tables** are the only core DataSets, and all other DataSets are built using one or more
core Tables. These Tables are similar to tables in a relational database with a few key differences:

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
The ``Table`` API provides basic methods to perform read, write and delete operations,
plus special scan, atomic increment and compare-and-swap operations::

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

Each basic operation has a method that takes an operation-type object as a parameter
plus handy methods for working directly with byte arrays.
If your application code already deals with byte arrays, you can use the latter methods to save a conversion.

Read
....
A ``get`` operation reads all columns or selection of columns of a single row::

	Table t;
	byte[] rowKey1;
	byte[] columnX;
	byte[] columnY;
	int n;

	// Read all columns of a row
	Row row = t.get(new Get("rowKey1"));

	// Read specified columns from a row
	Row rowSelection = t.get(new Get("rowKey1").add("column1").add("column2"));

	// Reads a column range from x (inclusive) to y (exclusive)
	// with a limit of n return values
	rowSelection = t.get(rowKey1, columnX, columnY; n);

	// Read only one column in one row byte[]
	value = t.get(rowKey1, columnX);

The ``Row`` object provides access to the Row data including its columns. If only a 
selection of row columns is requested, the returned Row object will contain only these columns.
The Row object provides an extensive API for accessing returned column values::

	// Get column value as a byte array
	byte[] value = row.get("column1");

	// Get column value of a specific type
	String valueAsString = row.getString("column1");
	Integer valueAsInteger = row.getInt("column1");

When requested, the value of a column is converted to a specific type automatically.
If the column is absent in a Row, the returned value is ``null``. To return primitive types,
the corresponding methods accepts default value to be returned when the column is absent::

	// Get column value as a primitive type or 0 if column is absent
	long valueAsLong = row.getLong("column1", 0);

Scan
....
A ``scan`` operation fetches a subset of rows or all of the rows of a Table::

	byte[] startRow;
	byte[] stopRow;
	Row row;

	// Scan all rows from startRow (inclusive) to
	// stopRow (exclusive)
	Scanner scanner = t.scan(startRow, stopRow);
	try {
	  while ((row = scanner.next()) != null) {
	    LOG.info("column1: " + row.getString("column1", "null"));
	  }
	} finally {
	  scanner.close();
	}

To scan a set of rows not bounded by ``startRow`` and/or ``stopRow``
you can pass ``null`` as their value::

	byte[] startRow;
	// Scan all rows of a table
	Scanner allRows = t.scan(null, null);
	// Scan all columns up to stopRow (exclusive)
	Scanner headRows = t.scan(null, stopRow);
	// Scan all columns starting from startRow (inclusive)
	Scanner tailRows = t.scan(startRow, null);

Write
.....
A ``put`` operation writes data into a row::

	// Write a set of columns with their values
	t.put(new Put("rowKey1").add("column1", "value1").add("column2", 55L));


Compare and Swap
................
A swap operation compares the existing value of a column with an expected value,
and if it matches, replaces it with a new value.
The operation returns ``true`` if it succeeds and ``false`` otherwise::

	byte[] expectedCurrentValue;
	byte[] newValue;
	if (!t.compareAndSwap(rowKey1, columnX,
	      expectedCurrentValue, newValue)) {
	  LOG.info("Current value was different from expected");
	}

Increment
.........
An increment operation increments a ``long`` value of one or more columns by either ``1L``
or an integer amount *n*.
If a column doesn’t exist, it is created with an assumed value
before the increment of zero::

	// Write long value to a column of a row
	t.put(new Put("rowKey1").add("column1", 55L));
	// Increment values of several columns in a row
	t.increment(new Increment("rowKey1").add("column1", 1L).add("column2", 23L));

If the existing value of the column cannot be converted to a ``long``,
a ``NumberFormatException`` will be thrown.

Delete
......
A delete operation removes an entire row or a subset of its columns::

	// Delete the entire row
	t.delete(new Delete("rowKey1"));
	// Delete a selection of columns from the row
	t.delete(new Delete("rowKey1").add("column1").add("column2"));

Note that specifying a set of columns helps to perform delete operation faster.
When you want to delete all the columns of a row and you know all of them,
passing all of them will make the deletion faster.

System DataSets
---------------
The Continuuity Reactor comes with several system-defined DataSets, including key/value Tables, indexed Tables and time series. Each of them is defined with the help of one or more embedded Tables, but defines its own interface. For example:

- The ``KeyValueTable`` implements a key/value store as a Table with a single column.

- The ``IndexedTable`` implements a Table with a secondary key using two embedded Tables,
  one for the data and one for the secondary index.

- The ``TimeseriesTable`` uses a Table to store keyed data over time
  and allows querying that data over ranges of time.

See the `Javadocs <javadocs/index.html>`__ for these classes and `the examples <examples/index.html>`
to learn more about these DataSets.

Custom DataSets
---------------
You can define your own DataSet classes to implement common data patterns specific to your code. Suppose you want to define a counter table that, in addition to counting words,
counts how many unique words it has seen. The DataSet will be built on top two underlying DataSets, one Table (``entryCountTable``) to count all the words and a second Table (``uniqueCountTable``) for the unique count::

	public class UniqueCountTable extends DataSet {

	  private Table entryCountTable;
	  private Table uniqueCountTable;

Custom DataSets can also optionally implement ``configure()`` and ``initialize()`` methods. The ``configure()`` method returns a specification which we can use to save metadata about the DataSet (such as configuration parameters). The ``initialize()`` method is called at execution time. It should be noted that any operations on the data of this DataSet are prohibited in ``initialize()``.

Now we can begin with the implementation of the ``UniqueCountTable`` logic. We start with a few constants::

	// Column name used for storing count of each entry.
	private static final byte[] ENTRY_COUNT = Bytes.toBytes("count");
	// Row and column name used for storing the unique count.
	private static final byte [] UNIQUE_COUNT = Bytes.toBytes("unique");

The ``UniqueCountTable`` stores a counter for each word in its own row of the entry count table. For each word the counter is incremented. If the result of the increment is 1, then this is the first time we've encountered the word, hence we have a new unique word and we increment the unique counter::

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

A complete application demonstrating use of a Custom DataSet is included in our `PageViewAnalytics <examples/PageViewAnalytics/index.html>` example.

DataSets & MapReduce
--------------------

A MapReduce job can interact with a DataSet by using it as an input or an output.
The DataSet needs to implement specific interfaces to support this.

When you run a MapReduce job, you can configure it to read its input from a DataSet. The source DataSet must implement the ``BatchReadable`` interface, which requires two methods::

	public interface BatchReadable<KEY, VALUE> {
	  List<Split> getSplits();
	  SplitReader<KEY, VALUE> createSplitReader(Split split);
	}

These two methods complement each other: ``getSplits()`` must return all splits of the DataSet that the MapReduce job will read; ``createSplitReader()`` is then called in every Mapper to read one of the splits. Note that the ``KEY`` and ``VALUE`` type parameters of the split reader must match the input key and value type parameters of the Mapper.

Because ``getSplits()`` has no arguments, it will typically create splits that cover the entire DataSet. If you want to use a custom selection of the input data, define another method in your DataSet with additional parameters and explicitly set the input in the ``beforeSubmit()`` method.

For example, the system DataSet ``KeyValueTable`` implements ``BatchReadable<byte[], byte[]>`` with an extra method that allows specification of the number of splits and a range of keys::

	public class KeyValueTable extends DataSet
	                           implements BatchReadable<byte[], byte[]> {
	  ...
	  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop);
	}

To read a range of keys and give a hint that you want 16 splits, write::

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

The ``write()`` method is used to redirect all writes performed by a Reducer to the DataSet.
Again, the ``KEY`` and ``VALUE`` type parameters must match the output key and value type parameters of the Reducer.


Transaction System
==================

The Need for Transactions
-------------------------

A Flowlet processes the data objects received on its inputs one at a time. While processing a single input object, all operations, including the removal of the data from the input, and emission of data to the outputs, are executed in a **transaction**. This provides us with ACID—atomicity, consistency, isolation, and durability properties:

- The process method runs under read isolation to ensure that it does not see dirty writes
  (uncommitted writes from concurrent processing) in any of its reads.
  It does see, however, its own writes.

- A failed attempt to process an input object leaves the data in a consistent state;
  it does not leave partial writes behind.

- All writes and emission of data are committed atomically;
  either all of them or none of them are persisted.

- After processing completes successfully, all its writes are persisted in a durable way.

In case of failure, the state of the data is unchanged and processing of the input
object can be reattempted. This ensures "exactly-once" processing of each object.

OCC: Optimistic Concurrency Control
-----------------------------------

The Continuuity Reactor uses *Optimistic Concurrency Control* (OCC) to implement transactions. Unlike most relational databases that use locks to prevent conflicting operations between transactions, under OCC we allow these conflicting writes to happen. When the transaction is committed, we can detect whether it has any conflicts: namely, if during the lifetime of the transaction, another transaction committed a write for one of the same keys that the transaction has written. In that case, the transaction is aborted and all of its writes are rolled back.

In other words: If two overlapping transactions modify the same row, then the transaction that commits first will succeed, but the transaction that commits last is rolled back due to a write conflict.

Optimistic Concurrency Control is lockless and therefore avoids problems such as idle processes waiting for locks, or even worse, deadlocks. However, it comes at the cost of rollback in case of write conflicts. We can only achieve high throughput with OCC if the number of conflicts is small. It is therefore a good practice to reduce the probability of conflicts wherever possible.

Here are some rules to follow for Flows, Flowlets and Procedures:

- Keep transactions short. The Continuuity Reactor attempts to delay the beginning of each
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
  depend on that return value, you should always perform an increment
  only as a write operation.

- Use hash-based partitioning for the inputs of highly concurrent Flowlets
  that perform writes. This helps reduce concurrent writes to the same
  key from different instances of the Flowlet.

Keeping these guidelines in mind will help you write more efficient and faster-performing code.


The Need for Disabling Transactions
-----------------------------------
Transactions providing ACID (atomicity, consistency, isolation, and durability) guarantees are useful in several applications where data accuracy is critical—examples include billing applications and computing click-through rates.

However, some applications—such as trending—might not need it. Applications that do not strictly require accuracy can trade off accuracy against increased throughput by taking advantage of not having to write/read all the data in a transaction.

Disabling Transactions
----------------------
Transaction can be disabled for a Flow by annotating the Flow class with the @DisableTransaction annotation. While this may speed up performance, if—for example—a Flowlet fails, the system would not be able to roll back to its previous state::

	@DisableTransaction
	class MyExampleFlow implements Flow {
	  ...
	}

You will need to judge whether the increase in performance offsets the increased risk of inaccurate data.

Transactions in MapReduce
-------------------------
When you run a MapReduce job that interacts with DataSets, the system creates a long-running transaction. Similar to the transaction of a Flowlet or a Procedure, here are some rules to follow:

- Reads can only see the writes of other transactions that were committed
  at the time the long-running transaction was started.

- All writes of the long-running transaction are committed atomically,
  and only become visible to others after they are committed.

- The long-running transaction can read its own writes.

However, there is a key difference: long-running transactions do not participate in conflict detection. If another transaction overlaps with the long-running transaction and writes to the same row, it will not cause a conflict but simply overwrite it.

It is not efficient to fail the long-running job based on a single conflict. Because of this, it is not recommended to write to the same DataSet from both real-time and MapReduce programs. It is better to use different DataSets, or at least ensure that the real-time processing writes to a disjoint set of columns.

It's important to note that the MapReduce framework will reattempt a task (Mapper or Reducer) if it fails. If the task is writing to a DataSet, the reattempt of the task will most likely repeat the writes that were already performed in the failed attempt. Therefore it is highly advisable that all writes performed by MapReduce programs be idempotent.

Best Practices for Developing Applications
==========================================

Initializing Instance Fields
----------------------------
There are three ways to initialize instance fields used in DataSets, Flowlets and Procedures:

#. Using the default constructor;
#. Using ``initialize()`` method of the DataSets, Flowlets and Procedures; and
#. Using ``@Property`` annotations.

To initialize using Property annotations, simply annotate the field definition with ``@Property``. 

An example demonstrating this is the ``Ticker`` example, where it is used in the custom DataSet 
``MultiIndexedTable`` to set a instance field ``timestampField``.

The instance field ``timestampFieldName`` is annotated with ``@Property``, and
when the DataSet is instantiated and deployed, a value is inserted into ``timestampFieldName``.

When the DataSet is initialized, the value is then used to set ``timestampField``::

	public class MultiIndexedTable extends DataSet {
	  . . .
	  // String representation of the field storing timestamp values
	  @Property
	  private String timestampFieldName;
	  . . .
	  public MultiIndexedTable(String name, byte[] timestampField, Set<byte[]> doNotIndex) {
	    super(name);
	    this.table = new Table(name);
	    this.indexTable = new Table(name + INDEX_SUFFIX);
	    this.timestampFieldName = Bytes.toString(timestampField);
	    this.ignoreIndexing = doNotIndex;
	  }
	
	  @Override
	  public void initialize(DataSetSpecification spec, DataSetContext context) {
	    super.initialize(spec, context);
	    this.timestampField = Bytes.toBytes(timestampFieldName);
	  }
	  . . .

Field types that are supported using the ``@Property`` annotation are primitives,
boxed types (e.g. ``Integer``), ``String`` and ``enum``.


Where to Go Next
================
Now that you've had an introduction to Continuuity Reactor, take a look at:

- `Continuuity Reactor Testing and Debugging Guide <debugging.html>`__,
  which covers both testing and debugging of Continuuity Reactor applications.
