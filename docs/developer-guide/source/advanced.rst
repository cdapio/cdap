.. :Author: Continuuity, Inc.
   :Description: Advanced Reactor Features

=====================================
Advanced Continuuity Reactor Features
=====================================

**Custom Services, Flow, Dataset, and Transaction Systems, 
with Best Practices for the Continuuity Reactor**

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

Custom Services
===============
In addition to Flows, MapReduce jobs, and Procedures, additional Services can be run in a 
Reactor Application. Developers can implement Custom Services that run in program containers,
to interface with a legacy system and perform additional processing beyond the Continuuity processing
paradigms. Examples could include running an IP-to-Geo lookup and serving user-profiles.

Services are implemented as `Twill applications <http://twill.incubator.apache.org>`__ and are run in
YARN. A service's lifecycle can be controlled by using REST endpoints. 

You can add services to your application by calling the ``addService`` method in the 
Application's ``configure`` method::

	public class AnalyticsApp extends AbstractApplication {
	  @Override
	  public void configure() {
	    setName("AnalyticsApp");
	    setDescription("Application for generating mobile analytics");
	    addStream(new Stream("event"));
	    addFlow(new EventProcessingFlow());
	    ....
	    addService(new IpGeoLookupService());
	    addService(new UserLookupServiceService());
	    ...
	  }
	}


A Custom Service is implemented as a Twill application, with one or more runnables. See the 
`Apache Twill guide <http://twill.incubator.apache.org>`__ for additional information.

::

	 public class IpGeoLookupService implements TwillApplication {
	   @Override
	   public TwillSpecification configure() {
	     return TwillSpecification.Builder.with()
	       .setName("IpGeoApplication")
	       .withRunnable()
	       .add(new IpGeoRunnable())
	       .noLocalFiles()
	       .anyOrder()
	       .build();
	  }
	}

The service logic is implemented by extending the ``AbstractTwillRunnable`` and implementing these
methods:

- ``intialize()``
- ``run()``
- ``stop()``
- ``destroy()``

::

	public final class IpGeoRunnable extends AbstractTwillRunnable {
	
	   @Override
	   public void initialize(TwillContext context) {
	     // Service initialization
	   }
	
	   @Override
	   public void run() {
	     // Start the custom service
	   }
	
	   @Override
	   public void stop() {
	     // Called to stop the running the service
	   }
	
	   @Override
	   public void destroy() {
	     // Called before shutting down the service
	   }
	}


Services Integration with Metrics and Logging
---------------------------------------------
Services are integrated with the Reactor Metrics and Logging framework. Programs 
implementing Custom Services can declare Metrics and Logger (SLF4J) member variables and 
the appropriate implementation will be injected by the run-time.

::

	public class IpGeoRunnable extends AbstractTwillRunnable {
	  private Metrics metrics;
	  private static final Logger LOG = LoggerFactory.getLogger(IpGeoRunnable.class);
	
	  @Override
	  public void run() {
	    LOG.info("Running ip geo lookup service");
	           metrics.count("ipgeo.instance", 1);
	  }
	}

The metrics and logs that are emitted by the service are aggregated and accessed similar 
to other program types. See the sections in the 
`Continuuity Reactor Operations Guide <operations.html>`__ on accessing 
`logs <operations.html#logging>`__ and `metrics <operations.html#metrics>`__. 


Service Discovery
-----------------
Services announce the host and port they are running on so that they can be discovered by—and provide
access to—other programs: Flows, Procedures, MapReduce jobs, and other Custom Services.

To announce a Service, call the ``announce`` method from ``TwillContext`` during the 
initialize method. The announce method takes a name—which the Service can register 
under—and the port which the Service is running on. The application name, service ID, and 
hostname required for registering the Service are automatically obtained.

::

	@Override
	public void initialize (TwillContext context) {
	  context.announce("GeoLookup", 7000);
	}


The service can then be discovered in Flows, Procedures, MapReduce jobs, and other Services using
appropriate program contexts.

For example, in Flows::

	public class GeoFlowlet extends AbstractFlowlet {
	
	  // Service discovery for ip-geo service
	  private ServiceDiscovered serviceDiscovered;
	
	  @Override
	  public void intialize(FlowletContext context) {
	    serviceDiscovered = context.discover("MyApp", "IpGeoLookupService", "GeoLookup"); 
	  }
	
	  @ProcessInput
	  public void process(String ip) {
	    Discoverable discoverable = Iterables.getFirst(serviceDiscovered, null);
	    if (discoverable != null) {
	      String hostName = discoverable.getSocketAddress().getHostName();
	      int port = discoverable.getSocketAddress().getPort();
	      // Access the appropriate service using the host and port info
	      ...
	    }
	  }
	}

In MapReduce Mapper/Reducer jobs::

	public class GeoMapper extends Mapper<byte[], Location, Text, Text> 
	    implements ProgramLifecycle<MapReduceContext> {
	
	  private ServiceDiscovered serviceDiscovered;
	  
	  @Override
	  public void initialize(MapReduceContext mapReduceContext) throws Exception {
	    serviceDiscovered = mapReduceContext.discover("MyApp", "IpGeoLookupService", "GeoLookup");
	  }
	  
	  @Override
	  public void map(byte[] key, Location location, Context context) throws IOException, InterruptedException {
	    Discoverable discoverable = Iterables.getFirst(serviceDiscovered, null);
	    if (discoverable != null) {
	      String hostName = discoverable.getSocketAddress().getHostName();
	      int port = discoverable.getSocketAddress().getPort();
	      // Access the appropriate service using the host and port info
	    }
	  }
	}

Using Services
-----------------
Custom Services are not displayed in the Continuuity Reactor Dashboard. To control their
lifecycle, use the `Reactor Client API <rest.html#reactor-client-http-api>`__ as described
in the `Continuuity Reactor HTTP REST API <rest.html#reactor-client-http-api>`__.


Flow System
===========
**Flows** are user-implemented real-time stream processors. They are comprised of one or
more **Flowlets** that are wired together into a directed acyclic graph or DAG. Flowlets
pass data between one another; each Flowlet is able to perform custom logic and execute
data operations for each individual data object it processes.

A Flowlet processes the data objects from its input one by one. If a Flowlet has multiple
inputs, they are consumed in a round-robin fashion. When processing a single input object,
all operations, including the removal of the object from the input, and emission of data
to the outputs, are executed in a transaction. This provides us with Atomicity,
Consistency, Isolation, and Durability (ACID) properties, and helps assure a unique and
core property of the Flow system: it guarantees atomic and "exactly-once" processing of
each input object by each Flowlet in the DAG.

Batch Execution
---------------
By default, a Flowlet processes a single data object at a time within a single
transaction. To increase throughput, you can also process a batch of data objects within
the same transaction::

	@Batch(100)
	@ProcessInput
	public void process(String words) {
	  ...

For the above batch example, the **process** method will be called up to 100 times per
transaction, with different data objects read from the input each time it is called.

If you are interested in knowing when a batch begins and ends, you can use an **Iterator**
as the method argument::

	@Batch(100)
	@ProcessInput
	public void process(Iterator<String> words) {
	  ...

In this case, the **process** will be called once per transaction and the **Iterator**
will contain up to 100 data objects read from the input.

Flowlets and Instances
----------------------
You can have one or more instances of any given Flowlet, each consuming a disjoint
partition of each input. You can control the number of instances programmatically via the
`REST interfaces <rest.html>`__ or via the Continuuity Reactor Dashboard. This enables you
to scale your application to meet capacity at runtime.

In the Local Reactor, multiple Flowlet instances are run in threads, so in some cases
actual performance may not be improved. However, in the Hosted and Enterprise Reactors
each Flowlet instance runs in its own Java Virtual Machine (JVM) with independent compute
resources. Scaling the number of Flowlets can improve performance and have a major impact
depending on your implementation.

Partitioning Strategies
-----------------------
As mentioned above, if you have multiple instances of a Flowlet the input queue is
partitioned among the Flowlets. The partitioning can occur in different ways, and each
Flowlet can specify one of these three partitioning strategies:

- **First-in first-out (FIFO):** Default mode. In this mode, every Flowlet instance
  receives the next available data object in the queue. However, since multiple consumers
  may compete for the same data object, access to the queue must be synchronized. This may
  not always be the most efficient strategy.

- **Round-robin:** With this strategy, the number of items is distributed evenly among the
  instances. In general, round-robin is the most efficient partitioning. Though more
  efficient than FIFO, it is not ideal when the application needs to group objects into
  buckets according to business logic. In those cases, hash-based partitioning is
  preferable.

- **Hash-based:** If the emitting Flowlet annotates each data object with a hash key, this
  partitioning ensures that all objects of a given key are received by the same consumer
  instance. This can be useful for aggregating by key, and can help reduce write conflicts.

Suppose we have a Flowlet that counts words::

	public class Counter extends AbstractFlowlet {

	  @UseDataSet("wordCounts")
	  private KeyValueTable wordCountsTable;

	  @ProcessInput("wordOut")
	  public void process(String word) {
	    this.wordCountsTable.increment(Bytes.toBytes(word), 1L);
	  }
	}

This Flowlet uses the default strategy of FIFO. To increase the throughput when this
Flowlet has many instances, we can specify round-robin partitioning::

	@RoundRobin
	@ProcessInput("wordOut")
	public void process(String word) {
	  this.wordCountsTable.increment(Bytes.toBytes(word), 1L);
	}

Now, if we have three instances of this Flowlet, every instance will receive every third
word. For example, for the sequence of words in the sentence, “I scream, you scream, we
all scream for ice cream”:

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

Now only one of the Flowlet instances will receive the word *scream*, and there can be no
more write conflicts. Note that in order to use hash-based partitioning, the emitting
Flowlet must annotate each data object with the partitioning key::

	@Output("wordOut")
	private OutputEmitter<String> wordOutput;
	...
	public void process(StreamEvent event) {
	  ...
	  // emit the word with the partitioning key name "wordHash"
	  wordOutput.emit(word, "wordHash", word.hashCode());
	}

Note that the emitter must use the same name ("wordHash") for the key that the consuming
Flowlet specifies as the partitioning key. If the output is connected to more than one
Flowlet, you can also annotate a data object with multiple hash keys—each consuming
Flowlet can then use different partitioning. This is useful if you want to aggregate by
multiple keys, such as counting purchases by product ID as well as by customer ID.

Partitioning can be combined with batch execution::

	@Batch(100)
	@HashPartition("wordHash")
	@ProcessInput("wordOut")
	public void process(Iterator<String> words) {
	   ...


Datasets System
===============
**Datasets** are your interface to the data. Instead of having to manipulate data with
low-level APIs, Datasets provide higher level abstractions and generic, reusable Java
implementations of common data patterns.

A Dataset represents both the API and the actual data itself; it is a named collection
of data with associated metadata, and it is manipulated through a Dataset class.


Types of Datasets
-----------------
A Dataset abstraction is defined with a Java class that implements the ``DatasetDefinition`` interface.
The implementation of a Dataset typically relies on one or more underlying (embedded) Datasets.
For example, the ``IndexedTable`` Dataset can be implemented by two underlying Table Datasets –
one holding the data and one holding the index.

We distinguish three categories of Datasets: *core*, *system*, and *custom* Datasets:

- The **core** Dataset of the Reactor is a Table. Its implementation may use internal
  Continuuity classes hidden from developers.

- A **system** Dataset is bundled with the Reactor and is built around
  one or more underlying core or system Datasets to implement a specific data pattern.

- A **custom** Dataset is implemented by you and can have arbitrary code and methods.
  It is typically built around one or more Tables (or other Datasets)
  to implement a specific data pattern.

Each Dataset is associated with exactly one Dataset implementation to
manipulate it. Every Dataset has a unique name and metadata that defines its behavior.
For example, every ``IndexedTable`` has a name and indexes a particular column of its primary table:
the name of that column is a metadata property of each Dataset of this type.


Core Datasets
-------------
**Tables** are the only core Datasets, and all other Datasets are built using one or more
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

The ``Row`` object provides access to the row data including its columns. If only a 
selection of row columns is requested, the returned ``Row`` object will contain only these columns.
The ``Row`` object provides an extensive API for accessing returned column values::

	// Get column value as a byte array
	byte[] value = row.get("column1");

	// Get column value of a specific type
	String valueAsString = row.getString("column1");
	Integer valueAsInteger = row.getInt("column1");

When requested, the value of a column is converted to a specific type automatically.
If the column is absent in a row, the returned value is ``null``. To return primitive types,
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

System Datasets
---------------
The Continuuity Reactor comes with several system-defined Datasets, including key/value Tables, 
indexed Tables and time series. Each of them is defined with the help of one or more embedded 
Tables, but defines its own interface. For example:

- The ``KeyValueTable`` implements a key/value store as a Table with a single column.

- The ``IndexedTable`` implements a Table with a secondary key using two embedded Tables,
  one for the data and one for the secondary index.

- The ``TimeseriesTable`` uses a Table to store keyed data over time
  and allows querying that data over ranges of time.

See the `Javadocs <javadocs/index.html>`__ for these classes and `the examples <examples/index.html>`__
to learn more about these Datasets.

Custom Datasets
---------------
You can define your own Dataset classes to implement common data patterns specific to your code.

Suppose you want to define a counter table that, in addition to counting words,
counts how many unique words it has seen. The Dataset can be built on top two underlying Datasets,
a first Table (``entryCountTable``) to count all the words and a second Table (``uniqueCountTable``) for the unique count.

When your custom Dataset is built on top of one or more existing Datasets, the simplest way to implement
it is to just define the data operations (by implementing the Dataset interface) and delegating all other
work (such as  administrative operations) to the embedded Dataset.

To do this, you need to implement the Dataset class and define the embedded Datasets by annotating
its constructor parameters.

In this case, our  ``UniqueCountTableDefinition`` will have two underlying Datasets:
an ``entryCountTable`` and an ``uniqueCountTable``, both of type ``Table``::

  public class UniqueCountTable extends AbstractDataset {

    private final Table entryCountTable;
    private final Table uniqueCountTable;

    public UniqueCountTable(DatasetSpecification spec,
                            @EmbeddedDataset("entryCountTable") Table entryCountTable,
                            @EmbeddedDataset("uniqueCountTable") Table uniqueCountTable) {
      super(spec.getName(), entryCountTable, uniqueCountTable);
      this.entryCountTable = entryCountTable;
      this.uniqueCountTable = uniqueCountTable;
    }

In this case, the class must have one constructor that takes a ``DatasetSpecification`` as a first
parameter and any number of ``Dataset``\s annotated with the ``@EmbeddedDataset`` annotation as the
remaining parameters. ``@EmbeddedDataset`` takes the embedded Dataset's name as a parameter.

The ``UniqueCountTable`` stores a counter for each word in its own row of the entry count table.
For each word the counter is incremented. If the result of the increment is 1, then this is the first time
we've encountered that word, hence we have a new unique word and we then increment the unique counter::

    public void updateUniqueCount(String entry) {
      long newCount = entryCountTable.increment(new Increment(entry, "count", 1L)).getInt("count");
      if (newCount == 1L) {
        uniqueCountTable.increment(new Increment("unique_count", "count", 1L));
      }
    }

Finally, we write a method to retrieve the number of unique words seen::

    public Long readUniqueCount() {
      return uniqueCountTable.get(new Get("unique_count", "count")).getLong("count");
    }


All administrative operations (such as create, drop, truncate) will be delegated to the embedded Datasets
in the order they are defined in the constructor. ``DatasetProperties`` that are passed during creation of
the Dataset will be passed as-is to the embedded Datasets.

To create a Dataset of ``UniqueCountTable`` type add the following into the Application implementation::

  Class MyApp extends AbstractApplication {
    public void configure() {
      createDataset("myCounters", UniqueCountTable.class)
      ...
    }
  }

You can also pass ``DatasetProperties`` as a third parameter to the ``createDataset`` method.
These properties will be used by embedded Datasets during creation and will be availalbe via ``DatasetSpecification``
passed to Dataset constructor.

Application components can access created Dataset via ``@UseDataSet``::

  Class MyFowlet extends AbstractFlowlet {
    @UseDataSet("myCounters")
    private UniqueCountTable counters;
    ...
  }

A complete application demonstrating the use of a custom Dataset is included in our
`PageViewAnalytics </examples/PageViewAnalytics/index.html>`__ example.

You can also create/drop/truncate Datasets using `Continuuity Reactor HTTP REST API <rest.html>`__. Please refer to the
REST APIs guide for more details on how to do that.


Datasets & MapReduce
--------------------

A MapReduce job can interact with a Dataset by using it as an input or an output.
The Dataset needs to implement specific interfaces to support this.

When you run a MapReduce job, you can configure it to read its input from a Dataset. The 
source Dataset must implement the ``BatchReadable`` interface, which requires two methods::

	public interface BatchReadable<KEY, VALUE> {
	  List<Split> getSplits();
	  SplitReader<KEY, VALUE> createSplitReader(Split split);
	}

These two methods complement each other: ``getSplits()`` must return all splits of the Dataset 
that the MapReduce job will read; ``createSplitReader()`` is then called in every Mapper to 
read one of the splits. Note that the ``KEY`` and ``VALUE`` type parameters of the split reader 
must match the input key and value type parameters of the Mapper.

Because ``getSplits()`` has no arguments, it will typically create splits that cover the 
entire Dataset. If you want to use a custom selection of the input data, define another 
method in your Dataset with additional parameters and explicitly set the input in the 
``beforeSubmit()`` method.

For example, the system Dataset ``KeyValueTable`` implements ``BatchReadable<byte[], byte[]>`` 
with an extra method that allows specification of the number of splits and a range of keys::

	public class KeyValueTable extends AbstractDataset
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

Similarly to reading input from a Dataset, you have the option to write to a Dataset as 
the output destination of a MapReduce job—if that Dataset implements the ``BatchWritable`` 
interface::

	public interface BatchWritable<KEY, VALUE> {
	  void write(KEY key, VALUE value);
	}

The ``write()`` method is used to redirect all writes performed by a Reducer to the Dataset.
Again, the ``KEY`` and ``VALUE`` type parameters must match the output key and value type 
parameters of the Reducer.


Transaction System
==================

The Need for Transactions
-------------------------

A Flowlet processes the data objects received on its inputs one at a time. While processing 
a single input object, all operations, including the removal of the data from the input, 
and emission of data to the outputs, are executed in a **transaction**. This provides us 
with ACID—atomicity, consistency, isolation, and durability properties:

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

The Continuuity Reactor uses *Optimistic Concurrency Control* (OCC) to implement 
transactions. Unlike most relational databases that use locks to prevent conflicting 
operations between transactions, under OCC we allow these conflicting writes to happen. 
When the transaction is committed, we can detect whether it has any conflicts: namely, if 
during the lifetime of the transaction, another transaction committed a write for one of 
the same keys that the transaction has written. In that case, the transaction is aborted 
and all of its writes are rolled back.

In other words: If two overlapping transactions modify the same row, then the transaction 
that commits first will succeed, but the transaction that commits last is rolled back due 
to a write conflict.

Optimistic Concurrency Control is lockless and therefore avoids problems such as idle 
processes waiting for locks, or even worse, deadlocks. However, it comes at the cost of 
rollback in case of write conflicts. We can only achieve high throughput with OCC if the 
number of conflicts is small. It is therefore a good practice to reduce the probability of 
conflicts wherever possible.

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

Keeping these guidelines in mind will help you write more efficient and faster-performing 
code.


The Need for Disabling Transactions
-----------------------------------
Transactions providing ACID (atomicity, consistency, isolation, and durability) guarantees 
are useful in several applications where data accuracy is critical—examples include billing 
applications and computing click-through rates.

However, some applications—such as trending—might not need it. Applications that do not 
strictly require accuracy can trade off accuracy against increased throughput by taking 
advantage of not having to write/read all the data in a transaction.

Disabling Transactions
----------------------
Transaction can be disabled for a Flow by annotating the Flow class with the 
``@DisableTransaction`` annotation::

	@DisableTransaction
	class MyExampleFlow implements Flow {
	  ...
	}

While this may speed up performance, if—for example—a Flowlet fails, the system would not 
be able to roll back to its previous state. You will need to judge whether the increase in 
performance offsets the increased risk of inaccurate data.

Transactions in MapReduce
-------------------------
When you run a MapReduce job that interacts with Datasets, the system creates a 
long-running transaction. Similar to the transaction of a Flowlet or a Procedure, here are 
some rules to follow:

- Reads can only see the writes of other transactions that were committed
  at the time the long-running transaction was started.

- All writes of the long-running transaction are committed atomically,
  and only become visible to others after they are committed.

- The long-running transaction can read its own writes.

However, there is a key difference: long-running transactions do not participate in 
conflict detection. If another transaction overlaps with the long-running transaction and 
writes to the same row, it will not cause a conflict but simply overwrite it.

It is not efficient to fail the long-running job based on a single conflict. Because of 
this, it is not recommended to write to the same Dataset from both real-time and MapReduce 
programs. It is better to use different Datasets, or at least ensure that the real-time 
processing writes to a disjoint set of columns.

It's important to note that the MapReduce framework will reattempt a task (Mapper or 
Reducer) if it fails. If the task is writing to a Dataset, the reattempt of the task will 
most likely repeat the writes that were already performed in the failed attempt. Therefore 
it is highly advisable that all writes performed by MapReduce programs be idempotent.

Best Practices for Developing Applications
==========================================

Initializing Instance Fields
----------------------------
There are three ways to initialize instance fields used in Flowlets and Procedures:

#. Using the default constructor;
#. Using the ``initialize()`` method of the Flowlets and Procedures; and
#. Using ``@Property`` annotations.

To initialize using an Property annotation, simply annotate the field definition with 
``@Property``. 

The following example demonstrates the convenience of using ``@Property`` in a 
``WordFilter`` flowlet
that filters out specific words::

	public static class WordFilter extends AbstractFlowlet {
	
	  private OutputEmitter<String> out;
	
	  @Property
	  private final String toFilterOut;
	
	  public CountByField(String toFilterOut) {
	    this.toFilterOut = toFilterOut;
	  }
	
	  @ProcessInput()
	  public void process(String word) {
	    if (!toFilterOut.equals(word)) {
	      out.emit(word);
	    }
	  }
	}


The Flowlet constructor is called with the parameter when the Flow is configured::

  public static class WordCountFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("WordCountFlow")
        .setDescription("Flow for counting words")
        .withFlowlets().add(new Tokenizer())
                       .add(new WordsFilter("the"))
                       .add(new WordsCounter())
        .connect().fromStream("text").to("Tokenizer")
                  .from("Tokenizer").to("WordsFilter")
                  .from("WordsFilter").to("WordsCounter")
        .build();
    }
  }


At run-time, when the Flowlet is started, a value is injected into the ``toFilterOut`` 
field.

Field types that are supported using the ``@Property`` annotation are primitives,
boxed types (e.g. ``Integer``), ``String`` and ``enum``.

Where to Go Next
================
Now that you've looked at the advanced features of Continuuity Reactor, take a look at:

- `Querying Datasets with SQL <query.html>`__,
  which covers ad-hoc querying of Continuuity Reactor Datasets using SQL.
