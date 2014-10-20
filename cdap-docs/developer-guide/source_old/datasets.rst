.. :author: Cask Data, Inc.
   :description: placeholder
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Datasets
============================================

**Datasets** store and retrieve data. Datasets are your means of reading
from and writing to the CDAP’s storage capabilities. Instead of
forcing you to manipulate data with low-level APIs, Datasets provide
higher-level abstractions and generic, reusable implementations of
common data patterns.

The core Dataset of the CDAP is a Table. Unlike relational database
systems, these tables are not organized into rows with a fixed schema.
They are optimized for efficient storage of semi-structured data, data
with unknown or variable schema, or sparse data.

Other Datasets are built on top of Tables. A Dataset can implement
specific semantics around a Table, such as a key/value Table or a
counter Table. A Dataset can also combine multiple Datasets to create a
complex data pattern. For example, an indexed Table can be implemented
by using one Table for the data and a second Table for the index of that data.

A number of useful Datasets—we refer to them as system Datasets—are
included with CDAP, including key/value tables, indexed tables and
time series. You can implement your own data patterns as custom
Datasets on top of Tables.

You can create a Dataset in CDAP using either the
:ref:`Cask Data Application Platform HTTP RESTful API <rest-datasets>` or command line tools.

You can also tell Applications to create a Dataset if it does not already
exist by declaring the Dataset details in the Application specification.
For example, to create a DataSet named *myCounters* of type 
`KeyValueTable <javadocs/co/cask/cdap/api/dataset/lib/KeyValueTable.html>`__, write::

  public void configure() {
      createDataset("myCounters", KeyValueTable.class);
      ...

To use the Dataset in a Program, instruct the runtime
system to inject an instance of the Dataset with the ``@UseDataSet``
annotation::

  class MyFlowlet extends AbstractFlowlet {
    @UseDataSet("myCounters")
    private KeyValueTable counters;
    ...
    void process(String key) {
      counters.increment(key.getBytes(), 1L);
    }

The runtime system reads the Dataset specification for the key/value
table *myCounters* from the metadata store and injects an
instance of the Dataset class into the Application.

You can also implement custom Datasets by implementing the ``Dataset``
interface or by extending existing Dataset types. See the
:ref:`Purchase Example<purchase>` for an implementation of a Custom Dataset.
For more details, refer to :ref:`Custom Datasets <custom-datasets>`

Types of Datasets
-----------------
A Dataset abstraction is defined by a Java class that implements the ``DatasetDefinition`` interface.
The implementation of a Dataset typically relies on one or more underlying (embedded) Datasets.
For example, the ``IndexedTable`` Dataset can be implemented by two underlying Table Datasets –
one holding the data and one holding the index.

We distinguish three categories of Datasets: *core*, *system*, and *custom* Datasets:

- The **core** Dataset of the CDAP is a Table. Its implementation may use internal
  CDAP classes hidden from developers.

- A **system** Dataset is bundled with the CDAP and is built around
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
Tables. These Tables are similar to tables in a relational database with a few key differences:

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

  // Increment and return result
  public Row incrementAndGet(Increment increment)
  public long incrementAndGet(byte[] row, byte[] column, long amount)
  public Row incrementAndGet(byte[] row, byte[][] columns, long[] amounts)

  // Increment without result
  public void increment(Increment increment)
  public void increment(byte[] row, byte[] column, long amount)
  public void increment(byte[] row, byte[][] columns, long[] amounts)

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
A ``get`` operation reads all columns or a selection of columns of a single row::

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
the corresponding methods accept a default value to be returned when the column is absent::

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
or an integer amount *n*.  If a column does not exist, it is created with an assumed value of zero
before the increment is applied::

  // Write long value to a column of a row
  t.put(new Put("rowKey1").add("column1", 55L));
  // Increment values of several columns in a row
  t.increment(new Increment("rowKey1").add("column1", 1L).add("column2", 23L));

If the existing value of the column cannot be converted to a ``long``,
a ``NumberFormatException`` will be thrown.

Two types of increment operations are supported:

- ``incrementAndGet(...)`` operations will increment the currently stored value and return the
  result; and
- ``increment(...)`` operations will increment the currently stored value without any return
  value.

*Read-less Increments*

By default, an increment operation will need to first perform a read operation to find the
currently stored column value, apply the increment to the stored value, and then write the final
result.  For high write volume workloads, with only occassional reads, this can impose a great
deal of unnecessary overhead for increments.

In these situations, you can configure the dataset to support read-less increments.  With read-less
increments, each operation only performs a write operation, storing the incremental value for the
column in a new cell.  This completely eliminates the cost of the read operation when performing
increments.  Instead, when reading the value for a column storing data for read-less increments,
all of the stored increment values are read and summed up together with the last stored complete
sum, in order to compute the final result.  As a result, read operations become more expensive, but
this trade-off can be very beneficial for workloads dominated by writes.

Read-less increments can only be used with the ``increment(...)`` operation, since it does not
return a value.  To configure a dataset to support read-less increments:

1. Set the property ``dataset.table.readless.increment`` to ``true`` in the DatasetSpecification
   properties.
2. Use the ``increment(...)`` methods for any operations that do not need the result value of the
   increment operation.

*Note:* the current implementation of read-less increments uses an HBase coprocessor to prefix the
stored values for incremental updates with a special prefix.  Since this prefix could occur
naturally in other stored data values, it is highly recommended that increments be stored in a
separate dataset and not be mixed in with other types of values.  This will ensure that other data is
not mis-identified as a stored increment and prevent incorrect results.

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
The Cask Data Application Platform comes with several system-defined Datasets, including but not limited to
key/value Tables, indexed Tables and time series. Each of them is defined with the help of one or more embedded
Tables, but defines its own interface. Examples include:

- The ``KeyValueTable`` implements a key/value store as a Table with a single column.

- The ``IndexedTable`` implements a Table with a secondary key using two embedded Tables,
  one for the data and one for the secondary index.

- The ``TimeseriesTable`` uses a Table to store keyed data over time
  and allows querying that data over ranges of time.

See the :doc:`Javadocs <javadocs/index>` for these classes and the :ref:`Examples <examples>`
to learn more about these Datasets. Any class in the CDAP libraries that implements the ``Dataset`` interface is a
system Dataset.

.. _custom-datasets:

Custom Datasets
---------------
You can define your own Dataset classes to implement common data patterns specific to your code.

Suppose you want to define a counter table that, in addition to counting words,
counts how many unique words it has seen. The Dataset can be built on top of two underlying Datasets. The first a
Table (``entryCountTable``) to count all the words and the second a Table (``uniqueCountTable``) for the unique count.

When your custom Dataset is built on top of one or more existing Datasets, the simplest way to implement
it is to just define the data operations (by implementing the Dataset interface) and delegating all other
work (such as  administrative operations) to the embedded Dataset.

To do this, you need to implement the Dataset class and define the embedded Datasets by annotating
its constructor arguments.

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
      long newCount = entryCountTable.incrementAndGet(new Increment(entry, "count", 1L)).getInt("count");
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

To create a Dataset of type ``UniqueCountTable``, add the following into the Application implementation::

  Class MyApp extends AbstractApplication {
    public void configure() {
      createDataset("myCounters", UniqueCountTable.class)
      ...
    }
  }

You can also pass ``DatasetProperties`` as a third parameter to the ``createDataset`` method.
These properties will be used by embedded Datasets during creation and will be available via the
``DatasetSpecification`` passed to the Dataset constructor.

Application components can access a created Dataset via the ``@UseDataSet`` annotation::

  Class MyFlowlet extends AbstractFlowlet {
    @UseDataSet("myCounters")
    private UniqueCountTable counters;
    ...
  }

A complete application demonstrating the use of a custom Dataset is included in our
:ref:`Purchase Example<purchase>`.

You can also create, drop, and truncate Datasets using the
:ref:`Cask Data Application Platform HTTP REST API <rest-datasets>`.

Datasets and MapReduce
----------------------

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

Just as you have the option to read input from a Dataset, you have the option to write to a Dataset as
the output destination of a MapReduce job if that Dataset implements the ``BatchWritable``
interface::

  public interface BatchWritable<KEY, VALUE> {
    void write(KEY key, VALUE value);
  }

The ``write()`` method is used to redirect all writes performed by a Reducer to the Dataset.
Again, the ``KEY`` and ``VALUE`` type parameters must match the output key and value type
parameters of the Reducer.