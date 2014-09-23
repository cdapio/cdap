.. :author: Cask Data, Inc.
   :description: Introduction to Programming Applications for the Cask Data Application Platform
   :copyright: Copyright © 2014 Cask Data, Inc.

================================================
Cask Data Application Platform Programming Guide
================================================

Introduction
============

This document covers the two core virtualizations in the Cask Data Application Platform (CDAP) — Data and Applications.
Data virtualizations are grouped into streams and datasets. Application virtualizations are grouped into
Flows, MapReduce, Spark, Workflows, and Services. This document
details how to work with these abstractions to build Big Data applications.

For a high-level view of the concepts of the Cask Data Application Platform,
please see :doc:`Concepts and Architecture </arch>`.

For more information beyond this document, see the
:doc:`Javadocs </javadocs/index>` and the code in the
:ref:`Examples <examples>` directory, both of which are on the
`Cask.co <http://cask.co>`_ `Developers website <http://cask.co/developers>`_ as well as in your
CDAP installation directory.

Data Virtualization
===================

There are two main data virtualizations: Streams and Datasets. Streams are ordered, partitioned
sequences of data, and are the primary means of bringing data from external systems into the CDAP
in realtime. Datasets are abstractions on top of data, allowing you to access your data using
higher-level abstractions and generic, reusable Java implementations of common data patterns
instead of requiring you to manipulate data with low-level APIs.

.. _streams:

Streams
=======

**Streams** are the primary means of bringing data from external systems into the CDAP in realtime.
They are ordered, time-partitioned sequences of data, usable for realtime collection and consumption of data.
You specify a Stream in your :ref:`Application <applications>` specification::

  addStream(new Stream("myStream"));

specifies a new Stream named *myStream*. Names used for Streams need to
be unique across the CDAP instance.

You can write to Streams either one operation at a time or in batches,
using either the :ref:`Cask Data Application Platform HTTP RESTful API <rest-streams>`
or command line tools.

Each individual signal sent to a Stream is stored as a ``StreamEvent``,
which is comprised of a header (a map of strings for metadata) and a
body (a blob of arbitrary binary data).

Streams are uniquely identified by an ID string (a "name") and are
explicitly created before being used. They can be created
programmatically within your application, through the CDAP Console,
or by or using a command line tool. Data written to a Stream
can be consumed in real-time by Flows or in batch by MapReduce. Streams are shared
between applications, so they require a unique name.

.. _Datasets:

Datasets
========

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
:doc:`KeyValueTable <javadocs/co/cask/cdap/api/dataset/lib/KeyValueTable>`, write::

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
:doc:`Purchase <examples/purchase>`
example for an implementation of a Custom Dataset.
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
or an integer amount *n*.
If a column does not exist, it is created with an assumed value of zero before the increment::

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
:doc:`Purchase <examples/purchase>` example.

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

Data Exploration
================

It is often useful to be able to explore a Dataset in an ad-hoc manner.
This can be done using SQL if your Dataset fulfills two requirements:

* it defines the schema for each record; and
* it has a method to scan its data record by record.

For CDAP Datasets, this is done by implementing the ``RecordScannable`` interface.
The CDAP built-in Dataset ``KeyValueTable`` already implements this and can be used for ad-hoc queries.

Let's take a closer look at the ``RecordScannable`` interface.

Defining the Record Schema
--------------------------
The record schema is given by returning the Java type of each record, and CDAP will derive the record schema from
that type::

  Type getRecordType();

For example, suppose you have a class ``Entry`` defined as::

  class Entry {
    private final String key;
    private final int value;
    ...
  }

You can implement a record-scannable Dataset that uses ``Entry`` as the record type::

  class MyDataset ... implements RecordScannable<Entry> {
    ...
    public Type getRecordType() {
      return Entry.class;
    }

Note that Java's ``Class`` implements ``Type`` and you can simply return ``Entry.class`` as the record type.
CDAP will use reflection to infer a SQL-style schema from the record type.

In the case of the above class ``Entry``, the schema will be::

  (key STRING, value INT)

.. _sql-limitations:

Limitations
-----------
* The record type must be a structured type, that is, a Java class with fields. This is because SQL tables require
  a structure type at the top level. This means the record type cannot be a primitive,
  collection or map type. However, these types may appear nested inside the record type.

* The record type must be that of an actual Java class, not an interface. The same applies to the types of any
  fields contained in the type. The reason is that interfaces only define methods but not fields; hence, reflection
  would not be able to derive any fields or types from the interface.

  The one exception to this rule is that Java collections such as ``List`` and ``Set`` are supported as well as
  Java ``Map``. This is possible because these interfaces are so commonly used that they deserve special handling.
  These interfaces are parameterized and require special care as described in the next section.

* The record type must not be recursive. In other words, it cannot contain any class that directly or indirectly
  contains a member of that same class. This is because a recursive type cannot be represented as a SQL schema.

* Fields of a class that are declared static or transient are ignored during schema generation. This means that the
  record type must have at least one non-transient and non-static field. For example,
  the ``java.util.Date`` class has only static and transient fields. Therefore a record type of ``Date`` is not
  supported and will result in an exception when the Dataset is created.

* A Dataset can only be used in ad-hoc queries if its record type is completely contained in the Dataset definition.
  This means that if the record type is or contains a parameterized type, then the type parameters must be present in
  the Dataset definition. The reason is that the record type must be instantiated when executing an ad-hoc query.
  If a type parameter depends on the jar file of the application that created the Dataset, then this jar file is not
  available to the query execution runtime.

  For example, you cannot execute ad-hoc queries over an ``ObjectStore<MyObject>`` if the ``MyObject`` is contained in
  the application jar. However, if you define your own Dataset type ``MyObjectStore`` that extends or encapsulates an
  ``ObjectStore<MyObject>``, then ``MyObject`` becomes part of the Dataset definition for ``MyObjectStore``. See the
  :doc:`Purchase </examples/purchase>` application for an example.


Parameterized Types
-------------------
Suppose instead of being fixed to ``String`` and ``int``, the ``Entry`` class is generic with type parameters for both
key and value::

  class GenericEntry<KEY, VALUE> {
    private final KEY key;
    private final VALUE value;
    ...
  }

We should easily be able to implement ``RecordScannable<GenericEntry<String, Integer>>`` by defining ``getRecordType()``.
However, due to Java's runtime type erasure, returning ``GenericEntry.class`` does not convey complete information
about the record type. With reflection, CDAP can only determine the names of the two fields, but not their types.

To convey information about the type parameters, we must instead return a ``ParameterizedType``, which Java's
``Class`` does not implement. An easy way is to use Guava's ``TypeToken``::

  class MyDataset ... implements RecordScannable<GenericEntry<String, Integer>>
    public Type getRecordType() {
      return new TypeToken<GenericEntry<String, Integer>>() { }.getType();
    }

While this seems a little more complex at first sight, it is the de-facto standard way of dealing with Java type
erasure.

Complex Types
-------------
Your record type can also contain nested structures, lists, or maps, and they will be mapped to type names as defined in
the `Hive language manual <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL>`_. For example, if
your record type is defined as::

  class Movie {
    String title;
    int year;
    Map<String, String> cast;
    List<String> reviews;
  }

The SQL schema of the dataset would be::

  (title STRING, year INT, cast MAP<STRING, STRING>, reviews ARRAY<STRING>)

Refer to the Hive language manual for more details on schema and data types.

.. _sql-scanning-records:

Scanning Records
----------------
The second requirement for enabling SQL queries over a Dataset is to provide a means of scanning the Dataset record
by record. Similar to how the ``BatchReadable`` interface makes Datasets readable by Map/Reduce jobs by iterating
over pairs of key and value, ``RecordScannable`` iterates over records. You need to implement a method to partition the
Dataset into splits, and an additional method to create a record scanner for each split::

      List<Split> getSplits();
      RecordScanner<RECORD> createSplitRecordScanner(Split split);

The ``RecordScanner`` is very similar to a ``SplitReader``; except that instead of ``nextKeyValue()``,
``getCurrentKey()``, and ``getCurrentValue()``, it implements ``nextRecord()`` and ``getCurrentRecord()``.

Typically, you do not implement these methods from scratch but rely on the ``BatchReadable``
implementation of the underlying Tables and Datasets. For example, if your Dataset is backed by a ``Table``::

  class MyDataset implements Dataset, RecordScannable<Entry> {

    private Table table;
    private static final byte[] VALUE_COLUMN = { 'c' };

    // ..
    // All other Dataset methods
    // ...

    @Override
    public Type getRecordType() {
      return Entry.class;
    }

    @Override
    public List<Split> getSplits() {
      return table.getSplits();
    }

    @Override
    public RecordScanner<Entry> createSplitRecordScanner(Split split) {

      final SplitReader<byte[], Row> reader = table.createSplitReader(split);

      return new RecordScanner<Entry>() {
        @Override
        public void initialize(Split split) {
          reader.initialize(split);
        }

        @Override
        public boolean nextRecord() {
          return reader.nextKeyValue();
        }

        @Override
        public Entry getCurrentRecord()  {
          return new Entry(
            Bytes.toString(reader.getCurrentKey()),
            reader.getCurrentValue().getInt(VALUE_COLUMN));
        }

        @Override
        public void close() {
          reader.close();
        }

      }
    }
  }

While this is straightforward, it is even easier if your Dataset already implements ``BatchReadable``.
In that case, you can reuse its implementation of ``getSplits()`` and implement the split record scanner
with a helper method
(``Scannables.splitRecordScanner``) already defined by CDAP. It takes a split reader and a ``RecordMaker``
that transforms a key and value, as produced by the ``BatchReadable``'s split reader,
into a record::

  @Override
  public RecordScanner<Entry> createSplitRecordScanner(Split split) {
    return Scannables.splitRecordScanner(
      table.createSplitReader(split),
      new Scannables.RecordMaker<byte[], Row, Entry>() {
        @Override
        public Entry makeRecord(byte[] key, Row row) {
          return new Entry(Bytes.toString(key), row.getInt(VALUE_COLUMN));
        }
      });
  }

Note there is an even simpler helper (``Scannables.valueRecordScanner``) that derives a split
record scanner from a split reader. For each key and value returned by the split reader it ignores the key
and returns the value. For example,
if your dataset implements ``BatchReadable<String, Employee>``, then you can implement ``RecordScannable<Employee>`` by
defining::

  @Override
  public RecordScanner<Employee> createSplitRecordScanner(Split split) {
    return Scannables.valueRecordScanner(table.createSplitReader(split));
  }

An example demonstrating an implementation of ``RecordScannable`` is included in the Cask Data Application Platform SDK in the
directory ``examples/Purchase``, namely the ``PurchaseHistoryStore``.

Writing to Datasets with SQL
----------------------------
Data can be inserted into Datasets using SQL. For example, you can write to a Dataset named
``ProductCatalog`` with this SQL query::

  INSERT INTO TABLE cdap_user_productcatalog SELECT ...

In order for a Dataset to enable record insertion from SQL query, it simply has to expose a way to write records
into itself.

For CDAP Datasets, this is done by implementing the ``RecordWritable`` interface.
The system Dataset KeyValueTable already implements this and can be used to insert records from SQL queries.

Let's take a closer look at the ``RecordWritable`` interface.

Defining the Record Schema
..........................

Just like in the ``RecordScannable`` interface, the record schema is given by returning the Java type of each record,
using the method::

  Type getRecordType();

:ref:`The same rules <sql-limitations>` that apply to the type of the ``RecordScannable`` interface apply
to the type of the ``RecordWritable`` interface. In fact, if a Dataset implements both ``RecordScannable`` and
``RecordWritable`` interfaces, they will have to use identical record types.

Writing Records
...............

To enable inserting SQL query results, a Dataset needs to provide a means of writing a record into itself.
This is similar to how the ``BatchWritable`` interface makes Datasets writable from MapReduce jobs by providing
a way to write pairs of key and value. You need to implement the ``RecordWritable`` method::

      void write(RECORD record) throws IOException;

Continuing the *MyDataset* :ref:`example used above <sql-scanning-records>`, which showed an implementation of
``RecordScannable``, this example an implementation of a ``RecordWritable`` Dataset that is backed by a ``Table``::

  class MyDataset implements Dataset, ..., RecordWritable<Entry> {

    private Table table;
    private static final byte[] VALUE_COLUMN = { 'c' };

    // ..
    // All other Dataset methods
    // ...

    @Override
    public Type getRecordType() {
      return Entry.class;
    }

    @Override
    public void write(Entry record) throws IOException {
      return table.put(Bytes.toBytes(record.getKey()), VALUE_COLUMN, Bytes.toBytes(record.getValue()));
    }
  }

Note that a Dataset can implement either ``RecordScannable``, ``RecordWritable``, or both.

Connecting to CDAP Datasets using CDAP JDBC driver
--------------------------------------------------
CDAP provides a JDBC driver to make integrations with external programs and third-party BI (business intelligence)
tools easier.

The JDBC driver is a JAR that is bundled with the CDAP SDK. You can find it in the ``lib``
directory of your SDK installation at ``lib/co.cask.cdap.cdap-explore-jdbc-<version>.jar``.

If you don't have a CDAP SDK and only want to connect to an existing instance of CDAP, you can download the CDAP JDBC
driver using this `link <https://repository.continuuity.com/content/repositories/releases-public/co/cask/cdap/cdap-explore-jdbc/>`__.
Go to the directory matching the version of your running CDAP instance, and download the file named ``cdap-explore-jdbc-<version>.jar``.

Using the CDAP JDBC driver in your Java code
............................................

To use CDAP JDBC driver in your code, place ``cdap-jdbc-driver.jar`` in the classpath of your application.
If you are using Maven, you can simply add a dependency in your file ``pom.xml``::

  <dependencies>
    ...
    <dependency>
      <groupId>co.cask.cdap</groupId>
      <artifactId>cdap-explore-jdbc</artifactId>
      <version><!-- Version of CDAP you want the JDBC driver to query --></version>
    </dependency>
    ...
  </dependencies>

Here is a snippet of Java code that uses the CDAP JDBC driver to connect to a running instance of CDAP,
and executes a query over CDAP Datasets::

  // First, register the driver once in your application
  Class.forName("co.cask.cdap.explore.jdbc.ExploreDriver");

  // If your CDAP instance requires a authorization token for connection,
  // you have to specify it here.
  // Replace <cdap-host> and <authorization_token> as appropriate to your installation.
  String connectionUrl = "jdbc:cdap://<cdap-host>:10000" +
    "?auth.token=<authorization_token>";

  // Connect to CDAP instance
  Connection connection = DriverManager.getConnection(connectionUrl);

  // Execute a query over CDAP Datasets and retrieve the results
  ResultSet resultSet = connection.prepareStatement("select * from cdap_user_mydataset").executeQuery();
  ...

JDBC drivers are a standard in the Java ecosystem, with many `resources about them available
<http://docs.oracle.com/javase/tutorial/jdbc/>`__.

Accessing CDAP Datasets through Business Intelligence Tools
...........................................................

Most Business Intelligence tools can integrate with relational databases using JDBC drivers. They often include
drivers to connect to standard databases such as MySQL or PostgreSQL.
Most tools allow the addition of non-standard JDBC drivers.

We'll look at two business intelligence tools — *SquirrelSQL* and *Pentaho Data Integration* —
and see how to connect them to a running CDAP instance and interact with CDAP Datasets.

SquirrelSQL
...........

*SquirrelSQL* is a simple JDBC client which executes SQL queries against many different relational databases.
Here's how to add the CDAP JDBC driver inside *SquirrelSQL*.

#. Open the ``Drivers`` pane, located on the far left corner of *SquirrelSQL*.
#. Click the ``+`` icon of the ``Drivers`` pane.

   .. image:: _images/jdbc/squirrel_drivers.png
      :width: 4in

#. Add a new Driver by entering a ``Name``, such as ``CDAP Driver``. The ``Example URL`` is of the form
   ``jdbc:cdap://<host>:10000?auth.token=<token>``. The ``Website URL`` can be left blank. In the ``Class Name``
   field, enter ``co.cask.cdap.explore.jdbc.ExploreDriver``.
   Click on the ``Extra Class Path`` tab, then on ``Add``, and put the path to ``co.cask.cdap.cdap-explore-jdbc-<version>.jar``.

   .. image:: _images/jdbc/squirrel_add_driver.png
      :width: 6in

#. Click on ``OK``. You should now see ``Cask CDAP Driver`` in the list of drivers from the ``Drivers`` pane of
   *SquirrelSQL*.
#. We can now create an alias to connect to a running instance of CDAP. Open the ``Aliases`` pane, and click on
   the ``+`` icon to create a new alias.
#. In this example, we are going to connect to a standalone CDAP from the SDK.
   The name of our alias will be ``CDAP Standalone``. Select the ``CDAP Driver`` in
   the list of available drivers. Our URL will be ``jdbc:cdap://localhost:10000``. Our standalone instance
   does not require an authorization token, but if yours requires one, HTML-encode your token
   and pass it as a parameter of the ``URL``. ``User Name`` and ``Password`` are left blank.

   .. image:: _images/jdbc/squirrel_add_alias.png
      :width: 6in

#. Click on ``OK``. ``CDAP Standalone`` is now added to the list of aliases.
#. A popup asks you to connect to your newly-added alias. Click on ``Connect``, and *SquirrelSQL* will retrieve
   information about your running CDAP Datasets.
#. To execute a SQL query on your CDAP Datasets, go to the ``SQL`` tab, enter a query in the center field, and click
   on the "running man" icon on top of the tab. Your results will show in the bottom half of the *SquirrelSQL* main view.

   .. image:: _images/jdbc/squirrel_sql_query.png
      :width: 6in

Pentaho Data Integration
........................

*Pentaho Data Integration* is an advanced, open source business intelligence tool that can execute
transformations of data coming from various sources. Let's see how to connect it to
CDAP Datasets using the CDAP JDBC driver.

#. Before opening the *Pentaho Data Integration* application, copy the ``co.cask.cdap.cdap-explore-jdbc-<version>.jar``
   file to the ``lib`` directory of *Pentaho Data Integration*, located at the root of the application's directory.
#. Open *Pentaho Data Integration*.
#. In the toolbar, select ``File -> New -> Database Connection...``.
#. In the ``General`` section, select a ``Connection Name``, like ``CDAP Standalone``. For the ``Connection Type``, select
   ``Generic database``. Select ``Native (JDBC)`` for the ``Access`` field. In this example, where we connect to
   a standalone instance of CDAP, our ``Custom Connection URL`` will then be ``jdbc:cdap://localhost:10000``.
   In the field ``Custom Driver Class Name``, enter ``co.cask.cdap.explore.jdbc.ExploreDriver``.

   .. image:: _images/jdbc/pentaho_add_connection.png
      :width: 6in

#. Click on ``OK``.
#. To use this connection, navigate to the ``Design`` tab on the left of the main view. In the ``Input`` menu,
   double click on ``Table input``. It will create a new transformation containing this input.

   .. image:: _images/jdbc/pentaho_table_input.png
      :width: 6in

#. Right-click on ``Table input`` in your transformation and select ``Edit step``. You can specify an appropriate name
   for this input such as ``CDAP Datasets query``. Under ``Connection``, select the newly created database connection;
   in this example, ``CDAP Standalone``. Enter a valid SQL query in the main ``SQL`` field. This will define the data
   available to your transformation.

   .. image:: _images/jdbc/pentaho_modify_input.png
      :width: 6in

#. Click on ``OK``. Your input is now ready to be used in your transformation, and it will contain data coming
   from the results of the SQL query on the CDAP Datasets.
#. For more information on how to add components to a transformation and link them together, see the
   `Pentaho Data Integration page <http://community.pentaho.com/projects/data-integration/>`__.


Formulating Queries
-------------------
When creating your queries, keep these limitations in mind:

- The query syntax of CDAP is a subset of the variant of SQL that was first defined by Apache Hive.
- The SQL commands ``UPDATE`` and ``DELETE`` are not allowed on CDAP Datasets.
- When addressing your datasets in queries, you need to prefix the data set name with the CDAP
  namespace ``cdap_user_``. For example, if your Dataset is named ``ProductCatalog``, then the corresponding table
  name is ``cdap_user_productcatalog``. Note that the table name is lower-case.

For more examples of queries, please refer to the `Hive language manual
<https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-InsertingdataintoHiveTablesfromqueries>`__.



Application Virtualization
==========================

Applications are a virtualization on top of your data, hiding low-level details of individual
programming paradigms and runtimes, while providing access to many useful and powerful services provided 
by CDAP such as the ability to dynamically scale processing units, distributed transactions, and service
discovery. Applications are abstracted away from the platform that runs the application. 
When you deploy and run the application into a specific installation of CDAP, the appropriate
implementations of all services and program runtimes are injected by CDAP; the application does not need
to change based on the environment. This allows you develop applications in one environment - like on your laptop
using a stand-alone CDAP for testing - and then seamlessly deploy it in a different environment - like
your distributed staging cluster.

With your data virtualized in CDAP as Streams and Datasets, you are able to process that data in realtime or in batch
using Programs (Flows, MapReduce, Spark, Workflow), and you can serve data to external clients using Services
and Procedures.

.. _applications:

Applications
============

An **Application** is a collection of Programs, Services, and Procedures that read from and write to the data
virtualization layer in CDAP. Programs include `Flows`_, `MapReduce`_, `Workflows`_, and `Spark`_, and are used
to process data. Services and Procedures are used to serve data.

The CDAP API is written in a
`"fluent" interface style <http://en.wikipedia.org/wiki/Fluent_interface>`_,
and often relies on ``Builder`` methods for creating many parts of the Application.

In writing a CDAP Application, it's best to use an integrated
development environment that understands the application interface to
provide code-completion in writing interface methods.

To create an Application, implement the ``Application`` interface
or subclass from ``AbstractApplication`` class, specifying
the Application metadata and declaring and configuring each of the Application elements::

      public class MyApp extends AbstractApplication {
        @Override
        public void configure() {
          setName("myApp");
          setDescription("My Sample Application");
          addStream(new Stream("myAppStream"));
          addFlow(new MyAppFlow());
          addProcedure(new MyAppQuery());
          addMapReduce(new MyMapReduceJob());
          addWorkflow(new MyAppWorkflow());
        }
      }

Notice that *Streams* are
defined using provided ``Stream`` class, and are referenced by names, while
other components are defined using user-written
classes that implement correspondent interfaces and are referenced by passing
an object, in addition to being assigned a unique name.

Names used for *Streams* and *Datasets* need to be unique across the
CDAP instance, while names used for Programs and Services need to be unique only to the application.

.. _flows:

Flows
=====

**Flows** are user-implemented real-time stream processors. They are comprised of one or
more **Flowlets** that are wired together into a directed acyclic graph or DAG. Flowlets
pass data between one another; each Flowlet is able to perform custom logic and execute
data operations for each individual data object it processes. All data operations happen
in a consistent and durable way.

When processing a single input object, all operations, including the
removal of the object from the input, and emission of data to the
outputs, are executed in a transaction. This provides us with Atomicity,
Consistency, Isolation, and Durability (ACID) properties, and helps
assure a unique and core property of the Flow system: it guarantees
atomic and "exactly-once" processing of each input object by each
Flowlet in the DAG.

Flows are deployed to the CDAP instance and hosted within containers. Each
Flowlet instance runs in its own container. Each Flowlet in the DAG can
have multiple concurrent instances, each consuming a partition of the
Flowlet’s inputs.

To put data into your Flow, you can either connect the input of the Flow
to a Stream, or you can implement a Flowlet to generate or pull the data
from an external source.

The ``Flow`` interface allows you to specify the Flow’s metadata, `Flowlets`_,
`Flowlet connections <#connecting-flowlets>`_, `Stream to Flowlet connections <#connection>`_,
and any `Datasets`_ used in the Flow.

To create a Flow, implement ``Flow`` via a ``configure`` method that
returns a ``FlowSpecification`` using ``FlowSpecification.Builder()``::

  class MyExampleFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("mySampleFlow")
        .setDescription("Flow for showing examples")
        .withFlowlets()
          .add("flowlet1", new MyExampleFlowlet())
          .add("flowlet2", new MyExampleFlowlet2())
        .connect()
          .fromStream("myStream").to("flowlet1")
          .from("flowlet1").to("flowlet2")
        .build();
  }

In this example, the *name*, *description*, *with* (or *without*)
Flowlets, and *connections* are specified before building the Flow.

.. _flowlets:

Flowlets
--------

**Flowlets**, the basic building blocks of a Flow, represent each
individual processing node within a Flow. Flowlets consume data objects
from their inputs and execute custom logic on each data object, allowing
you to perform data operations as well as emit data objects to the
Flowlet’s outputs. Flowlets specify an ``initialize()`` method, which is
executed at the startup of each instance of a Flowlet before it receives
any data.

The example below shows a Flowlet that reads *Double* values, rounds
them, and emits the results. It has a simple configuration method and
doesn't do anything for initialization or destruction::

  class RoundingFlowlet implements Flowlet {

    @Override
    public FlowletSpecification configure() {
      return FlowletSpecification.Builder.with().
        setName("round").
        setDescription("A rounding Flowlet").
        build();
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
    }

    @Override
    public void destroy() {
    }

    OutputEmitter<Long> output;
    @ProcessInput
    public void round(Double number) {
      output.emit(Math.round(number));
    }


The most interesting method of this Flowlet is ``round()``, the method
that does the actual processing. It uses an output emitter to send data
to its output. This is the only way that a Flowlet can emit output to
another connected Flowlet::

  OutputEmitter<Long> output;
  @ProcessInput
  public void round(Double number) {
    output.emit(Math.round(number));
  }

Note that the Flowlet declares the output emitter but does not
initialize it. The Flow system initializes and injects its
implementation at runtime.

The method is annotated with ``@ProcessInput`` — this tells the Flow
system that this method can process input data.

You can overload the process method of a Flowlet by adding multiple
methods with different input types. When an input object comes in, the
Flowlet will call the method that matches the object’s type::

  OutputEmitter<Long> output;

  @ProcessInput
  public void round(Double number) {
    output.emit(Math.round(number));
  }
  @ProcessInput
  public void round(Float number) {
    output.emit((long)Math.round(number));
  }

If you define multiple process methods, a method will be selected based
on the input object’s origin; that is, the name of a Stream or the name
of an output of a Flowlet.

A Flowlet that emits data can specify this name using an annotation on
the output emitter. In the absence of this annotation, the name of the
output defaults to “out”::

  @Output("code")
  OutputEmitter<String> out;

Data objects emitted through this output can then be directed to a
process method of a receiving Flowlet by annotating the method with the
origin name::

  @ProcessInput("code")
  public void tokenizeCode(String text) {
    ... // perform fancy code tokenization
  }

Input Context
.............

A process method can have an additional parameter, the ``InputContext``.
The input context provides information about the input object, such as
its origin and the number of times the object has been retried. For
example, this Flowlet tokenizes text in a smart way and uses the input
context to decide which tokenizer to use::

  @ProcessInput
  public void tokenize(String text, InputContext context) throws Exception {
    Tokenizer tokenizer;
    // If this failed before, fall back to simple white space
    if (context.getRetryCount() > 0) {
      tokenizer = new WhiteSpaceTokenizer();
    }
    // Is this code? If its origin is named "code", then assume yes
    else if ("code".equals(context.getOrigin())) {
      tokenizer = new CodeTokenizer();
    }
    else {
      // Use the smarter tokenizer
      tokenizer = new NaturalLanguageTokenizer();
    }
    for (String token : tokenizer.tokenize(text)) {
      output.emit(token);
    }
  }

Type Projection
...............

Flowlets perform an implicit projection on the input objects if they do
not match exactly what the process method accepts as arguments. This
allows you to write a single process method that can accept multiple
**compatible** types. For example, if you have a process method::

  @ProcessInput
  count(String word) {
    ...
  }

and you send data of type ``Long`` to this Flowlet, then that type does
not exactly match what the process method expects. You could now write
another process method for ``Long`` numbers::

  @ProcessInput count(Long number) {
    count(number.toString());
  }

and you could do that for every type that you might possibly want to
count, but that would be rather tedious. Type projection does this for
you automatically. If no process method is found that matches the type
of an object exactly, it picks a method that is compatible with the
object.

In this case, because Long can be converted into a String, it is
compatible with the original process method. Other compatible
conversions are:

- Every primitive type that can be converted to a ``String`` is compatible with
  ``String``.
- Any numeric type is compatible with numeric types that can represent it.
  For example, ``int`` is compatible with ``long``, ``float`` and ``double``,
  and ``long`` is compatible with ``float`` and ``double``, but ``long`` is not
  compatible with ``int`` because ``int`` cannot represent every ``long`` value.
- A byte array is compatible with a ``ByteBuffer`` and vice versa.
- A collection of type A is compatible with a collection of type B,
  if type A is compatible with type B.
  Here, a collection can be an array or any Java ``Collection``.
  Hence, a ``List<Integer>`` is compatible with a ``String[]`` array.
- Two maps are compatible if their underlying types are compatible.
  For example, a ``TreeMap<Integer, Boolean>`` is compatible with a
  ``HashMap<String, String>``.
- Other Java objects can be compatible if their fields are compatible.
  For example, in the following class ``Point`` is compatible with ``Coordinate``,
  because all common fields between the two classes are compatible.
  When projecting from ``Point`` to ``Coordinate``, the color field is dropped,
  whereas the projection from ``Coordinate`` to ``Point`` will leave the ``color`` field
  as ``null``::

    class Point {
      private int x;
      private int y;
      private String color;
    }

    class Coordinates {
      int x;
      int y;
    }

Type projections help you keep your code generic and reusable. They also
interact well with inheritance. If a Flowlet can process a specific
object class, then it can also process any subclass of that class.

Stream Event
............

A Stream event is a special type of object that comes in via Streams. It
consists of a set of headers represented by a map from String to String,
and a byte array as the body of the event. To consume a Stream with a
Flow, define a Flowlet that processes data of type ``StreamEvent``::

  class StreamReader extends AbstractFlowlet {
    ...
    @ProcessInput
    public void processEvent(StreamEvent event) {
      ...
    }

Tick Methods
............

A Flowlet’s method can be annotated with ``@Tick``. Instead of
processing data objects from a Flowlet input, this method is invoked
periodically, without arguments. This can be used, for example, to
generate data, or pull data from an external data source periodically on
a fixed cadence.

In this code snippet from the *CountRandom* example, the ``@Tick``
method in the Flowlet emits random numbers::

  public class RandomSource extends AbstractFlowlet {

    private OutputEmitter<Integer> randomOutput;

    private final Random random = new Random();

    @Tick(delay = 1L, unit = TimeUnit.MILLISECONDS)
    public void generate() throws InterruptedException {
      randomOutput.emit(random.nextInt(10000));
    }
  }

Note: @Tick method calls are serial; subsequent calls to the tick
method will be made only after the previous @Tick method call has returned.

Connecting Flowlets
...................

There are multiple ways to connect the Flowlets of a Flow. The most
common form is to use the Flowlet name. Because the name of each Flowlet
defaults to its class name, when building the Flow specification you can
simply write::

  .withFlowlets()
    .add(new RandomGenerator())
    .add(new RoundingFlowlet())
  .connect()
    .fromStream("RandomGenerator").to("RoundingFlowlet")

If you have multiple Flowlets of the same class, you can give them explicit names::

  .withFlowlets()
    .add("random", new RandomGenerator())
    .add("generator", new RandomGenerator())
    .add("rounding", new RoundingFlowlet())
  .connect()
    .from("random").to("rounding")

Batch Execution
...............

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
......................

You can have one or more instances of any given Flowlet, each consuming a disjoint
partition of each input. You can control the number of instances programmatically via the
:ref:`REST interfaces <rest-scaling-flowlets>` or via the CDAP Console. This enables you
to scale your application to meet capacity at runtime.

In the stand-alone CDAP, multiple Flowlet instances are run in threads, so in some cases
actual performance may not be improved. However, in the Distributed CDAP,
each Flowlet instance runs in its own Java Virtual Machine (JVM) with independent compute
resources. Scaling the number of Flowlets can improve performance and have a major impact
depending on your implementation.

Partitioning Strategies
.......................

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

.. _mapreduce:

MapReduce
=========

**MapReduce** is used to process data in batch. MapReduce jobs can be
written as in a conventional Hadoop system. Additionally, CDAP
**Datasets** can be accessed from MapReduce jobs as both input and
output.

To process data using MapReduce, specify ``addMapReduce()`` in your
Application specification::

  public void configure() {
    ...
    addMapReduce(new WordCountJob());

You must implement the ``MapReduce`` interface, which requires the
implementation of three methods:

- ``configure()``
- ``beforeSubmit()``
- ``onFinish()``

::

  public class WordCountJob implements MapReduce {
    @Override
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
        .setName("WordCountJob")
        .setDescription("Calculates word frequency")
        .useInputDataSet("messages")
        .useOutputDataSet("wordFrequency")
        .build();
    }

The configure method is similar to the one found in Flows and
Applications. It defines the name and description of the MapReduce job.
You can also specify Datasets to be used as input or output for the job.

The ``beforeSubmit()`` method is invoked at runtime, before the
MapReduce job is executed. Through a passed instance of the
``MapReduceContext`` you have access to the actual Hadoop job
configuration, as though you were running the MapReduce job directly on
Hadoop. For example, you can specify the Mapper and Reducer classes as
well as the intermediate data format::

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
  }

The ``onFinish()`` method is invoked after the MapReduce job has
finished. You could perform cleanup or send a notification of job
completion, if that was required. Because many MapReduce jobs do not
need this method, the ``AbstractMapReduce`` class provides a default
implementation that does nothing::

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) {
    // do nothing
  }

CDAP ``Mapper`` and ``Reducer`` implement `the standard Hadoop APIs
<http://hadoop.apache.org/docs/r2.3.0/api/org/apache/hadoop/mapreduce/package-summary.html>`__::

  public static class TokenizerMapper
      extends Mapper<byte[], byte[], Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(byte[] key, byte[] value, Context context)
        throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(Bytes.toString(value));
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
      extends Reducer<Text, IntWritable, byte[], byte[]> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      context.write(key.copyBytes(), Bytes.toBytes(sum));
    }
  }

MapReduce and Datasets
----------------------
Both CDAP ``Mapper`` and ``Reducer`` can directly read
from a Dataset or write to a Dataset similar to the way a Flowlet or Service can.

To access a Dataset directly in Mapper or Reducer, you need (1) a
declaration and (2) an injection:

#. Declare the Dataset in the MapReduce job’s configure() method.
   For example, to have access to a Dataset named *catalog*::

     public class MyMapReduceJob implements MapReduce {
       @Override
       public MapReduceSpecification configure() {
         return MapReduceSpecification.Builder.with()
           ...
           .useDataSet("catalog")
           ...


#. Inject the Dataset into the mapper or reducer that uses it::

     public static class CatalogJoinMapper extends Mapper<byte[], Purchase, ...> {
       @UseDataSet("catalog")
       private ProductCatalog catalog;

       @Override
       public void map(byte[] key, Purchase purchase, Context context)
           throws IOException, InterruptedException {
         // join with catalog by product ID
         Product product = catalog.read(purchase.getProductId());
         ...
       }

.. _Workflows:

Workflow
========

**Workflows** are used to execute a series of `MapReduce`_ jobs. A
Workflow is given a sequence of jobs that follow each other, with an
optional schedule to run the Workflow periodically. On successful
execution of a job, the control is transferred to the next job in
sequence until the last job in the sequence is executed. On failure, the
execution is stopped at the failed job and no subsequent jobs in the
sequence are executed.

To process one or more MapReduce jobs in sequence, specify
``addWorkflow()`` in your application::

  public void configure() {
    ...
    addWorkflow(new PurchaseHistoryWorkflow());

You'll then implement the ``Workflow`` interface, which requires the
``configure()`` method. From within ``configure``, call the
``addSchedule()`` method to run a WorkFlow job periodically::

  public static class PurchaseHistoryWorkflow implements Workflow {

    @Override
    public WorkflowSpecification configure() {
      return WorkflowSpecification.Builder.with()
        .setName("PurchaseHistoryWorkflow")
        .setDescription("PurchaseHistoryWorkflow description")
        .startWith(new PurchaseHistoryBuilder())
        .last(new PurchaseTrendBuilder())
        .addSchedule(new DefaultSchedule("FiveMinuteSchedule", "Run every 5 minutes",
                     "0/5 * * * *", Schedule.Action.START))
        .build();
    }
  }

If there is only one MapReduce job to be run as a part of a WorkFlow,
use the ``onlyWith()`` method after ``setDescription()`` when building
the Workflow::

  public static class PurchaseHistoryWorkflow implements Workflow {

    @Override
    public WorkflowSpecification configure() {
      return WorkflowSpecification.Builder.with() .setName("PurchaseHistoryWorkflow")
        .setDescription("PurchaseHistoryWorkflow description")
        .onlyWith(new PurchaseHistoryBuilder())
        .addSchedule(new DefaultSchedule("FiveMinuteSchedule", "Run every 5 minutes",
                     "0/5 * * * *", Schedule.Action.START))
        .build();
    }
  }


.. _spark:

Spark (Beta, Standalone CDAP only)
==================================

**Spark** is used for in-memory cluster computing. It lets you load large sets of data into memory and query them
repeatedly. This makes it suitable for both iterative and interactive programs. Similar to MapReduce,
Spark can access **Datasets** as both input and output. Spark programs in CDAP can be written in either Java or Scala.

In the current release, Spark is supported only in the Standalone CDAP.

To process data using Spark, specify ``addSpark()`` in your Application specification::

  public void configure() {
    ...
      addSpark(new WordCountProgram());

You must implement the ``Spark`` interface, which requires the
implementation of three methods:

- ``configure()``
- ``beforeSubmit()``
- ``onFinish()``

::

  public class WordCountProgram implements Spark {
    @Override
    public SparkSpecification configure() {
      return SparkSpecification.Builder.with()
        .setName("WordCountProgram")
        .setDescription("Calculates word frequency")
        .setMainClassName("com.example.WordCounter")
        .build();
    }

The configure method is similar to the one found in Flows and
MapReduce jobs. It defines the name, description, and the class containing the main method of a Spark program.

The ``beforeSubmit()`` method is invoked at runtime, before the
Spark program is executed. Because many Spark programs do not
need this method, the ``AbstractSpark`` class provides a default
implementation that does nothing::

  @Override
  public void beforeSubmit(SparkContext context) throws Exception {
    // Do nothing by default
  }

The ``onFinish()`` method is invoked after the Spark program has
finished. You could perform cleanup or send a notification of program
completion, if that was required. Like ``beforeSubmit()``, since many Spark programs do not
need this method, the ``AbstractSpark`` class also provides a default
implementation for this method that does nothing::

  @Override
  public void onFinish(boolean succeeded, SparkContext context) throws Exception {
    // Do nothing by default
  }

CDAP SparkContext
-----------------
CDAP provides its own ``SparkContext`` which is needed to access **Datasets**.

CDAP Spark programs must implement either ``JavaSparkProgram`` or ``ScalaSparkProgram``,
depending upon the language (Java or Scala) in which the program is written. You can also access the Spark's
``SparkContext`` (for Scala programs) and ``JavaSparkContext`` (for Java programs) in your CDAP Spark program by calling
``getOriginalSparkContext()`` on CDAP ``SparkContext``.

- Java::

     public class MyJavaSparkProgram implements JavaSparkProgram {
       @Override
       public void run(SparkContext sparkContext) {
         JavaSparkContext originalSparkContext = sparkContext.originalSparkContext();
           ...
       }
     }

- Scala::

    class MyScalaSparkProgram implements ScalaSparkProgram {
      override def run(sparkContext: SparkContext) {
        val originalSparkContext = sparkContext.originalSparkContext();
          ...
        }
    }

Spark and Datasets
------------------
Spark programs in CDAP can directly access **Dataset** similar to the way a MapReduce or
Procedure can. These programs can create Spark's Resilient Distributed Dataset (RDD) by reading a Datasets and also
write RDD to a Dataset.

- Creating an RDD from Dataset

  - Java:

  ::

     JavaPairRDD<byte[], Purchase> purchaseRDD = sparkContext.readFromDataset("purchases",
                                                                               byte[].class,
                                                                               Purchase.class);

  - Scala:

  ::

     val purchaseRDD: RDD[(Array[Byte], Purchase)] = sparkContext.readFromDataset("purchases",
                                                                                   classOf[Array[Byte]],
                                                                                   classOf[Purchase]);

- Writing an RDD to Dataset

  - Java:

  ::

    sparkContext.writeToDataset(purchaseRDD, "purchases", byte[].class, Purchase.class);

  - Scala:

  ::

    sparkContext.writeToDataset(purchaseRDD, "purchases", classOf[Array[Byte]], classOf[Purchase])

Services
========

Services can be run in a Cask Data Application Platform (CDAP) Application to serve data to external clients.
Similar to Flows, Services run in containers and the number of running service instances can be dynamically scaled.
Developers can implement Custom Services to interface with a legacy system and perform additional processing beyond
the CDAP processing paradigms. Examples could include running an IP-to-Geo lookup and serving user-profiles.

Custom Services lifecycle can be controlled via the CDAP Console or by using the
:ref:`CDAP Java Client API <client-api>` or :ref:`CDAP RESTful HTTP API <restful-api>`.

Services are implemented by extending ``AbstractService``, which consists of ``HttpServiceHandler`` \s to serve requests.

You can add Services to your application by calling the ``addService`` method in the
Application's ``configure`` method::

  public class AnalyticsApp extends AbstractApplication {
    @Override
    public void configure() {
      setName("AnalyticsApp");
      setDescription("Application for generating mobile analytics");
      addStream(new Stream("event"));
      addFlow(new EventProcessingFlow());
      ...
      addService(new IPGeoLookupService());
      addService(new UserLookupService());
      ...
    }
  }

::

  public class IPGeoLookupService extends AbstractService {

    @Override
    protected void configure() {
      setName("IpGeoLookupService");
      setDescription("Service to lookup locations of IP addresses.");
      useDataset("IPGeoTable");
      addHandler(new IPGeoLookupHandler());
    }
  }

Service Handlers
----------------

``ServiceHandler`` \s are used to handle and serve HTTP requests.

You add handlers to your Service by calling the ``addHandler`` method in the Service's ``configure`` method.

To use a Dataset within a handler, specify the Dataset by calling the ``useDataset`` method in the Service's
``configure`` method and include the ``@UseDataSet`` annotation in the handler to obtain an instance of the Dataset.
Each request to a method is committed as a single transaction.

::

  public class IPGeoLookupHandler extends AbstractHttpServiceHandler {
    @UseDataSet("IPGeoTable")
    Table table;

    @Path("lookup/{ip}")
    @GET
    public void lookup(HttpServiceRequest request, HttpServiceResponder responder,
                                                      @PathParam("ip") String ip) {
      // ...
      responder.sendString(200, location, Charsets.UTF_8);
    }
  }

Service Discovery
-----------------

Services announce the host and port they are running on so that they can be discovered by—and provide
access to—other programs.

Service are announced using the name passed in the ``configure`` method. The *application name*, *service id*, and
*hostname* required for registering the Service are automatically obtained.

The Service can then be discovered in Flows, Procedures, MapReduce jobs, and other Services using
appropriate program contexts. You may also access Services in a different Application
by specifying the Application name in the ``getServiceURL`` call.

For example, in Flows::

  public class GeoFlowlet extends AbstractFlowlet {

    // URL for IPGeoLookupService
    private URL serviceURL;

    // URL for SecurityService in SecurityApplication
    private URL securityURL;

    @ProcessInput
    public void process(String ip) {
      // Get URL for Service in same Application
      serviceURL = getContext().getServiceURL("IPGeoLookupService");

      // Get URL for Service in a different Application
      securityURL = getContext().getServiceURL("SecurityApplication", "SecurityService");

      // Access the IPGeoLookupService using its URL
      URLConnection connection = new URL(serviceURL, String.format("lookup/%s", ip)).openConnection();
      BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      ...
    }
  }

.. _Procedures:

Procedures
----------

To query CDAP and its Datasets and retrieve results, you can use Procedures.

Procedures allow you to make synchronous calls into CDAP from an external system
and perform server-side processing on-demand, similar to a stored procedure in a
traditional database.

Procedures are typically used to post-process data at query time. This
post-processing can include filtering, aggregating, or joins over
multiple Datasets—in fact, a Procedure can perform all the same
operations as a Flowlet with the same consistency and durability
guarantees. They are deployed into the same pool of application
containers as Flows, and you can run multiple instances to increase the
throughput of requests.

A Procedure implements and exposes a very simple API: a method name
(String) and arguments (map of Strings). This implementation is then
bound to a REST endpoint and can be called from any external system.

To create a Procedure you implement the ``Procedure`` interface, or more
conveniently, extend the ``AbstractProcedure`` class.

A Procedure is configured and initialized similarly to a Flowlet, but
instead of a process method you’ll define a handler method. Upon
external call, the handler method receives the request and sends a
response.

The initialize method is called when the Procedure handler is created.
It is not created until the first request is received for it.

The most generic way to send a response is to obtain a
``Writer`` and stream out the response as bytes. Make sure to close the
``Writer`` when you are done::

  import static co.cask.cdap.api.procedure.ProcedureResponse.Code.SUCCESS;
  ...
  class HelloWorld extends AbstractProcedure {

    @Handle("hello")
    public void wave(ProcedureRequest request,
                     ProcedureResponder responder) throws IOException {
      String hello = "Hello " + request.getArgument("who");
      ProcedureResponse.Writer writer =
        responder.stream(new ProcedureResponse(SUCCESS));
      writer.write(ByteBuffer.wrap(hello.getBytes())).close();
    }
  }

This uses the most generic way to create the response, which allows you
to send arbitrary byte content as the response body. In many cases, you
will actually respond with JSON. A CDAP
``ProcedureResponder`` has convenience methods for returning JSON maps::

  // Return a JSON map
  Map<String, Object> results = new TreeMap<String, Object>();
  results.put("totalWords", totalWords);
  results.put("uniqueWords", uniqueWords);
  results.put("averageLength", averageLength);
  responder.sendJson(results);

There is also a convenience method to respond with an error message::

  @Handle("getCount")
  public void getCount(ProcedureRequest request, ProcedureResponder responder)
                       throws IOException, InterruptedException {
    String word = request.getArgument("word");
    if (word == null) {
      responder.error(Code.CLIENT_ERROR,
                      "Method 'getCount' requires argument 'word'");
      return;
    }

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

The Cask Data Application Platform uses *Optimistic Concurrency Control* (OCC) to implement
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
number of conflicts is small. It is therefore good practice to reduce the probability of
conflicts wherever possible.

Here are some rules to follow for Flows, Flowlets, Services, and Procedures:

- Keep transactions short. The Cask Data Application Platform attempts to delay the beginning of each
  transaction as long as possible. For instance, if your Flowlet only performs write
  operations, but no read operations, then all writes are deferred until the process
  method returns. They are then performed and transacted, together with the
  removal of the processed object from the input, in a single batch execution.
  This minimizes the duration of the transaction.

- However, if your Flowlet performs a read, then the transaction must
  begin at the time of the read. If your Flowlet performs long-running
  computations after that read, then the transaction runs longer, too,
  and the risk of conflicts increases. It is therefore good practice
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
Transactions can be disabled for a Flow by annotating the Flow class with the
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
Now that you've had an introduction to programming applications
for CDAP, take a look at:

- :doc:`Apps and Packs <apps-packs>`, to walk through some example applications and useful datasets.
