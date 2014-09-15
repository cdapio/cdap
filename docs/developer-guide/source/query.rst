.. :author: Cask Data, Inc.
   :description: Ad-hoc Querying of Cask Data Application Platform Datasets using SQL 

==================================
Interacting with Datasets with SQL
==================================

**Ad-hoc Querying and Inserting using SQL with Cask Data Application Platform (CDAP) Datasets**

Querying Datasets with SQL
==========================

Introduction
------------
Procedures are a programmatic way to access and query the data in your Datasets. Yet sometimes you may want to explore
a Dataset in an ad-hoc manner rather than writing procedure code. This can be done using SQL if your Dataset fulfills
two requirements:

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

Limitations
-----------
* The record type must be a structured type, that is, a Java class with fields. This is because SQL tables require
  a structure type at the top level. That means, the record type cannot be a primitive,
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
  supported and will result in an exception when the dataset is created.

* A dataset can only be used in ad-hoc queries if its record type is completely contained in the dataset definition.
  This means that if the record type is or contains a parametrized type, then the type parameters must be present in
  the dataset definition. The reason is that the record type must be instantiated when executing an ad-hoc query.
  If a type parameter depends on the jar file of the application that created the dataset, then this jar file is not
  available to the query execution runtime.

  For example, you cannot execute ad-hoc queries over an ``ObjectStore<MyObject>`` if the ``MyObject`` is contained in
  the application jar. However, if you define your own dataset type ``MyObjectStore`` that extends or encapsulates an
  ``ObjectStore<MyObject>``, then ``MyObject`` becomes part of the dataset definition for ``MyObjectStore``. See the
  Purchase application for an example.


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
============================
Data can be inserted into Datasets using SQL. For example, you can write in a Dataset named
``ProductCatalog`` with this SQL query::

  INSERT INTO TABLE cdap_user_productcatalog SELECT ...

In order for a Dataset to enable record insertion from SQL query, it simply has to expose a way to write records
into itself.

For CDAP Datasets, this is done by implementing the ``RecordWritable`` interface.
Similarly to `Querying Datasets with SQL`_, the CDAP built-in Dataset KeyValueTable already implements this and
can be used to insert records from SQL queries.

Let's take a closer look at the ``RecordWritable`` interface.

Defining the Record Schema
--------------------------
Just like in the ``RecordScannable`` interface, the record schema is given by returning the Java type of each record,
using the method::

  Type getRecordType();

`The same rules <limitations>`_ as for the type of the ``RecordScannable`` interface apply to the type of the
``RecordWritable`` interface. In fact, if a Dataset implements both ``RecordScannable`` and ``RecordWritable``
interfaces, they will have to use identical record types.

Writing Records
---------------
To enable inserting SQL queries result into a Dataset, it needs to provide a means of writing a record into itself.
This is similar to how the ``BatchWritable`` interface makes Datasets writable from Map/Reduce jobs by providing
a way to write pairs of key and value. You need to implement the ``RecordWritable`` method::

      void write(RECORD record) throws IOException;

Continuing the * MyDataset*`example used above <scanning-records>`_, which showed an implementation of
``RecordScannable``, this example shows implementing a ``RecordWritable`` Dataset that is backed by a ``Table``::

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
==================================================
Now that CDAP exposes a SQL interface, we have made available a JDBC driver that you can use in your code
or in third party tools to connect to CDAP Datasets and execute SQL queries over them.

The JDBC driver is a JAR that is bundled with CDAP SDK. You can find it at the root of your SDK, at
``lib/co.cask.cdap.explore-jdbc-<version>.jar``.

 // NOTE: we may want to tell users to download the jar from our website - figure this out

If you don't have a CDAP SDK and only want to connect to an existing instance of CDAP, you can download the CDAP JDBC
driver using this `link <https://repository.continuuity.com/content/repositories/releases-public/co/cask/cdap/explore-jdbc/>`__.
Go to the directory matching the version of your running CDAP instance, and download the file named ``explore-jdbc-<version>.jar``.

Using the CDAP JDBC driver in your Java code
--------------------------------------------
To use CDAP JDBC driver in your code, you need to put ``cdap-jdbc-driver.jar`` in the classpath of your application.
If you are using Maven, you can simply add a dependency in your file ``pom.xml``::

  <dependencies>
    ...
    <dependency>
      <groupId>co.cask.cdap</groupId>
      <artifactId>explore-jdbc</artifactId>
      <version><!-- Version of CDAP you want the JDBC driver to query --></version>
    </dependency>
    ...
  </dependencies>

Here is a snippet of Java code that uses the CDAP JDBC driver to connect to a running instance of CDAP,
and executes a query over CDAP Datasets::

  Class.forName("co.cask.cdap.explore.jdbc.ExploreDriver");  // First, register the driver once in your application

  String connectionUrl = "jdbc:cdap://<cdap-host>:10000" +
    "?auth.token=<authorization_token>";  // If your CDAP instance requires a authorization token for connection,
                                          // you have to specify it here

  Connection connection = DriverManager.getConnection(connectionUrl); // Connect to CDAP instance

  // Execute a query over CDAP Datasets and retrieve the results
  ResultSet resultSet = connection.prepareStatement("select * from cdap_user_mydataset").executeQuery();
  // ...

JDBC drivers are a standard in the Java ecosystem, and you can find more about how to use them at
`this page <http://docs.oracle.com/javase/tutorial/jdbc/>`__.

Access CDAP Datasets through Business Intelligence tools
--------------------------------------------------------
Most Business Intelligence tools have a way to integrate with relational databases using JDBC drivers. They already
make available a variety of drivers to connect to standard databases, like MySQL or PostgreSQL. They also allow
to add non-standard JDBC drivers. This is what is being detailed in this section for two Business Intelligence
tools - *SquirrelSQL* and *Pentaho Data Integration*. Let's see how it is possible to connect to a running CDAP instance
and interact with CDAP Datasets using CDAP JDBC driver inside those tools.

CDAP JDBC driver integration with SquirrelSQL
.............................................
*SquirrelSQL* is a simple JDBC client which allows to execute SQL queries over a variety of relational databases.
Here is how we can add the CDAP JDBC driver inside *SquirrelSQL*:

#. Open the ``Drivers`` pane, located on the far left corner of *SquirrelSQL*.
#. Click the ``+`` icon of the ``Drivers`` pane.

   .. image:: _images/jdbc/squirrel_drivers.png

#. Add a new Driver by entering a ``Name``, for example ``Cask CDAP Driver``. The ``Example URL`` is of the form
   ``jdbc:cdap://<host>:10000?auth.token=<token>``. The ``Website URL`` can be left blank. In the ``Class Name``
   field, enter ``co.cask.cdap.explore.jdbc.ExploreDriver``.
   Click on the ``Extra Class Path`` tab, then on ``Add``, and put the path to ``co.casl.cdap.explore-jdbc-<version>.jar``.

   .. image:: _images/jdbc/squirrel_add_driver.png

#. Click on ``OK``. You should now see ``Cask CDAP Driver`` in the list of drivers from the ``Drivers`` pane of
   *SquirrelSQL*.
#. We can now create an alias to connect to a running instance of CDAP. Open the ``Aliases`` pane, and click on
   the ``+`` icon to create a new alias.
#. In the ``Add Alias`` popup, choose a name. In this example, we are going to connect to a standalone CDAP
   which we got running from the SDK. Our name will be ``CDAP Standalone``. Select the ``Cask CDAP Driver`` in
   the list of available drivers. Our ``URL`` will be ``jdbc:cdap://localhost:10000``. Our standalone instance
   does not require an authorization token, but if yours requires one, be sure to HTML encode your token
   and pass it as a parameter of the ``URL``. ``User Name`` and ``Password`` are left blank.

   .. image:: _images/jdbc/squirrel_add_alias.png

#. Click on ``OK``. ``CDAP Standalone`` is now added to the list of aliases.
#. A popup asks you to connect to your newly added alias.


Formulating Queries
===================
When creating your queries, keep these limitations in mind:

- The query syntax of CDAP is a subset of the variant of SQL that was first defined by Apache Hive.
- The SQL commands ``UPDATE`` and ``DELETE`` are not allowed on CDAP Datasets.
- When addressing your datasets in queries, you need to prefix the data set name with the CDAP
  namespace ``cdap_user_``. For example, if your Dataset is named ``ProductCatalog``, then the corresponding table
  name is ``cdap_user_productcatalog``. Note that the table name is lower-case.
  
For more examples of queries, please refer to the `Hive language manual
<https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-InsertingdataintoHiveTablesfromqueries>`__.

Where to Go Next
================
Now that you've seen ad-hoc querying, take a look at:

- `Cask Data Application Platform Testing and Debugging Guide <debugging.html>`__,
  which covers both testing and debugging of CDAP applications.

