.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _data-exploration:

============================================
Data Exploration
============================================


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
CDAP will use reflection to infer a SQL-style schema using the fields of the record type.

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
  :ref:`Purchase <examples-purchase>` application for an example.


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
directory of your SDK installation at::

  lib/co.cask.cdap.cdap-explore-jdbc-<version>.jar

If you don't have a CDAP SDK and only want to connect to an existing instance of CDAP, 
you can download the CDAP JDBC driver from `this link 
<https://repository.continuuity.com/content/groups/public/co/cask/cdap/cdap-explore/>`__.
Go to the directory matching the version of your running CDAP instance, and download the file 
with the matching version number::

  cdap-explore-jdbc-<version>.jar

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

  // If your CDAP instance requires a authentication token for connection,
  // you have to specify it here.
  // Replace <cdap-host> and <authentication_token> as appropriate to your installation.
  String connectionUrl = "jdbc:cdap://<cdap-host>:10000" +
    "?auth.token=<authentication_token>";

  // Connect to CDAP instance
  Connection connection = DriverManager.getConnection(connectionUrl);

  // Execute a query over CDAP Datasets and retrieve the results
  ResultSet resultSet = connection.prepareStatement("select * from cdap_user_mydataset").executeQuery();
  ...

JDBC drivers are a standard in the Java ecosystem, with many `resources about them available
<http://docs.oracle.com/javase/tutorial/jdbc/>`__.

Accessing CDAP Datasets through Business Intelligence Tools
-----------------------------------------------------------

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

   .. image:: ../_images/jdbc/squirrel_drivers.png
      :width: 4in

#. Add a new Driver by entering a ``Name``, such as ``CDAP Driver``. The ``Example URL`` is of the form
   ``jdbc:cdap://<host>:10000?auth.token=<token>``. The ``Website URL`` can be left blank. In the ``Class Name``
   field, enter ``co.cask.cdap.explore.jdbc.ExploreDriver``.
   Click on the ``Extra Class Path`` tab, then on ``Add``, and put the path to ``co.cask.cdap.cdap-explore-jdbc-<version>.jar``.

   .. image:: ../_images/jdbc/squirrel_add_driver.png
      :width: 6in

#. Click on ``OK``. You should now see ``Cask CDAP Driver`` in the list of drivers from the ``Drivers`` pane of
   *SquirrelSQL*.
#. We can now create an alias to connect to a running instance of CDAP. Open the ``Aliases`` pane, and click on
   the ``+`` icon to create a new alias.
#. In this example, we are going to connect to a standalone CDAP from the SDK.
   The name of our alias will be ``CDAP Standalone``. Select the ``CDAP Driver`` in
   the list of available drivers. Our URL will be ``jdbc:cdap://localhost:10000``. Our standalone instance
   does not require an authentication token, but if yours requires one, HTML-encode your token
   and pass it as a parameter of the ``URL``. ``User Name`` and ``Password`` are left blank.

   .. image:: ../_images/jdbc/squirrel_add_alias.png
      :width: 6in

#. Click on ``OK``. ``CDAP Standalone`` is now added to the list of aliases.
#. A popup asks you to connect to your newly-added alias. Click on ``Connect``, and *SquirrelSQL* will retrieve
   information about your running CDAP Datasets.
#. To execute a SQL query on your CDAP Datasets, go to the ``SQL`` tab, enter a query in the center field, and click
   on the "running man" icon on top of the tab. Your results will show in the bottom half of the *SquirrelSQL* main view.

   .. image:: ../_images/jdbc/squirrel_sql_query.png
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

   .. image:: ../_images/jdbc/pentaho_add_connection.png
      :width: 6in

#. Click on ``OK``.
#. To use this connection, navigate to the ``Design`` tab on the left of the main view. In the ``Input`` menu,
   double click on ``Table input``. It will create a new transformation containing this input.

   .. image:: ../_images/jdbc/pentaho_table_input.png
      :width: 6in

#. Right-click on ``Table input`` in your transformation and select ``Edit step``. You can specify an appropriate name
   for this input such as ``CDAP Datasets query``. Under ``Connection``, select the newly created database connection;
   in this example, ``CDAP Standalone``. Enter a valid SQL query in the main ``SQL`` field. This will define the data
   available to your transformation.

   .. image:: ../_images/jdbc/pentaho_modify_input.png
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
