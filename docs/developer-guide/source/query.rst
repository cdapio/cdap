Querying DataSets with SQL
==========================

Procedures are a programmatic way to access and query the data in your DataSets. Yet sometimes you may want to explore
a DataSet in an ad-hoc manner rather than writing procedure code. This can be done using SQL—if your DataSet fulfills
these two requirements:

* It defines the schema for each row; and
* It has a method to scan its data row by row.

For Reactor DataSets, this is done by implementing the ``RowScannable`` interface. Many of the Reactor built-in
DataSets already implement this, including ``KeyValueTable`` and ``ObjectStore``. Let's take a closer look at the
``RowScannable`` interface.

Defining the Row Schema
-----------------------
The row schema is given by returning the Java type of each row, and the Reactor will derive the row schema from
that type::

	Type getRowType();

For example, suppose you have a class ``Entry`` defined as::

	class Entry {
	  private final String key;
	  private final int value;
	  ...
	} 

You can implement a row-scannable DataSet that uses ``Entry`` as the row type::

	class MyDataset ... implements RowScannable<Entry> {
	  ...
	  public Type getRowType() {
	    return Entry.class;
	  } 
      
Note that Java's ``Class`` implements ``Type`` and therefore you can simply return ``Entry.class`` as the row type.
The Reactor will use reflection to infer a SQL-style row schema from the row type. In that case, the schema will be::

	(key STRING, value INT)

Note that the row type must be that of an actual Java class, not an interface. The same applies to the types of any
fields contained in the type. The reason is that interfaces only define methods but not fields; hence, reflection
would not be able to derive any fields or types from the interface.

The one exception to this rule is that Java collections such as ``List`` and ``Set`` are supported as well as
Java ``Map``. This is possible because these interfaces are so commonly used that they deserve special handling.
Note that these interfaces are parameterized and therefore require special care as described in the next section.

Parameterized Types
-------------------

Suppose instead of being fixed to ``Strings``, the ``Entry`` class is generic with type parameters for both key
and value::

	class GenericEntry<KEY, VALUE> {
	  private final KEY key;
	  private final VALUE value;
	  ...
	} 

We should easily be able to implement ``RowScannable<GenericEntry<String, Integer>>`` by defining ``getRowType()``.
However, due to Java's runtime type erasure, returning ``GenericEntry.class`` does not convey complete information
about the row type. With reflection, the Reactor can only determine the names of the two fields, but not their types.

To convey information about the type parameters, we must instead return a ``ParameterizedType``, which Java's
``Class`` does not implement. An easy way is to use Guava's ``TypeToken``::

	class MyDataset ... implements RowScannable<GenericEntry<String, Integer>>
	  public Type getRowType() {
	    return new TypeToken<GenericEntry<String, Integer>>() { }.getType();
	  } 

While this seems a little more complex at first sight, it is the de-facto standard way of dealing with Java type
erasure.

Complex Types
-------------

Your row type can also contain nested structures, lists or maps, and they will be mapped to type names as defined in
the `Hive language manual <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL>`_. For example, if
your row type is defined as::

  class Movie {
    String title;
    int year;
    Map<String, String> cast;
    List<String> reviews;
  }

Then the SQL schema of the dataset is::

  (title STRING, year INT, cast MAP<STRING, STRING>, reviews ARRAY<STRING>)

Refer to the Hive language manual for more details on schema and data types.

Scanning Rows
-------------
The second requirement for enabling SQL queries over a DataSet is to provide a means of scanning the DataSet row
by row. In a similar fashion as the BatchReadable interface makes a DataSet readable by Map/Reduce jobs by iterating
over pairs of key and value, ``RowScannable`` iterates over rows. You need to implement a method to partition the
DataSet into splits, and an additional method to create a row scanner for each split::

      List<Split> getSplits();
      SplitRowScanner<ROW> createSplitScanner(Split split);

The ``SplitRowScanner`` is very similar to a ``SplitReader``, except that instead of ``nextKeyValue()``,
``getCurrentKey()`` and ``getCurrentValue()``, it implements ``nextRow()`` and ``getCurrentRow()``. Typically,
you do not implement these methods from scratch but rely on the ``BatchReadable`` implementation of the underlying
Tables and DataSets. For example, if your DataSet is backed by a ``Table``::

	class MyDataset implements Dataset, RowScannable<Entry> {
	
	  private Table table;
	  private static final byte[] VALUE_COLUMN = { 'c' };
	
	  // ..
	  // All other DataSet methods
	  // ...
	
	  @Override
	  public Type getRowType() {
	    return Entry.class;
	  }
	
	  @Override
	  public List<Split> getSplits() {
	    return table.getSplits();
	  }
	
	  @Override
	  public SplitRowScanner<Entry> createSplitScanner(Split split) {

	    final SplitReader<byte[], Row> reader = table.createSplitReader(split);

	    return new SplitRowScanner<Entry>() {
	      @Override
	      public void initialize(Split split) {
	        reader.initialize(split);
	      }
	
	      @Override
	      public boolean nextRow() {
	        return reader.nextKeyValue();
	      }
	
	      @Override
	      public Entry getCurrentRow()  {
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

While this is straightforward, it is even easier if your DataSet already implements ``BatchReadable``. In that case,
you can reuse its implementation of ``getSplits()`` and implement the split row scanner with a helper
(``Scannables.splitRowScanner``) already defined by Reactor. It takes a split reader and a ``RowMaker``
that transforms a key and value, as produced by the ``BatchReadable``s split reader,
into a row [DOCNOTE: this example is confusing, because the ``Row`` is actually the value type of the batch readable,
whereas ``Entry`` is the row type of the row scannable. This is because our built-in Table dataset uses a class named
``Row`` for its values, which has nothing to do with the ROW type parameter of row scannable...]::

	@Override
	public SplitRowScanner<Entry> createSplitScanner(Split split) {
	  return Scannables.splitRowScanner(
	    table.createSplitReader(split),
	    new Scannables.RowMaker<byte[], Row, Entry>() {
	      @Override
	      public Entry makeRow(byte[] key, Row row) {
	        return new Entry(Bytes.toString(key), row.getInt(VALUE_COLUMN));
	      }
	    });
	}

Note there is an even simpler helper (``Scannables.valueRowScanner``) that derives a split
row scanner from a split reader. For each key and value returned by the split reader it ignores the key
and returns each the value. For example,
if your dataset implements ``BatchReadable<String, Employee>``, then you can implement ``RowScannable<Employee>`` by
defining::

	@Override
	public SplitRowScanner<Employee> createSplitScanner(Split split) {
	  return Scannables.valueRowScanner(table.createSplitReader(split));
	}

An example demonstrating these implementations is included in the Continuuity Reactor SDK in the directory
``examples/SQLQuery``. [DOCNOTE: FIXME! To be created. Or we could extend the Purchase app to support querying]

Formulating Queries
-------------------

The query syntax of the Reactor is a subset of the variant of SQL that was first defined by Apache Hive. However,
in contrast to HiveQL, Reactor queries only allow reading from data sets, not writing (``INSERT``, ``UPDATE``,
``DELETE``). When addressing your datasets in queries, you need to prefix the data set name with the reactor
namespace. For example, if your data set is named ``ProductCatalog``, then the corresponding table name is
``continuuity_user_ProductCatalog``. [DOCNOTE: FIXME! verify this prefix is correct]

