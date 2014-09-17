Data-fabric
===========

The Transaction System
=======================
A Flowlet processes the data objects from its inputs one at a time. While processing a single input object, all operations, including the removal of the data from the input, and emission of data to the outputs, are executed in a transaction. This provides us with ACID properties:

  * The process method runs under read isolation to ensure that it does not see dirty writes (uncommitted writes from concurrent processing) in any of its reads. It does see, however, its own writes.
  * A failed attempt to process an input object leaves the DataFabric in a consistent state, that is, it does not leave partial writes behind.
  * All writes and emission of data are committed atomically, that is, either all of them or none of them are persisted.
  * After processing completes successfully, all its writes are persisted in a durable way.
  * 
In case of failure, the state of the DataFabric is unchanged and therefore, processing of the input object can be reattempted. This ensures exactly-once processing of each object.

The AppFabric uses Optimistic Concurrency Control (OCC) to implement transactions. Unlike most relational databases that use locks to prevent conflicting operations between transactions, under OCC we allow these conflicting writes to happen. When the transaction is committed, we can detect whether it has any conflicts: namely if during the lifetime of this transaction, another transaction committed a write for one the same keys that this transaction has written. In that case, the transaction is aborted and all of its writes are rolled back. 

In other words: If two overlapping transactions modify the same row, then the transaction that commits first will succeed, but the transaction that commits last is rolled back due to a write conflict.
Optimistic Concurrency Control is lockless and therefore avoids problems such as idle processes waiting for locks, or even worse, deadlocks. However, it comes at the cost of rollback in case of write conflicts. We can only achieve high throughput with OCC if the number of conflicts is small. It is therefore a good practice to reduce the probability of conflicts where possible:

  * Keep transactions short. The AppFabric attempts to delay the beginning of each transaction as long as possible. For instance, if your flowlet only performs write operations, but no read operations, then all writes are deferred until the process method returns. They are then performed and transacted, together with the removal of the processed object from the input, in a single batch execution. This minimizes the duration of the transaction. 
  * However, if your flowlet performs a read, then the transaction must begin at the time of the read. If your flowlet performs long-running computations after that read, then the transaction runs longer, too, and the risk of conflicts increases. It is therefore a good practice to perform reads as late in the process method as possible.
  * There are two ways to perform an increment: As a write operation that returns nothing, or as a read-write operation that returns the incremented value. If you perform the read-write operation, then that forces the transaction to begin, and the chance of conflict increases. Unless you depend on that return value, you should always perform an increment as a write operation.    
  
Keeping these guidelines in mind will help you write more efficient code. 
 
The Dataset System
===================
Datasets are your interface to the DataFabric. Instead of requiring to manipulate data with low-level DataFabric APIs, datasets provide higher level abstractions and generic, reusable Java implementations of common data patterns.  A dataset represents both the API and the actual data itself. In other words, a dataset class is a reusable, generic Java implementation of a common data pattern.  A dataset Instance is a named collection of data with associated metadata, and it is manipulated through a Dataset Class. 

Types of Datasets
-----------------
A dataset is a Java class that extends the abstract DataSet class with its own, custom methods. The implementation of a dataset typically relies on one or more underlying (embedded) datasets. For example, the IndexedTable dataset can be implemented by two underlying Table datasets, one holding the data itself, and one holding the index. We distinguish three categories of datasets: core, system, and custom datasets:
  * The core dataset of the DataFabric is a Table. Its implementation is hidden from developers and it may use private DataFabric interfaces that are not available to you.
  * A custom dataset is implemented by you and can have arbitrary code and methods. It is typically built around one or more tables (or other datasets) to implement a more specific data pattern. In fact, a custom dataset can only interact with the DataFabric through its underlying datasets. 
  * A system dataset is bundled with the AppFabric but implemented in the same way as a custom dataset, relying on one or more underlying core or system datasets. The key difference between custom and system datasets is that system datasets are implemented (and tested) by CDAP and as such they are reliable and trusted code.
  
Each dataset instance has exactly one dataset class to manipulate it - we think of the class as the type or the interface of the dataset. Every instance of a dataset has a unique name (unique within the account that it belongs to), and some metadata that defines its behavior. For example, every IndexedTable has a name and indexes a particular column of its primary table: The name of that column is a metadata property of each instance. 

Every applications must declare all datasets that it uses in its application specification. The specification of the dataset must include its name and all its metadata, including the specifications of its underlying datasets. This allows creating the dataset - if it does not exist yet - and storing its metadata at the time of deployment of the application. Application code (for example, a flow or procedure) can then use a dataset by giving only its name and type - the runtime system can use the stored metadata to create an instance of the dataset class with all required metadata.

Core Datasets - Tables
----------------------
Tables are the only core dataset, and all other datasets are built using one or more underlying tables. A table is similar to tables in a relational database, but with a few key differences:
  * Tables have no fixed schema. Unlike relational tables where every row has the same schema, every row of a table can have a different set of columns.
  * Because the set of columns is not known ahead of time, the columns of a row do not have a rich type. All column values are byte arrays and it is up to the application to convert them to and from rich types. The column names and the row key are also byte arrays.
  * When reading from a table, one need not know the names of columns: The read operation returns a map from column name to column value. It is, however, possible to specify exactly which columns to read.
  * Tables are organized in a way that the columns of a row can be read and written independently of other columns, and columns are ordered in byte-lexicographic order. They are therefore also called Ordered Columnar Tables. 
Interface	The table interface provides methods to perform read and write operations, plus a special increment method:

<pre>
public class Table extends DataSet {

  public Table(String name);

  public OperationResult<Map<byte[], byte[]>> read(Read read) 
    throws OperationException;

  public void write(WriteOperation op) 
    throws OperationException;

  public Map<byte[], Long> increment(Increment increment) 
    throws OperationException;
}
</pre>

All operations can throw an OperationException. In case of success, the read operation returns an OperationResult, which is a wrapper class around the actual return type. In addition to carrying the result value it can indicate that no result was found and the reason why. 

<pre>
class OperationResult<ReturnType> {
  public boolean isEmpty();
  public String getMessage();
  public int getStatus();
  public ReturnType getValue();
}
</pre>

Read 	To read from a table you specify a row key and optionally the columns to read:

<pre>
  Table table;
  // reads all columns
  result = table.read(new Read(rowkey));
  // reads only one column
  result = table.read(new Read(rowkey, column1));
  // reads specified set of columns
  result = table.read(new Read(rowkey, new byte[][] { col1, col2, col3 }));
  // reads all columns from colA up to but excluding colB
  result = table.read(new Read(rowkey, colA, colB));
  // reads upto 100 columns from colA up to but excluding colB
  result = table.read(new Read(rowkey, colA, colB, 100));
  // read all columns up to, but excluding colB
  result = table.read(new Read(rowkey, null, colB));
  // read upto 100 columns starting with colA
  result = table.read(new Read(rowkey, colA, null, 100));
  // read all columns in the row
  result = table.read(new Read(rowkey, null, null));
</pre>

Write	There are four types of write operations: Write, Delete, Swap and Increment. A write specifies a row key, a set of column keys and the same number of column values:

<pre>
  // write a new value to a single column
  table.write(new Write(rowkey, column1, value1));
  // write multiple columns
  table.write(new Write(rowkey, new byte[][] { col1, col2 }, 
                                new byte[][] { val1, val2 })) 
</pre>

Note that the write does not replace the entire row: It only overwrites the values of the specified columns whereas existing values of other columns remain unmodified. If you want remove columns from a row, you must use a Delete operation (see below).

Increment	
---------
An Increment interprets each column as an 8-byte (long) integer and increments it by a given value. If the column does not exist, it is created with the value that was given as the increment. If the existing value of the column is not 8 bytes long, the operation will throw an exception.
<pre>
  // write a new value to a single column
  table.write(new Increment(rowkey, countCol, 1L));
  // write multiple columns
  table.write(new Increment(rowkey, new byte[][] { col1, col2 }, 
                                    new long[]   { 5   , 10   })); 
</pre>                                    
Note that this does not return the incremented values. If you want to use ,the result of the increment operation in subsequent code, you can use the incrementAndGet method. For example, to emit the incremented value to an output:
<pre>
  Map<byte[], Long> res = 
    table.incrementAndGet(new Increment(rowkey, col, 1L));
  Long incrementedValue = res.get(col);
  output.emit(incrementedValue);
</pre>  

Delete	
------
A delete removes the specified columns from a row. Note that this does not remove the entire row. If you want to delete an entire row, you need to know its columns and specify each one. 
<pre>
  // delete a single column
  table.write(new Delete(rowkey, column1));
  // delete multiple columns
  table.write(new Delete(rowkey, new byte[][] { col1, col2 })); 
</pre>

Swap	
----
A swap operation compares the existing value of a column with an expected value, and if it matches, replaces it with a new value. This is useful to verify that a value has not changed since it was read. If it does not match, the operation fails and throws an exception. 
<pre>
  // read a user profile
  result = table.read(new Read(userkey, profileCol));
  oldProfile = result.getValue().get(profileCol);
  ...
  newProfile = manipulate(oldProfile, ...);
  // fails if somebody else has updated it in the mean time
  table.swap(userkey, profileCol, oldProfile, newProfile);
</pre>
 
System Datasets
===============
The AppFabric comes with several system-defined datasets, including key/value tables, indexed tables and time series. Each of them is defined with the help one or more embedded tables, but defines its own interface. For example:
  * The KeyValueTable implements a key/value store as a table with a single column. 
  * The IndexedTable implements a table with a secondary key using two embedded tables, one for the data and one for the secondary index.
  * The TimeseriesTable uses a table to store keyed data over time and allows querying that data over ranges of time.
See the Java documentation of these classes to learn more about these datasets.

Custom Datasets
================
You can define your own dataset classes to implement common data patterns specific to your code. We will illustrate how to define your own dataset by means of an example. Suppose we want to define a counter table that in addition to counting words also counts how many unique words it has seen.  The dataset will be built on top two underlying datasets, a KeyValueTable to count all the words and a core table for the unique count:

<pre>
public class UniqueCountTable extends DataSet {

  private Table uniqueCountTable;
  private KeyValueTable wordCountTable;
</pre>

In the constructor we take a name and create the two underlying datasets. Note that we use different names for the two tables, both derived from the name of the unique count table.  
<pre>
  public UniqueCountTable(String name) {
    super(name);
    this.uniqueCountTable = new Table("unique_count_" + name);
    this.wordCountTable = new KeyValueTable("word_count_" + name);
  }
</pre>

Like most other components of an application, the dataset must implement a configure() method that returns a specification. In the specification we save metadata about the dataset (such as its name) and the specifications of the embedded datasets obtained by calling their respective configure methods. 
<pre>
  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this)
        .dataset(this.uniqueCountTable.configure())
        .dataset(this.wordCountTable.configure())
        .create();
  }
</pre>
So far, we have written all code needed to use the dataset in an application specification, that is, at application deploy time. At that time the dataset specification returned by configure() is stored with the application’s metadata. At runtime the dataset must be available in all places that use the dataset, that is, in all instances of flowlets or procedures. To accomplish this the dataset is instantiated by reading its specification from the metadata store and calling the dataset’s constructor that takes a dataset specification as argument. Every dataset class must implement such a constructor; otherwise, it will not be functional at runtime.
 
<pre>
public UniqueCountTable(DataSetSpecification spec) {
    super(spec);
    this.uniqueCountTable = new Table(
        spec.getSpecificationFor("unique_count_" + this.getName()));
    this.wordCountTable = new KeyValueTable(
        spec.getSpecificationFor("word_count_" + this.getName()));
  }
</pre>
Now we can begin with the implementation of the dataset logic. We begin with a constants.  
<pre>
  /** Row and column name used for storing the unique count */
  private static final byte [] UNIQUE_COUNT = Bytes.toBytes("unique");
</pre>

The dataset stores a counter for each word in its own row of the word count table, and for every word it increments its counter. If the resulting value is 1, then this was the first time we encountered the word, hence we have new unique word and we increment the unique counter.

<pre>
  public void updateUniqueCount(String word)
      throws OperationException {
    // increment the counter for this word
    long newCount = this.wordCountTable.incrementAndGet(Bytes.toBytes(word), 1L);
    if (newCount == 1L) { // first time? Increment unique count
      this.uniqueCountTable.write(new Increment(UNIQUE_COUNT, UNIQUE_COUNT, 1L));
    }
  }
</pre>

Note how this method first uses the `incrementAndGet()` method to increase the count for the word, because it needs to know the result to decide whether this is a new unique word. But the second increment is done with the write() method of the table, which does not return a result. Unless you really need to get back the result, you should always use that method, because it can be optimized for higher performance by the transaction engine.

Finally, we write a method to retrieve the number of unique words seen. This method is extra cautious as it verifies that the value of the unique count column is actually eight bytes long.

<pre>
public Long readUniqueCount() throws OperationException {
    OperationResult<Map<byte[], byte[]>> result =
        this.uniqueCountTable.read(new Read(UNIQUE_COUNT, UNIQUE_COUNT));
    if (result.isEmpty()) {
      return 0L;
    }
    byte [] countBytes = result.getValue().get(UNIQUE_COUNT);
    if (countBytes == null || countBytes.length != 8) {
      return 0L;
    } 
    return Bytes.toLong(countBytes);
  }
</pre>

You may have noticed that we only use one single cell of the uniqueCountTable, and we could certainly write this code more efficiently (yet the purpose of this example is to illustrate how to implement a custom dataset on top of multiple underlying datasets). 

Notes
=====
Queues, Operations and Search

This is depending on a custom release of hbase. We keep the hbase patch is in this directory for reference, it can serve as a base for future patches. 
The current patch is HBASE-DF-NATIVE-QUEUES-1.3.4.patch and it is based on:

hbase andreas$ git show
commit 3abd441376501780c6f54fc8c9befe979cced6e3
Author: Jimmy Xiang <jxiang@apache.org>
Date:   Tue Sep 18 17:04:49 2012 +0000

    HBASE-6803 script hbase should add JAVA_LIBRARY_PATH to LD_LIBRARY_PATH
    
    git-svn-id: https://svn.apache.org/repos/asf/hbase/branches/0.94@1387260 13f79535-47bb-0310-9956-ffa450edef68


