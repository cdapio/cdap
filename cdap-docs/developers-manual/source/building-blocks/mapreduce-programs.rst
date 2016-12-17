.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

.. _mapreduce:

==================
MapReduce Programs
==================

A *MapReduce* program is used to process data in batch. MapReduce can be
written as in a conventional Hadoop system. Additionally, CDAP
**datasets** can be accessed from MapReduce as both input and
output.

To process data using MapReduce, either specify ``addMapReduce()`` in your
application specification::

  public void configure() {
    ...
    addMapReduce(new WordCountJob());
    
or specify ``addWorkflow()`` in your application and specify your MapReduce in the
:ref:`workflow <workflows>` definition::

  public void configure() {
    ...
    // Run a MapReduce on the acquired data using a workflow
    addWorkflow(new PurchaseHistoryWorkflow());
    
You must implement the ``MapReduce`` interface, which requires the
implementation of three methods:

- ``configure()``
- ``initialize()``
- ``onFinish()``

::

  public class PurchaseHistoryBuilder extends AbstractMapReduce {
    @Override
    public void configure() {
      setName("Purchase History Builder MapReduce");
      setDescription("Builds a purchase history for each customer");
    }

The configure method is similar to the one found in flows and
applications. It defines the name and description of the MapReduce.
You can also :ref:`specify resources <mapreduce-resources>` (memory and virtual cores) used by the
mappers and reducers.

The ``initialize()`` method is invoked at runtime, before the MapReduce is executed.
Through the ``getContext()`` method you can obtain an instance of the ``MapReduceContext``.
It allows you to :ref:`specify datasets <mapreduce-datasets>` to be used as input or output;
it also provides you access to the actual Hadoop job configuration, as though you were running the
MapReduce directly on Hadoop. For example, you can specify the input and output datasets,
the mapper and reducer classes as well as the intermediate data format::

  @Override
  public void initialize() throws Exception {
    MapReduceContext context = getContext();
    context.addInput(Input.ofDataset("purchases"));
    context.addOutput(Output.ofDataset("history"));

    Job job = context.getHadoopJob();
    job.setMapperClass(PurchaseMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(PerUserReducer.class);
  }

The ``onFinish()`` method is invoked after the MapReduce has
finished. You could perform cleanup or send a notification of job
completion, if that was required. Because many MapReduce programs do not
need this method, the ``AbstractMapReduce`` class provides a default
implementation that does nothing::

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) {
    // do nothing
  }

CDAP ``Mapper`` and ``Reducer`` implement `the standard Hadoop APIs
<http://hadoop.apache.org/docs/r2.3.0/api/org/apache/hadoop/mapreduce/package-summary.html>`__
(note that CDAP only supports the "new" API in ``org.apache.hadoop.mapreduce``, and not the
"old" API in ``org.apache.hadoop.mapred``)::

  public static class PurchaseMapper 
      extends Mapper<byte[], Purchase, Text, Text> {

    private Metrics mapMetrics;

    @Override
    public void map(byte[] key, Purchase purchase, Context context)
      throws IOException, InterruptedException {
      String user = purchase.getCustomer();
      if (purchase.getPrice() > 100000) {
        mapMetrics.count("purchases.large", 1);
      }
      context.write(new Text(user), new Text(new Gson().toJson(purchase)));
    }
  }

  public static class PerUserReducer 
      extends Reducer<Text, Text, String, PurchaseHistory> {
    
    @UseDataSet("frequentCustomers")
    private KeyValueTable frequentCustomers;
    private Metrics reduceMetrics;

    public void reduce(Text customer, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      PurchaseHistory purchases = new PurchaseHistory(customer.toString());
      int numPurchases = 0;
      for (Text val : values) {
        purchases.add(new Gson().fromJson(val.toString(), Purchase.class));
        numPurchases++;
      }
      if (numPurchases == 1) {
        reduceMetrics.count("customers.rare", 1);
      } else if (numPurchases > 10) {
        reduceMetrics.count("customers.frequent", 1);
        frequentCustomers.write(customer.toString(), String.valueOf(numPurchases));
      }
      context.write(customer.toString(), purchases);
    }
  }

.. _mapreduce-datasets:

MapReduce and Datasets
======================

.. highlight:: java

.. rubric: Reading and Writing to Datasets from a MapReduce program

Both a CDAP ``Mapper`` and ``Reducer`` can directly read or write to a dataset, using
one of the following options. (Note that the second and third options can be used for a
``Partitioner`` or ``Comparator``, if configured on the MapReduce job.)

#. Inject the dataset into the mapper or reducer that uses it. This method
   is useful if the name of the dataset is constant or known at compile time.
   For example, to have access to a dataset named *catalog*::

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

#. Acquire the dataset in the mapper's or reducer's ``initialize()`` method. As opposed
   to the previous method, this does not require the dataset name to be constant; it
   only needs to be known at the time the task starts (for example, through configuration).
   Note that this requires that the mapper or reducer class implements the ``ProgramLifecycle``
   interface, which includes the two methods ``initialize()`` and ``destroy()``::

     public static class CatalogJoinMapper extends Mapper<byte[], Purchase, ...>
       implements ProgramLifecycle<MapReduceTaskContext> {

       private ProductCatalog catalog;

       @Override
       public void initialize(MapReduceTaskContext mapReduceTaskContext) throws Exception {
         catalog = mapReduceTaskContext.getDataset(
             mapReduceTaskContext.getRuntimeArguments().get("catalog.table.name"));
       }

       @Override
       public void destroy() {
       }

       @Override
       public void map(byte[] key, Purchase purchase, Context context)
           throws IOException, InterruptedException {
         // join with catalog by product ID
         Product product = catalog.read(purchase.getProductId());
         ...
       }

#. Dynamically acquire the dataset in the mapper every time it is accessed. This is useful if
   the name of the dataset is not known at initialization time; for example, if it depends on
   the data passed to each ``map()`` call. In this case, you also implement the ``ProgramLifecycle``
   interface, to save the ``MapReduceTaskContext`` for use in the ``map()`` method. For example::

     public static class CatalogJoinMapper extends Mapper<byte[], Purchase, ...>
       implements ProgramLifecycle<MapReduceTaskContext> {

       private MapReduceTaskContext taskContext;

       @Override
       public void initialize(MapReduceTaskContext mapReduceTaskContext) throws Exception {
         taskContext = mapReduceTaskContext;
       }

       @Override
       public void destroy() {
       }

       @Override
       public void map(byte[] key, Purchase purchase, Context context)
           throws IOException, InterruptedException {
         // join with catalog by product ID
         String catalogName = determineCatalogName(purchase.getProductCategory());
         ProductCatalog catalog = taskContext.getDataset(catalogName);
         Product product = catalog.read(purchase.getProductId());
         ...
       }

       private String determineCatalogName(String productCategory) {
         ...
       }

See also the section on :ref:`Using Datasets in Programs <datasets-in-programs>`.

.. rubric: Datasets as MapReduce Input or Output

A MapReduce program can interact with a dataset by using it as 
:ref:`an input <mapreduce-datasets-input>` or :ref:`an output <mapreduce-datasets-output>`.
The dataset needs to implement specific interfaces to support this, as described in the
following sections.

.. _mapreduce-datasets-input:

.. rubric:: A Dataset as the Input Source of a MapReduce Program

When you run a MapReduce program, you can configure it to read its input from a dataset. The
source dataset must implement the ``BatchReadable`` interface, which requires two methods::

  public interface BatchReadable<KEY, VALUE> {
    List<Split> getSplits();
    SplitReader<KEY, VALUE> createSplitReader(Split split);
  }

These two methods complement each other: ``getSplits()`` must return all splits of the dataset
that the MapReduce program will read; ``createSplitReader()`` is then called in every Mapper to
read one of the splits. Note that the ``KEY`` and ``VALUE`` type parameters of the split reader
must match the input key and value type parameters of the Mapper.

Because ``getSplits()`` has no arguments, it will typically create splits that cover the
entire dataset. If you want to use a custom selection of the input data, define another
method in your dataset with additional parameters and explicitly set the input in the
``initialize()`` method.

For example, the system dataset ``KeyValueTable`` implements ``BatchReadable<byte[], byte[]>``
with an extra method that allows specification of the number of splits and a range of keys::

  public class KeyValueTable extends AbstractDataset
                             implements BatchReadable<byte[], byte[]> {
    ...
    public List<Split> getSplits(int numSplits, byte[] start, byte[] stop);
  }

To read a range of keys and give a hint that you want 16 splits, write::

  @UseDataSet("myTable")
  KeyValueTable kvTable;
  ...
  @Override
  public void initialize() throws Exception {
    MapReduceContext context = getContext();
    ...
    context.addInput(Input.ofDataset("myTable", kvTable.getSplits(16, startKey, stopKey)));
  }

.. _mapreduce-datasets-output:

.. rubric:: A Dataset as the Output Destination of a MapReduce Program

Just as you have the option to read input from a dataset, you have the option to write to a dataset as
the output destination of a MapReduce program if that dataset implements the ``BatchWritable``
interface::

  public interface BatchWritable<KEY, VALUE> {
    void write(KEY key, VALUE value);
  }

The ``write()`` method is used to redirect all writes performed by a Reducer to the dataset.
Again, the ``KEY`` and ``VALUE`` type parameters must match the output key and value type
parameters of the Reducer.


.. rubric:: Multiple Output Destinations of a MapReduce Program

To write to multiple output datasets from a MapReduce program, begin by adding the datasets as outputs::

  public void beforeSubmit(MapReduceContext context) throws Exception {
    ...
    context.addOutput(Input.ofDataset("productCounts"));
    context.addOutput(Input.ofDataset("catalog"));
  }

Then, have the ``Mapper`` and/or ``Reducer`` implement ``ProgramLifeCycle<MapReduceTaskContext>``.
This is to obtain access to the ``MapReduceTaskContext`` in their initialization methods and
to be able to write using the write method of the ``MapReduceTaskContext``::

  public static class CustomMapper extends Mapper<LongWritable, Text, NullWritable, Text>
    implements ProgramLifecycle<MapReduceTaskContext<NullWritable, Text>> {

    private MapReduceTaskContext<NullWritable, Text> mapReduceTaskContext;

    @Override
    public void initialize(MapReduceTaskContext<NullWritable, Text> context) throws Exception {
      this.mapReduceTaskContext = context;
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // compute some condition
      ...
      if (someCondition) {
        mapReduceTaskContext.write("productCounts", key, value);
      } else {
        mapReduceTaskContext.write("catalog", key, value);
      }
    }

  }

Note that the multiple output write method |---| ``MapReduceTaskContext.write(String, KEY key, VALUE value)`` |---| can
only be used if there are multiple outputs. Similarly, the single output write
method |---| ``MapReduceTaskContext.write(KEY key, VALUE value)`` |---| can only be used if there
is a single output to the MapReduce program.


.. _mapreduce-transactions:

MapReduce and Transactions
==========================
When you run a MapReduce that interacts with datasets, the system creates a
long-running transaction. Similar to the transaction of a flowlet, here are
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
this, it is not recommended to write to the same dataset from both real-time and MapReduce
programs. It is better to use different datasets, or at least ensure that the real-time
processing writes to a disjoint set of columns.

It's important to note that the MapReduce framework will reattempt a task (Mapper or
Reducer) if it fails. If the task is writing to a dataset, the reattempt of the task will
most likely repeat the writes that were already performed in the failed attempt. Therefore
it is highly advisable that all writes performed by MapReduce programs be idempotent.

See :ref:`transaction-system` for additional information.


.. _mapreduce-resources:

MapReduce and Resources
=======================

Both the YARN container size and the number of virtual cores used in a MapReduce job can be specified
as part of the configuration. They can also be set at runtime through the use of runtime arguments. An
example of this is shown in the :ref:`Purchase <examples-purchase>` example, where the memory requirements
are set:

.. literalinclude:: /../../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseHistoryBuilder.java
   :language: java
   :lines: 47-57
   :append: ...

The Resources API, if called with two arguments, sets both the memory used in megabytes
and the number of virtual cores used.


MapReduce Program Examples
==========================

- For an example of **a MapReduce program,** see the :ref:`Purchase
  <examples-purchase>` example.

- For a longer example, the how-to guide :ref:`cdap-mapreduce-guide` also
  demonstrates the use of MapReduce.

- The :ref:`Tutorial <tutorials>` :ref:`WISE: Web Analytics <cdap-tutorial-wise>` uses MapReduce.
