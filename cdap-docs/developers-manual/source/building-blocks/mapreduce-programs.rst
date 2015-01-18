.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _mapreduce:

============================================
MapReduce Programs
============================================

A **MapReduce** program is used to process data in batch. MapReduce can be
written as in a conventional Hadoop system. Additionally, CDAP
**Datasets** can be accessed from MapReduce as both input and
output.

To process data using MapReduce, either specify ``addMapReduce()`` in your
Application specification::

  public void configure() {
    ...
    addMapReduce(new WordCountJob());
    
or specify ``addWorkflow()`` in your Application and specify your MapReduce in the
:ref:`Workflow <workflows>` definition::

  public void configure() {
    ...
    // Run a MapReduce on the acquired data using a Workflow
    addWorkflow(new PurchaseHistoryWorkflow());
    
You must implement the ``MapReduce`` interface, which requires the
implementation of three methods:

- ``configure()``
- ``beforeSubmit()``
- ``onFinish()``

::

  public class PurchaseHistoryBuilder extends AbstractMapReduce {
    @Override
    public void configure() {
      setName("Purchase History Builder MapReduce");
      setDescription("Builds a purchase history for each customer");
      useDatasets("frequentCustomers");
      setInputDataset("purchases");
      setOutputDataset("history");
    }

The configure method is similar to the one found in Flows and
Applications. It defines the name and description of the MapReduce.
You can also specify Datasets to be used as input or output and
resources (memory and virtual cores) used by the Mappers and Reducers.

The ``beforeSubmit()`` method is invoked at runtime, before the
MapReduce is executed. Through a passed instance of the
``MapReduceContext`` you have access to the actual Hadoop job
configuration, as though you were running the MapReduce directly on
Hadoop. For example, you can specify the Mapper and Reducer classes as
well as the intermediate data format::

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
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
<http://hadoop.apache.org/docs/r2.3.0/api/org/apache/hadoop/mapreduce/package-summary.html>`__::

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
----------------------

.. rubric: Reading and Writing to Datasets from a MapReduce program

Both CDAP ``Mapper`` and ``Reducer`` can directly read
or write to a Dataset, similar to the way a Flowlet or Service can.

To access a Dataset directly in Mapper or Reducer, you need (1) a
declaration and (2) an injection:

#. Declare the Dataset in the MapReduce’s configure() method.
   For example, to have access to a Dataset named *catalog*::

     public class MyMapReduceJob implements MapReduce {
       @Override
       public void configure(MapReduceConfigurer configurer) {
         ...
         useDatasets(Arrays.asList("catalog"))
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


.. rubric: Datasets as MapReduce Input or Output

Additionally, a MapReduce program can interact with a Dataset by using it as an input or an
output, as described in :ref:`datasets-mapreduce-programs`.


.. rubric::  Examples of Using MapReduce Programs

- For an example of **a MapReduce program,** see the :ref:`Purchase
  <examples-purchase>` example.

- For a longer example, the how-to guide :ref:`cdap-mapreduce-guide` also
  demonstrates the use of MapReduce.

- The :ref:`Tutorial <tutorials>` 
  `WISE: Web Analytics <http://docs.cask.co/tutorial/current/en/tutorial2.html>`__ 
  uses MapReduce.
