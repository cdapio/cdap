.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _mapreduce:

============================================
MapReduce Jobs
============================================

A **MapReduce Job** is used to process data in batch. MapReduce jobs can be
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

.. _mapreduce-datasets:

MapReduce and Datasets
----------------------

.. rubric: Reading and Writing to Datasets from a MapReduce Job

Both CDAP ``Mapper`` and ``Reducer`` can directly read
or write to a Dataset, similar to the way a Flowlet or Service can.

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


.. rubric: Datasets as MapReduce Input or Output

Additionally, a MapReduce job can interact with a Dataset by using it as an input or an
output, as described in :ref:`datasets-map-reduce-jobs`.


.. rubric::  Examples of Using Map Reduce Jobs

Flows and Flowlets are included in just about every CDAP :ref:`application <apps-and-packs>`,
:ref:`tutorial <tutorials>`, :ref:`guide <guides-index>` or :ref:`example <examples-index>`.

- For an example of **a MapReduce Job,** see the :ref:`Purchase
  <examples-purchase>` example.

- For a longer example, the how-to guide :ref:`cdap-mapreduce-guide` also
  demonstrates the use of MapReduce.
