.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _spark:

=============================================
Spark Programs *(Beta, Standalone CDAP only)*
=============================================

**Apache Spark** is used for in-memory cluster computing. It lets you load large sets of
data into memory and query them repeatedly. This makes it suitable for both iterative and
interactive programs. Similar to MapReduce, Spark can access **Datasets** as both input
and output. Spark programs in CDAP can be written in either Java or Scala.

In the current release, Spark (version 1.0 or higher) is supported only in the Standalone CDAP. 

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
MapReduce programs. It defines the name, description, and the class containing the main method of a Spark program.

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
Procedure can. These programs can create Spark's Resilient Distributed Dataset (RDD) by
reading a Dataset and can also write RDD to a Dataset.

In order to access a Dataset in Spark, both the key and value classes have to be serializable.
Otherwise, Spark will fail to read or write them.
For example, the Table Dataset has a value type of Row, which is not serializable.

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

Spark and Streams
------------------
Spark programs in CDAP can directly access **Streams** similar to the way a MapReduce can.
These programs can create Spark's Resilient Distributed Dataset (RDD) by reading a Stream.
You can read from a Stream using:

- Java::

    JavaPairRDD<LongWritable, Text> backlinkURLs = sc.readFromStream("backlinkURLStream",
                                                                      Text.class);

- Scala::

    val ratingsDataset: NewHadoopRDD[Array[Byte], Text] = sc.readFromStream("ratingsStream",
                                                                             classOf[Text])

It’s possible to read parts of a Stream by specifying start and end timestamps using::

    sc.readFromStream(streamName, vClass, startTime, endTime);

You can read custom objects from a Stream by providing a decoderType extended from
`StreamEventDecoder <../reference-manual/javadocs/co/cask/cdap/api/stream/StreamEventDecoder.html>`__::

    sc.readFromStream(streamName, vClass, startTime, endTime, decoderType);

Spark and Services
------------------
Spark programs in CDAP, including worker nodes, can discover Services.
Service Discovery by worker nodes ensures that if an endpoint changes during the execution of a Spark program,
due to failure or another reason, worker nodes will see the most recent endpoint.

Here is an example of service discovery in a Spark program::

    final ServiceDiscoverer discoveryServiceContext = sc.getServiceDiscoverer();
    JavaPairRDD<byte[], Integer> ranksRaw = ranks.mapToPair(new PairFunction<Tuple2<String, Double>,
                                                            byte[], Integer>() {
      @Override
      public Tuple2<byte[], Integer> call(Tuple2<String, Double> tuple) throws Exception {
        URL serviceURL = discoveryServiceContext.getServiceURL(SparkPageRankApp.GOOGLE_TYPE_PR_SERVICE_NAME);
        if (serviceURL == null) {
          throw new RuntimeException("Failed to discover service: " +
                                                                 SparkPageRankApp.GOOGLE_TYPE_PR_SERVICE_NAME);
        }
        try {
          URLConnection connection = new URL(serviceURL, String.format("transform/%s",
                                                                      tuple._2().toString())).openConnection();
          BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream(),
                                                                           Charsets.UTF_8));
          try {
            String pr = reader.readLine();
            return new Tuple2<byte[], Integer>(tuple._1().getBytes(Charsets.UTF_8), Integer.parseInt(pr));
          } finally {
            Closeables.closeQuietly(reader);
          }
        } catch (Exception e) {
          LOG.warn("Failed to read the Stream for service {}",
                                                              SparkPageRankApp.GOOGLE_PR_SERVICE, e);
          throw Throwables.propagate(e);
        }
      }
    });

Spark Metrics
------------------
Spark programs in CDAP emit metrics, similar to a MapReduce program.
CDAP collect system metrics emitted by Spark and display them in the **CDAP Console**.
This helps in monitoring the progress and resources used by a Spark program.
You can also emit custom user metrics from the worker nodes of your Spark Program::

    final Metrics sparkMetrics = sc.getMetrics();
    JavaPairRDD<byte[], Integer> ranksRaw = ranks.mapToPair(new PairFunction<Tuple2<String, Double>,
                                                            byte[], Integer>() {
      @Override
      public Tuple2<byte[], Integer> call(Tuple2<String, Double> tuple) throws Exception {
        if (tuple._2() > 100) {
          sparkMetrics.count(MORE_THAN_100_KEY, 1);
        }
      }
    });
    
Spark in Workflows
------------------
Spark programs in CDAP can also be added to a :ref:`Workflow <workflow>`, similar to a :ref:`MapReduce <mapreduce>`.

.. rubric::  Examples of Using Spark Programs

- For an example of **a Spark Program,** see the :ref:`Spark K-Means <examples-spark-k-means>`
  and :ref:`Spark Page Rank <examples-spark-page-rank>` examples.

- For a longer example, the how-to guide :ref:`cdap-spark-guide` gives another demonstration.
