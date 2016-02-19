.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _spark:

==============
Spark Programs
==============

*Apache Spark* is used for in-memory cluster computing. It lets you load large sets of
data into memory and query them repeatedly. This makes it suitable for both iterative and
interactive programs. Similar to MapReduce, Spark can access :ref:`datasets <spark-datasets>` 
as both input and output. *Spark programs* in CDAP can be written in either Java or Scala.

To process data using Spark, specify ``addSpark()`` in your application specification::

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

The configure method is similar to the one found in flows and
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


Spark and Resources
===================
When a Spark program is configured, the resource requirements for both the Spark driver
processes and the Spark executor processes can be set, both in terms of the amount of
memory (in megabytes) and the number of virtual cores assigned.

For example, in the :ref:`Spark Page Rank <examples-spark-page-rank>` example, in the configuration of
the ``PageRankSpark``, the amount of memory is specified:

.. literalinclude:: /../../../cdap-examples/SparkPageRank/src/main/java/co/cask/cdap/examples/sparkpagerank/SparkPageRankApp.java
   :language: java
   :lines: 104-116

If both the memory and the number of cores needs to be set, this can be done using::

    setExecutorResources(new Resources(1024, 2));
    
In this case, 1024 MB and two cores is assigned to each executor process.


CDAP SparkContext
=================
CDAP provides its own ``SparkContext``, which is needed to access :ref:`datasets <spark-datasets>`.

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

.. _spark-datasets:


Spark and Datasets
==================
Spark programs in CDAP can directly access **dataset** similar to the way a MapReduce can. 
These programs can create Spark's Resilient Distributed Dataset (RDD) by
reading a dataset and can also write RDD to a dataset.

In order to access a dataset in Spark, both the key and value classes have to be serializable.
Otherwise, Spark will fail to read or write them.
For example, the Table dataset has a value type of Row, which is not serializable.
An ``ObjectStore`` dataset can be used, provided its classes are serializable.

- Creating an RDD from dataset

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

- Writing an RDD to dataset

  - Java:

  ::

    sparkContext.writeToDataset(purchaseRDD, "purchases", byte[].class, Purchase.class);

  - Scala:

  ::

    sparkContext.writeToDataset(purchaseRDD, "purchases", classOf[Array[Byte]], classOf[Purchase])

You can also access a dataset directly by calling the ``getDataset()`` method of the SparkContext.
See also the section on :ref:`Using Datasets in Programs <datasets-in-programs>`.


Spark and Streams
=================
Spark programs in CDAP can directly access **streams** similar to the way a MapReduce can.
These programs can create Spark's Resilient Distributed Dataset (RDD) by reading a stream.
You can read from a stream using:

- Java::

    JavaPairRDD<LongWritable, Text> backlinkURLs = sc.readFromStream("backlinkURLStream",
                                                                      Text.class);

- Scala::

    val ratingsDataset: NewHadoopRDD[Array[Byte], Text] = sc.readFromStream("ratingsStream",
                                                                             classOf[Text])

It’s possible to read parts of a stream by specifying start and end timestamps using::

    sc.readFromStream(streamName, vClass, startTime, endTime);

You can read custom objects from a stream by providing a decoderType extended from
`StreamEventDecoder <../../reference-manual/javadocs/co/cask/cdap/api/stream/StreamEventDecoder.html>`__::

    sc.readFromStream(streamName, vClass, startTime, endTime, decoderType);


Spark and Services
==================
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
          LOG.warn("Failed to read the stream for service {}",
                                                              SparkPageRankApp.GOOGLE_PR_SERVICE, e);
          throw Throwables.propagate(e);
        }
      }
    });


Spark Metrics
=============
Spark programs in CDAP emit metrics, similar to a MapReduce program.
CDAP collect system metrics emitted by Spark and display them in the **CDAP UI**.
This helps in monitoring the progress and resources used by a Spark program.
You can also emit custom user metrics from the worker nodes of your Spark program::

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
==================
Spark programs in CDAP can also be added to a :ref:`workflow <workflows>`, similar to a :ref:`MapReduce <mapreduce>`.


Spark SQL
=========
The entry point to functionality in Spark SQL is through a Spark `SQLContext
<http://spark.apache.org/docs/latest/sql-programming-guide.html#starting-point-sqlcontext>`__.
To run a Spark SQL program in CDAP, you can obtain a ``SQLContext`` from CDAP's ``SparkContext``
using one of these approaches:

- Java::

    org.apache.spark.SparkContext originalSparkContext = sc.getOriginalSparkContext();
    SQLContext sqlContext = new SQLContext(originalSparkContext);

- Scala::

    val originalSparkContext:org.apache.spark.SparkContext = sc.getOriginalSparkContext[org.apache.spark.SparkContext]
    val sqlContext = new org.apache.spark.sql.SQLContext(originalSparkContext)


Spark Program Examples
======================
- For an example of **a Spark program,** see the :ref:`Spark K-Means <examples-spark-k-means>`
  and :ref:`Spark Page Rank <examples-spark-page-rank>` examples.

- For a longer example, the how-to guide :ref:`cdap-spark-guide` gives another demonstration.

- If you have problems with resolving methods when developing Spark problems in an IDE 
  or running Spark programs, see :ref:`these hints <development-troubleshooting-spark>`.
  
