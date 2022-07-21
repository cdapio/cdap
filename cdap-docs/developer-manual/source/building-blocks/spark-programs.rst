.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2021 Cask Data, Inc.

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

It is recommended to implement the ``Spark`` interface by extending the ``AbstractSpark``
class, which allows the overriding of these three methods:

- ``configure()``
- ``initialize()``
- ``destroy()``

You can extend from the abstract class ``AbstractSpark`` to simplify the implementation::

  public class WordCountProgram extends AbstractSpark {
    @Override
    public SparkSpecification configure() {
      return SparkSpecification.Builder.with()
        .setName("WordCountProgram")
        .setDescription("Calculates word frequency")
        .setMainClassName("com.example.WordCounter")
        .build();
    }
    ...
  }

The configure method is similar to the one found in service and MapReduce programs. It
defines the name, description, and the class containing the Spark program to be executed
by the Spark framework.

The ``initialize()`` method is invoked at runtime, before the
Spark program is executed. Because many Spark programs do not
need this method, the ``AbstractSpark`` class provides a default
implementation that does nothing.

However, if your program requires it, you can override this method to obtain access to the
``SparkConf`` configuration and use it to set the :spark-docs:`Spark properties
<configuration.html>` for the program::

    import org.apache.spark.SparkConf;
    . . .
    @Override
    protected void initialize() throws Exception {
      getContext().setSparkConf(new SparkConf().set("spark.driver.extraJavaOptions", "-XX:MaxDirectMemorySize=1024m"));
    }

The ``destroy()`` method is invoked after the Spark program has
finished. You could perform cleanup or send a notification of program
completion, if that was required. Like ``initialize()``, since many Spark programs do not
need this method, the ``AbstractSpark`` class also provides a default
implementation for this method that does nothing.


Spark and Resources
===================
When a Spark program is configured, the resource requirements for both the Spark driver
processes and the Spark executor processes can be set, both in terms of the amount of
memory (in megabytes) and the number of virtual cores assigned.

If both the memory and the number of cores needs to be set, this can be done using::

    setExecutorResources(new Resources(1024, 2));
    
In this case, 1024 MB and two cores is assigned to each executor process.


CDAP Spark Program
==================
The main class being set through the ``setMainClass`` or ``setMainClassName`` method inside the ``Spark.configure()``
method will be executed by the Spark framework. The main class must have one of these properties:

#. Extends from ``SparkMain``, if written in Scala
#. Have a ``def main(args: Array[String])`` method, if written in Scala
#. Implements ``JavaSparkMain``, if written in Java
#. Have a ``public static void main(String[] args)`` method, if written in Java

A user program is responsible for creating a ``SparkContext`` or ``JavaSparkContext`` instance, either inside
the ``run`` methods of ``SparkMain`` or ``JavaSparkMain``, or inside their ``main`` methods.


CDAP SparkExecutionContext
==========================
CDAP provides a ``SparkExecutionContext``, which is needed to access :ref:`datasets <spark-datasets>` and to
interact with CDAP services such as metrics and service discovery. It is only available to Spark programs that
are extended from either ``SparkMain`` or ``JavaSparkMain``.

.. tabbed-parsed-literal::
  :tabs: Scala,Java
  :dependent: java-scala
  :languages: scala,java

  .. Scala

  class MyScalaSparkProgram extends SparkMain {
    override def run(implicit sec: SparkExecutionContext): Unit = {
      val sc = new SparkContext
      val RDD[(String, String)] = sc.fromDataset("mydataset")
        ...
    }
  }

  .. Java

  public class MyJavaSparkProgram implements JavaSparkMain {
    @Override
    public void run(JavaSparkExecutionContext sec) {
      JavaSparkContext jsc = new JavaSparkContext();
      JavaPairRDD<String, String> rdd = sec.fromDataset("mydataset");
        ...
    }
  }


.. _spark-datasets:

Spark and Datasets
==================
Spark programs in CDAP can directly access **datasets** similar to the way a MapReduce can. 
These programs can create Spark's Resilient Distributed Dataset (RDD) by reading a dataset
and can also write RDD to a dataset. In Scala, implicit objects are provided for reading
and writing datasets directly through the ``SparkContext`` and ``RDD`` objects.

In order to access a dataset in Spark, both the key and value classes have to be
serializable. Otherwise, Spark will fail to read or write them. For example, the Table
dataset has a value type of Row, which is not serializable. An ``ObjectStore`` dataset can
be used, provided its classes are serializable.

- Creating an RDD from a dataset:

  .. tabbed-parsed-literal::
    :tabs: Scala,Java
    :dependent: java-scala
    :languages: scala,java

    .. Scala

    val sc = new SparkContext
    val purchaseRDD = sc.readFromDataset[Array[Byte], Purchase]("purchases");

    .. Java

    JavaSparkContext jsc = new JavaSparkContext();
    JavaPairRDD<byte[], Purchase> purchaseRDD = sec.fromDataset("purchases");

- Writing an RDD to a dataset:

  .. tabbed-parsed-literal::
    :tabs: Scala,Java
    :dependent: java-scala
    :languages: scala,java

    .. Scala

    purchaseRDD.saveAsDataset("purchases")

    .. Java

    sec.saveAsDataset(purchaseRDD, "purchases");

**Note**: Spark programs can read or write to datasets in different namespaces using
:ref:`Cross Namespace Dataset Access <cross-namespace-dataset-access>` by passing a
``String`` containing the :term:`namespace` as an additional parameter before the :term:`dataset`
name parameter. (By default, if the namespace parameter is not supplied, the namespace in
which the program runs is used.).


Spark and Services
==================
Spark programs in CDAP, including worker nodes, can discover Services.
Service Discovery by worker nodes ensures that if an endpoint changes during the execution of a Spark program,
due to failure or another reason, worker nodes will see the most recent endpoint.

Here is an example of service discovery in a Spark program::

    final ServiceDiscoverer serviceDiscover = sec.getServiceDiscoverer();
    JavaPairRDD<byte[], Integer> ranksRaw = ranks.mapToPair(new PairFunction<Tuple2<String, Double>,
                                                            byte[], Integer>() {
      @Override
      public Tuple2<byte[], Integer> call(Tuple2<String, Double> tuple) throws Exception {
        URL serviceURL = serviceDiscover.getServiceURL(SparkPageRankApp.GOOGLE_TYPE_PR_SERVICE_NAME);
        if (serviceURL == null) {
          throw new RuntimeException("Failed to discover service: " +
                                                                 SparkPageRankApp.GOOGLE_TYPE_PR_SERVICE_NAME);
        }
        try {
          URLConnection connection = new URL(serviceURL, String.format("transform/%s",
                                                                      tuple._2().toString())).openConnection();
          try (
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), Charsets.UTF_8))
          ) {
            String pr = reader.readLine();
            return new Tuple2<byte[], Integer>(tuple._1().getBytes(Charsets.UTF_8), Integer.parseInt(pr));
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
Spark programs in CDAP can also be added to a :ref:`workflow <workflows>`, similar to a
:ref:`MapReduce <mapreduce>`. The Spark program can get information about the workflow
through the ``SparkExecutionContext.getWorkflowInfo`` method.


.. _spark-transactions:

Transactions and Spark
======================
When a Spark program interacts with datasets, CDAP will automatically create a
long-running transaction that covers the Spark job execution. A Spark job refers to a
Spark action and any tasks that need to be executed to evaluate the action (see
:spark-docs:`Spark Job Scheduling <job-scheduling.html#scheduling-within-an-application>`
for details). 

You can also control the transaction scope yourself explicitly. It's useful when you want
multiple Spark actions to be committed in the same transaction. For example, in Kafka
Spark Streaming, you can persist the Kafka offsets together with the changes in the
datasets in the same transaction to obtain exactly-once processing semantics.

When using an *explicit* transaction, you can access a dataset directly by calling the
``getDataset()`` method of the ``DatasetContext`` provided to the transaction. However,
the dataset acquired through ``getDataset()`` cannot be used through a function closure.
See the section on :ref:`Using Datasets in Programs <datasets-in-programs>` for additional
information.

Here is an example of using an explicit transaction in Spark:

.. tabbed-parsed-literal::
  :tabs: Scala,Java
  :dependent: java-scala
  :languages: scala,java

  .. Scala

  // Perform multiple operations in the same transaction
  Transaction {
    // Create a standard wordcount RDD
    val wordCountRDD = sc.fromStream[String]("stream")
        .flatMap(_.split(" "))
        .map((_, 1))
        .reduceByKey(_ + _)

    // Save those words that have count > 10 to the "aboveten" dataset
    wordCountRDD
      .filter(_._2 > 10)
      .saveAsDataset("aboveten")

    // Save all wordcount to an "allcounts" dataset
    wordCountRDD.saveAsDataset("allcounts")

    // Updates to both the "aboveten" and "allcounts" datasets will be committed within the same transaction
  }

  // Perform RDD operations together with direct dataset access in the same transaction
  Transaction((datasetContext: DatasetContext) => {
    sc.fromDataset[String, Int]("source")
      .saveAsDataset("sink")

    val table: Table = datasetContext.getDataset("copyCount")
    table.increment(new Increment("source", "sink", 1L))
  })

  .. Java

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    // Perform RDD operations together with direct dataset access in the same transaction
    sec.execute(new TransactionRunnable(sec));
  }

  static class TransactionRunnable implements TxRunnable, Serializable {

    private final JavaSparkExecutionContext sec;

    public TransactionRunnable(JavaSparkExecutionContext sec) {
      this.sec = sec;
    }

    @Override
    public void run(DatasetContext context) throws Exception {
      JavaPairRDD<String, Integer> source = sec.fromDataset("source");
      sec.saveAsDataset(source, "sink");

      Table table = context.getDataset("copyCount");
      table.increment(new Increment("source", "sink", 1L));
    }
  }

.. _spark-spark-versions:

.. highlight:: xml

Spark Versions
==============
CDAP allows you to write Spark programs using either Spark2 or Spark3.
To use Spark2, you must add the ``cdap-api-spark2_2.11`` Maven dependency::

    . . .
    <dependency>
      <groupId>io.cdap.cdap</groupId>
      <artifactId>cdap-api-spark2_2.11</artifactId>
      <version>${cdap.version}</version>
    </dependency>
    . . .

To use Spark3, you must add the ``cdap-api-spark3_2.12`` Maven dependency::

    . . .
    <dependency>
      <groupId>io.cdap.cdap</groupId>
      <artifactId>cdap-api-spark2_2.11</artifactId>
      <version>${cdap.version}</version>
    </dependency>
    . . .

Spark Program Examples
======================
- For an example, the how-to guide :ref:`cdap-spark-guide` gives a demonstration.

- If you have problems with resolving methods when developing Spark problems in an IDE 
  or running Spark programs, see :ref:`these hints <development-troubleshooting-spark>`.
