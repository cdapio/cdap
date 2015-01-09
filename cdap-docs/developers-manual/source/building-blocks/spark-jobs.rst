.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _spark:

============================================
Spark Jobs *(Beta, Standalone CDAP only)*
============================================

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
MapReduce jobs. It defines the name, description, and the class containing the main method of a Spark program.

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
Procedure can. These programs can create Spark's Resilient Distributed Dataset (RDD) by reading a Datasets and also
write RDD to a Dataset.

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


.. rubric::  Examples of Using Spark Jobs

- For an example of **a Spark Job,** see the :ref:`Spark K-Means <examples-spark-k-means>`
  and :ref:`Spark Page Rank <examples-spark-page-rank>` examples.

- For a longer example, the how-to guide :ref:`cdap-spark-guide` gives another demonstration.
