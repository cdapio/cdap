.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _development-troubleshooting:

==================================
Troubleshooting a CDAP Application
==================================

A collection of tips and hints on solving problems encountered during development.


.. rubric:: Using Sequence Files in Programs

**Question:**

  I am trying to read a sequence file in a flowlet from HDFS with a custom type in value.
  The same code works when I run it in a Java app with proper classpaths but while using
  the same reader code in a flowlet, getting a ``"custom object not found"`` error. I
  checked in the fat jar which is uploaded to CDAP by ``jar -tvf`` and the custom object
  occurs in the fat jar. But I am seeing the following error in the flowlet::

    Caused by: java.lang.ClassNotFoundException: Class com.example.MyObject not found
    at org.apache.hadoop.conf.Configuration.getClassByName(Configuration.java:1953)
    at org.apache.hadoop.io.WritableName.getClass(WritableName.java:75)
    at org.apache.hadoop.io.SequenceFile$Reader.getValueClass(SequenceFile.java:2028)
    at org.apache.hadoop.io.SequenceFile$Reader.init(SequenceFile.java:1960)
    at org.apache.hadoop.io.SequenceFile$Reader.initialize(SequenceFile.java:1810)
    at org.apache.hadoop.io.SequenceFile$Reader.<init>(SequenceFile.java:1759)
    at org.apache.hadoop.io.SequenceFile$Reader.<init>(SequenceFile.java:1773)

**Answer:**

  You need to set the classLoader for the ``Configuration`` object; otherwise,
  ``SequenceFile`` cannot load custom classes.

  Try using::

    config.setClassLoader(Thread.currentThread().getContextClassLoader());
  
  before passing it (``config``) to the SequenceFile.Reader constructor.


.. _development-troubleshooting-spark:

.. rubric:: Spark's MLlib Dependencies

Spark's MLlib (Machine Learning Library) has various dependencies; please make sure you
have the appropriate dependencies according to your installation:

- Spark 1.2: https://spark.apache.org/docs/1.2.0/mllib-guide.html#dependencies
- Spark 1.3: https://spark.apache.org/docs/1.3.0/mllib-guide.html#dependencies
- Spark 1.4: https://spark.apache.org/docs/1.4.0/mllib-guide.html#dependencies

.. rubric:: Spark and IDEs Unable to Resolve Methods Imported Implicitly

Your IDE might fail to resolve methods which are imported implicitly while writing Spark
programs. The method ``reduceByKey`` in the class ``PairRDDFunctions`` is an example of
such a method. If you are using either Spark 1.2 or 1.3 to write your application, you can
resolve this issue by adding an explicit import::

  import org.apache.spark.SparkContext

**Note:** This issue has been resolved in Spark 1.4; in that version and later you will
not need this explicit import.
