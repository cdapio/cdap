:orphan:

.. :author: Cask Data, Inc.
   :description: Examples for the Cask Data Application Platform
   :copyright: Copyright © 2014 Cask Data, Inc.

.. index::
   single: Example Applications

Example Applications
====================

The SDK includes these examples:

.. list-table::
  :widths: 15 60
  :header-rows: 1

  * - Example Name
    - Description
  * - :ref:`HelloWorld<hello-world>`
    - A Simple HelloWorld App that's written using CDAP. It introduces how Stream, Dataset, Flow and Procedure
      are used in an CDAP application.
  * - :ref:`WordCount<word-count>`
    - A simple application that counts words and tracks word associations and unique words seen on the Stream.
      It demonstrates the power of using datasets and how they can be used to simplify storing complex data.
  * - :ref:`CountRandom<count-random>`
    - A simple application that demonstrates the ``@Tick`` feature of flows. It uses a tick method to generate
      random numbers, which are then counted by downstream flowlets.
  * - :ref:`Purchase<purchase>`
    - This example demonstrates use of each of the CDAP elements: Streams, Flows, Flowlets, Datasets, Queries,
      Procedures, MapReduce Jobs, Workflows, and Services in a single Application.

      A flow receives events from a stream, each describing a purchase ("John bought 5 apples for $2"), processes
      and stores them it in a dataset. A Mapreduce Job reads that dataset, compiles the purchases of each customer
      into a purchase history and stores it in a dataset. The purchase histories can then be queried through a
      procedure and also through Ad-hoc SQL queries.
  * - :ref:`SparkKMeans<spark-k-means>`
    - An application that demonstrates streaming text analysis using a Spark program. It calculates the centers
      of points from an input stream using the KMeans Clustering method.
  * - :ref:`SparkPageRank<spark-page-rank>`
    - An application that demonstrates streaming text analysis using a Spark program. It computes the page rank
      of URLs from an input stream.

.. include:: /examples/helloworld.rstx
.. include:: /examples/wordcount.rstx
.. include:: /examples/countrandom.rstx
.. include:: /examples/purchase.rstx
.. include:: /examples/sparkKMeans.rstx
.. include:: /examples/sparkPageRank.rstx
