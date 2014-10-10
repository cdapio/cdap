.. :author: Cask Data, Inc.
   :description: Examples
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Additional Examples
============================================


.. toctree::
   :maxdepth: 1
   :titlesonly:
   
   HelloWorld <hello-world>
   WordCount <word-count>
   CountRandom <count-random>
   Purchase <purchase>
   SparkKMeans <spark-k-means>
   SparkPageRank <spark-page-rank>
   Web Analytics <web-analytics>


The SDK includes these additional examples:

.. list-table::
  :widths: 15 60
  :header-rows: 1

  * - Example Name
    - Description
  * - :doc:`HelloWorld<hello-world>`
    - A simple HelloWorld App that's written using CDAP. It introduces how the elements Stream, Dataset, Flow,
      and Procedure are used in a CDAP application.
  * - :doc:`WordCount<word-count>`
    - A simple application that counts words, and tracks word associations and unique words seen on the Stream.
      It demonstrates the power of using datasets and how they can be employed to simplify storing complex data.
  * - :doc:`CountRandom<count-random>`
    - An application that demonstrates the ``@Tick`` feature of Flows. It uses a tick method to generate random
      numbers which are then counted by downstream Flowlets.
  * - :doc:`Purchase<purchase>`
    - This example demonstrates use of each of the CDAP elements—Streams, Flows, Flowlets, Datasets, Queries,
      Procedures, MapReduce, Workflows, and Services—in a single Application.
      A Flow receives events from a Stream, each event describing a purchase ("John bought 5 apples for $2");
      the Flow processes the events and stores them in a Dataset. A Mapreduce Job reads the Dataset, compiles
      the purchases for each customer into a purchase history and stores the histories in a second Dataset.
      The purchase histories can then be queried either through a Procedure or an ad-hoc SQL query.
  * - :doc:`SparkKMeans<spark-k-means>`
    - An application that demonstrates streaming text analysis using a Spark program. It calculates the centers
      of points from an input stream using the KMeans Clustering method.
  * - :doc:`SparkPageRank<spark-page-rank>`
    - An application that demonstrates streaming text analysis using a Spark program. It computes the page rank
      of URLs from an input stream.
  * - :doc:`Web Analytics<web-analytics>`
    - An application to generate statistics and to provide insights about web usage through the analysis
      of web traffic.
