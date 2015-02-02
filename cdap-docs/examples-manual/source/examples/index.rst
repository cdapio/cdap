.. meta::
    :author: Cask Data, Inc.
    :description: Examples
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-toc: true

.. _examples-index:

========
Examples
========

.. toctree::
   :maxdepth: 1
   :titlesonly:

   Hello World <hello-world>
   Word Count <word-count>
   File Sets <fileset>
   Count Random <count-random>
   Purchase <purchase>
   Spark K-Means <spark-k-means>
   Spark Page Rank <spark-page-rank>
   Stream Conversion <stream-conversion>
   Web Analytics <web-analytics>

In addition to the :ref:`Getting Started's <getting-started-index>` 
:ref:`Quick Start/Web Log Analytics example, <quick-start>` the SDK includes these examples:

.. list-table::
  :widths: 15 60
  :header-rows: 1

  * - Example Name
    - Description
  * - :doc:`Hello World<hello-world>`
    - A simple HelloWorld App that's written using CDAP. It introduces how the components Stream, Flow, Dataset,
      and Service are used in a CDAP application.
  * - :doc:`Word Count<word-count>`
    - A simple application that counts words, and tracks word associations and unique words seen on the Stream.
      It demonstrates the power of using datasets and how they can be employed to simplify storing complex data.
  * - :doc:`File Sets<fileset>`
    - A variation of the WordCount example that operates on files. It demonstrates the usage of the FileSet
      dataset, including a service to upload and download files, and a MapReduce that operates over these files.
  * - :doc:`Count Random<count-random>`
    - An application that demonstrates the ``@Tick`` feature of Flows. It uses a tick method to generate random
      numbers which are then counted by downstream Flowlets.
  * - :doc:`Purchase<purchase>`
    - This example demonstrates use of many of the CDAP components—Streams, Flows, Flowlets, Datasets, Queries,
      MapReduce Programs, Workflows, and Services—in a single Application.

      A Flow receives events from a Stream, each event describing a purchase ("John bought 5 apples for $2");
      the Flow processes the events and stores them in a Dataset. A MapReduce program reads the Dataset, compiles
      the purchases for each customer into a purchase history and stores the histories in a second Dataset.
      The purchase histories can then be queried either through a Service or an ad-hoc SQL query.
  * - :doc:`Spark K-Means<spark-k-means>`
    - An application that demonstrates streaming text analysis using a Spark program. It calculates the centers
      of points from an input stream using the K-Means Clustering method.
  * - :doc:`Spark Page Rank<spark-page-rank>`
    - An application that demonstrates streaming text analysis using a Spark program. It computes the page rank
      of URLs from an input stream.
  * - :doc:`Stream Conversion<stream-conversion>`
    - An application that demonstrates the use of time-partitioned file sets.
      It periodically converts a stream into partitions of a file set, which can be read by SQL queries.
  * - :doc:`Web Analytics<web-analytics>`
    - An application to generate statistics and to provide insights about web usage through the analysis
      of web traffic.
  * - :doc:`User Profiles<user-profiles>`
    - An application that demonstrates column-level conflict detection using the example of updating of
      user profiles in a Dataset.


.. rubric:: What's Next

You can learn more about developing data application using CDAP by:

- Look at our :ref:`How-To Guides<guides-index>` and
  :ref:`Tutorials<tutorials>`, with a collection of quick how-to-guides and
  longer tutorials covering a complete range of Big Data application topics.
- Exploring the Web Analytics Application :doc:`source code.<web-analytics>` It includes 
  test cases that show unit-testing an application.
- For a detailed understanding of what CDAP is capable of, read our :ref:`Overview <cdap-overview>` and 
  :ref:`Building Blocks <building-blocks>` sections.
