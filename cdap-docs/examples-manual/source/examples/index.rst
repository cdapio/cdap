.. meta::
    :author: Cask Data, Inc.
    :description: Examples
    :copyright: Copyright © 2014-2018 Cask Data, Inc.

:hide-toc: true

.. _examples-index:

========
Examples
========

.. toctree::
   :maxdepth: 1
   :titlesonly:

   Introduction to CDAP <introduction-to-cdap>
   Clicks and Views <clicks-and-views>
   Data Cleansing <data-cleansing>
   Decision Tree Regression <decision-tree-regression>
   FileSet Example <fileset-example>
   Log Analysis <log-analysis>
   Purchase <purchase>
   Spam Classifier <spam-classifier>
   Spark K-Means <spark-k-means>
   Spark Page Rank <spark-page-rank>
   Sport Results <sport-results>
   Wikipedia Pipeline <wikipedia-data-pipeline>
   Word Count <word-count>


For a comprehensive inital introduction to CDAP and its capabilities, follow the
:ref:`Introduction to CDAP <introduction-to-cdap>` tutorial, covering everything from installation
of a CDAP Sandbox through the creation of a real-world application.

For developers intent on building Java-based CDAP applications, see the :ref:`Getting Started's
<getting-started-index>` :ref:`Quick Start/Web Log Analytics <quick-start>` example.

The :ref:`CDAP Sandbox <sandbox>` includes these examples in the download:

.. list-table::
  :widths: 15 60
  :header-rows: 1

  * - Example Name
    - Description
  * - :doc:`Clicks and Views <clicks-and-views>`
    - An application that demonstrates a reduce-side join across two streams using a MapReduce program.
  * - :doc:`Data Cleansing <data-cleansing>`
    - A Cask Data Application Platform (CDAP) example demonstrating incrementally consuming partitions of a
      partitioned fileset using MapReduce.
  * - :doc:`Decision Tree Regression <decision-tree-regression>`
    - An application demonstrating machine-learning model training using a Spark2 program. It trains decision tree
      regression models from labeled data uploaded through a Service.
  * - :doc:`FileSet Example <fileset-example>`
    - A variation of the *WordCount* example that operates on files. It demonstrates the usage of the FileSet
      dataset, including a service to upload and download files, and a MapReduce that operates over these files.
  * - :doc:`Log Analysis <log-analysis>`
    - An example demonstrating Spark and MapReduce running in parallel inside a workflow, showing the use of
      forks within workflows.
  * - :doc:`Purchase <purchase>`
    - This example demonstrates use of many of the CDAP components |---| streams, flows, flowlets, datasets, queries,
      MapReduce programs, workflows, and services |---| in a single application.

      A flow receives events from a stream, each event describing a purchase ("John bought 5 apples for $2");
      the flow processes the events and stores them in a dataset. A MapReduce program reads the dataset, compiles
      the purchases for each customer into a purchase history and stores the histories in a second dataset.
      The purchase histories can then be queried either through a service or an ad-hoc SQL query.
  * - :doc:`Spam Classifier <spam-classifier>`
    - An application that demonstrates a Spark Streaming application that classifies Kafka
      messages as either "spam" or "ham" (not "spam") based on a trained Spark MLlib NaiveBayes model.
  * - :doc:`Spark K-Means <spark-k-means>`
    - An application that demonstrates streaming text analysis using a Spark program. It calculates the centers
      of points from an input stream using the K-Means clustering method.
  * - :doc:`Spark Page Rank <spark-page-rank>`
    - An application that demonstrates text analysis using Spark and MapReduce programs. It computes the page rank
      of URLs from an input stream.
  * - :doc:`Sport Results <sport-results>`
    - An application that illustrates the use of partitioned File sets.
      It loads game results into a File set partitioned by league and season, and processes them with MapReduce.
  * - :doc:`Wikipedia Pipeline <wikipedia-data-pipeline>`
    - An application that performs analysis on Wikipedia data using MapReduce and Spark programs
      running within a CDAP workflow: *WikipediaPipelineWorkflow*.
  * - :doc:`Word Count <word-count>`
    - A simple application that counts words, and tracks word associations and unique words seen on the stream.
      It demonstrates the power of using datasets and how they can be employed to simplify storing complex data.
      It uses a configuration class to configure the application at deployment time.


.. rubric:: What's Next

For more about developing data application using CDAP:

- Look at our :ref:`How-To Guides <guides-index>` and :ref:`Tutorials <tutorials>`, with a
  collection of quick how-to-guides and longer tutorials covering a complete range of Big
  Data application topics.
- For a detailed understanding of what CDAP is capable of, read our :ref:`Overview <cdap-overview>` and
  :ref:`Building Blocks <building-blocks>` sections.
