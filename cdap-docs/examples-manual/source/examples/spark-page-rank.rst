.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform SparkPageRank Application
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _examples-spark-page-rank:

=======================
Spark Page Rank Example
=======================

A Cask Data Application Platform (CDAP) Example Demonstrating Spark and page ranking.

Overview
===========

This example demonstrates a Spark application performing streaming log analysis, computing the page rank based on
information about backlink URLs.

Data from a sample file is sent to CDAP by the external script *inject-data*
to the *backlinkURLStream*, which stores the URL pair event in its entirety.

After these events are streamed, they are taken up by the *SparkPageRankProgram*, which
goes through the entries, calculates page rank and tabulates results in an ObjectStore Dataset, *ranks*.

Once the application completes, you can query the *ranks* Dataset by using the ``rank`` endpoint of the *RanksService*.
It will send back a string result with page rank based on the ``url`` query parameter.

Let's look at some of these components, and then run the Application and see the results.

The SparkPageRank Application
------------------------------

As in the other `examples <index.html>`__, the components
of the Application are tied together by the class ``SparkPageRankApp``:

.. literalinclude:: /../../../cdap-examples/SparkPageRank/src/main/java/co/cask/cdap/examples/sparkpagerank/SparkPageRankApp.java
   :language: java
   :lines: 39-74

``ranks``: ObjectStore Data Storage
------------------------------------

The calculated page rank data is stored in an ObjectStore Dataset, *ranks*.

``RanksService``: Service
--------------------------

This service has a ``rank`` endpoint to obtain the page rank of a given URL.


Building and Starting
=================================

- You can either build the example (as described `below
  <#building-an-example-application>`__) or use the pre-built JAR file included in the CDAP SDK.
- Start CDAP, deploy and start the application and its components as described below in 
  `Running CDAP Applications`_\ .
  Make sure you start the Services as described below.
- Once the application has been deployed and started, you can `run the example. <#running-the-example>`__

Running CDAP Applications
============================================

.. include:: /../../developers-manual/source/getting-started/building-apps.rst
   :start-line: 9

Running the Example
===================

Starting the Services
------------------------------

Once the application is deployed:

- Click on ``SparkPageRank`` in the Overview page of the CDAP Console to get to the
  Application detail page, then click the triangular *Start* button in the right-hand of 
  the Service pane; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start service SparkPageRank.GoogleTypePR``
    * - 
      - ``$ ./bin/cdap-cli.sh start service SparkPageRank.RanksService``
    * - On Windows:
      - ``> bin\cdap-cli.bat start service SparkPageRank.GoogleTypePR``    
    * - 
      - ``> bin\cdap-cli.bat start service SparkPageRank.RanksService``    

Injecting URL Pairs
------------------------------

Run this script to inject URL pairs
to the Stream named *backlinkURLStream* in the ``SparkPageRank`` application:

.. list-table::
  :widths: 20 80
  :stub-columns: 1

  * - On Linux:
    - ``$ ./bin/inject-data.sh``
  * - On Windows:
    - ``> bin\inject-data.bat``    

Running the Spark Program
------------------------------

There are three ways to start the Spark program:

1. Click on the *Process* button in the left sidebar of the CDAP Console,
   then click *SparkPageRankProgram* in the *Process* page to get to the
   Spark detail page, then click the *Start* button; or

#. Send a query via an HTTP request using the ``curl`` command::

    curl -w '\n' -v -d '{args="3"}' \
      'http://localhost:10000/v2/apps/SparkPageRank/spark/SparkPageRankProgram/start'

   **Note:** A version of ``curl`` that works with Windows is included in the CDAP Standalone
   SDK in ``libexec\bin\curl.exe``

#. Use the Command Line Interface:

   .. list-table::
     :widths: 20 80
     :stub-columns: 1

     * - On Linux:
       - ``$ ./bin/cdap-cli.sh start spark SparkPageRank.SparkPageRankProgram``
     * - On Windows:
       - ``> bin\cdap-cli.bat start spark SparkPageRank.SparkPageRankProgram``    

Querying the Results
------------------------------

To query the *ranks* ObjectStore through the ``RanksService``,
send a query via an HTTP request using the ``curl`` command. For example::

    curl -w '\n' -v \
      'http://localhost:10000/v2/apps/SparkPageRank/services/RanksService/methods/rank?url=http://example.com/page1'

**Note:** A version of ``curl`` that works with Windows is included in the CDAP Standalone
SDK in ``libexec\bin\curl.exe``

You can also use the Command Line Interface:

.. list-table::
  :widths: 20 80
  :stub-columns: 1

  * - On Linux:
    - ``$ ./bin/cdap-cli.sh call service PageRankApp.PageRankService GET 'pagerank?url=http://example.com/page1'``
  * - On Windows:
    - ``> bin\cdap-cli.bat call service PageRankApp.PageRankService GET 'pagerank?url=http://example.com/page1'``

Stopping the Application
-------------------------------
Once done, you can stop the application as described above in `Stopping an Application. 
<#stopping-an-application>`__ Here is an example-specific description of the steps:

**Stopping the Spark Program**

- Click on the *Process* button in the left sidebar of the CDAP Console,
  then click *SparkKMeansProgram* in the *Process* page to get to the
  Spark detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh stop flow PageRankApp.PageRankService``
    * - On Windows:
      - ``> bin\cdap-cli.bat stop flow PageRankApp.PageRankService``    

**Stopping the Services**

- Click on *SparkPageRank* in the Overview page of the CDAP Console to get to the
  Application detail page, then click the square *Stop* button in the right-hand of 
  the Service pane; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh stop service SparkPageRank.GoogleTypePR``
    * - 
      - ``$ ./bin/cdap-cli.sh stop service SparkPageRank.RanksService``
    * - On Windows:
      - ``> bin\cdap-cli.bat stop service SparkPageRank.GoogleTypePR``    
    * - 
      - ``> bin\cdap-cli.bat stop service SparkPageRank.RanksService``    
