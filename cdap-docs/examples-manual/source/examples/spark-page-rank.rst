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
to the *backlinkURLStream*. This data is processed by the
``BackLinkFlow``, which stores the URL pair event in its entirety in *backlinkURLs*, an ObjectStore Dataset.

As these entries are created, they are taken up by the *SparkPageRankProgram*, which
goes through the entries, calculates page rank and tabulates results in another ObjectStore Dataset, *ranks*.

Once the application completes, you can query the *ranks* Dataset by using the ``rank`` endpoint of the *RanksService*.
It will send back a string result with page rank based on the ``url`` query parameter.

Let's look at some of these elements, and then run the Application and see the results.

The SparkPageRank Application
------------------------------

As in the other `examples <index.html>`__, the components
of the Application are tied together by the class ``SparkPageRankApp``:

.. literalinclude:: /../../../cdap-examples/SparkPageRank/src/main/java/co/cask/cdap/examples/sparkpagerank/SparkPageRankApp.java
   :language: java
   :lines: 49-81

``backlinkURLs`` and ``ranks``: ObjectStore Data Storage
------------------------------------------------------------

The raw URL pair data is stored in an ObjectStore Dataset, *backlinkURLs*.
The calculated page rank data is stored in a second ObjectStore Dataset, *ranks*.

``RanksService``: Service
------------------------------------------------------------

This service has a ``rank`` endpoint to obtain the page rank of a given URL.


Building and Starting
=================================

- You can either build the example (as described `below
  <#building-an-example-application>`__) or use the pre-built JAR file included in the CDAP SDK.
- Start CDAP, deploy and start the application as described below in 
  `Running CDAP Applications`_\ .
  Make sure you start the Flow and Service as described.
- Once the application has been deployed and started, you can `run the example. <#running-the-example>`__

Running CDAP Applications
============================================

.. include:: /../../developers-manual/source/getting-started/building-apps.rst
   :start-line: 9

Running the Example
===================

Running the Spark program
============================================

Once the application is deployed:

- Click on the ``SparkPageRankProgram`` in the Application page of the CDAP Console to get to the
  Spark dialogue, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command-line Interface:

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start spark SparkPageRank.SparkPageRankProgram``
    * - On Windows:
      - ``> bin\cdap-cli.bat start spark SparkPageRank.SparkPageRankProgram``

Running the RanksService
============================================

Once the application is deployed:

- Click on ``SparkPageRank`` in the Overview page of the CDAP Console to get to the
  Application detail page, click ``RanksService`` in the *Service* pane to get to the
  Service detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command-line Interface:

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start service SparkPageRank.RanksService``
    * - On Windows:
      - ``> bin\cdap-cli.bat start service SparkPageRank.RanksService``

Running the GoogleTypePR
============================================

Once the application is deployed:

- Click on ``SparkPageRank`` in the Overview page of the CDAP Console to get to the
  Application detail page, click ``GoogleTypePR`` in the *Service* pane to get to the
  Service detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command-line Interface:

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start service SparkPageRank.GoogleTypePR``
    * - On Windows:
      - ``> bin\cdap-cli.bat start service SparkPageRank.GoogleTypePR``

Injecting URL Pairs
------------------------------

Run this script to inject URL pairs
to the Stream named *backlinkURLStream* in the ``SparkPageRank`` application::

  $ ./bin/inject-data.sh

On Windows::

  > bin\inject-data.bat

Querying the Results
------------------------------

If the Service has not already been started, you start it either through the
CDAP Console or via an HTTP request using the ``curl`` command::

  curl -v -X POST 'http://localhost:10000/v2/apps/SparkPageRank/services/RanksService/start'

To query the *ranks* ObjectStore through the ``RanksService``,
send a query via an HTTP request using the ``curl`` command. For example::

    curl -w '\n' -v \
      'http://localhost:10000/v2/apps/SparkPageRank/services/RanksService/methods/rank?url=http://example.com/page1'

  On Windows, the copy of ``curl`` is located in the ``libexec`` directory of the SDK::

    libexec\curl...

Stopping the Spark program
============================================

Once the application is deployed:

- Click on the ``SparkPageRankProgram`` in the Application page of the CDAP Console to get to the
  Spark dialogue, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command-line Interface:

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh stop spark SparkPageRank.SparkPageRankProgram``
    * - On Windows:
      - ``> bin\cdap-cli.bat stop spark SparkPageRank.SparkPageRankProgram``

Stopping the RanksService
============================================

Once the application is deployed:

- Click on ``SparkPageRank`` in the Overview page of the CDAP Console to get to the
  Application detail page, click ``RanksService`` in the *Service* pane to get to the
  Service detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command-line Interface:

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh stop service SparkPageRank.RanksService``
    * - On Windows:
      - ``> bin\cdap-cli.bat stop service SparkPageRank.RanksService``

Stopping the GoogleTypePR
============================================

Once the application is deployed:

- Click on ``SparkPageRank`` in the Overview page of the CDAP Console to get to the
  Application detail page, click ``GoogleTypePR`` in the *Service* pane to get to the
  Service detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command-line Interface:

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh stop service SparkPageRank.GoogleTypePR``
    * - On Windows:
      - ``> bin\cdap-cli.bat stop service SparkPageRank.GoogleTypePR``

Stopping the Application
-------------------------------
Once done, you can stop the application as described above in `Stopping an Application. <#stopping-an-application>`__
