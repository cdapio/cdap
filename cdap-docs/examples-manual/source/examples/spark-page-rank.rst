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

Once the application completes, you can query the *ranks* Dataset by using the ``rank`` method of the *RanksProcedure*.
It will send back a JSON-formatted result with page rank based on the ``url`` parameter.

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

``RanksProcedure``: Procedure
------------------------------------------------------------

This procedure has a ``rank`` method to obtain the page rank of a given URL.


Building and Starting
=================================

- You can either build the example (as described `below
  <#building-an-example-application>`__) or use the pre-built JAR file included in the CDAP SDK.
- Start CDAP, deploy and start the application as described below in 
  `Running CDAP Applications`_\ .
  Make sure you start the flow and procedure as described.
- Once the application has been deployed and started, you can `run the example. <#running-the-example>`__

Running CDAP Applications
============================================

.. include:: /../../developers-manual/source/getting-started/building-apps.rst
   :start-line: 9

Running the Example
===================

Injecting URL Pairs
------------------------------

Run this script to inject URL pairs
to the Stream named *backlinkURLStream* in the ``SparkPageRank`` application::

  $ ./bin/inject-data.sh

On Windows::

  > bin\inject-data.bat

Running the Spark Program
------------------------------

There are three ways to start the Spark program:

1. Click on the ``SparkPageRankProgram`` in the Application page of the CDAP Console to get to the
   Spark dialogue, then click the *Start* button.

2. Send a query via an HTTP request using the ``curl`` command::

     curl -v -d '{args="3"}' \
       'http://localhost:10000/v2/apps/SparkPageRank/spark/SparkPageRankProgram/start'

   On Windows, the copy of ``curl`` is located in the ``libexec`` directory of the SDK::

     libexec\curl...

3. Use the command::

    $ ./bin/app-manager.sh --action run

  On Windows::

    > bin\app-manager.bat run

Querying the Results
------------------------------

If the Procedure has not already been started, you start it either through the 
CDAP Console or via an HTTP request using the ``curl`` command::

  curl -v -d 'http://localhost:10000/v2/apps/SparkPageRank/procedures/RanksProcedure/start'
  
There are two ways to query the *ranks* ObjectStore through the ``RanksProcedure`` procedure:

1. Send a query via an HTTP request using the ``curl`` command. For example::

     curl -v -d '{"url": "http://example.com/page1"}' \
       'http://localhost:10000/v2/apps/SparkPageRank/procedures/RanksProcedure/methods/rank'; echo

   On Windows, the copy of ``curl`` is located in the ``libexec`` directory of the SDK::

    libexec\curl...

2. Type the Procedure method name, ``RanksProcedure``, in the Query page of the CDAP Console.

   1. Click the *Query* button in the left side-bar of the CDAP Console.
   #. Click on the *RanksProcedure* Procedure.
   #. Type ``rank`` in the *Method* text box.
   #. Type the parameters required for this method, a JSON string with the name *url* and
      value of a URI, ``"http://example.com/page1"``::

        { "url" : "http://example.com/page1" }

   #. Click the *Execute* button.
   #. The rank for that URL will be displayed in the Console in JSON format::

        "0.9988696312751688"

Stopping the Application
-------------------------------
Once done, you can stop the application as described above in `Stopping an Application. <#stopping-an-application>`__
