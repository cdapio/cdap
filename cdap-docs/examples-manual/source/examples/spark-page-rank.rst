.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform SparkPageRank Application
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _examples-spark-page-rank:

=======================
Spark Page Rank Example
=======================

A Cask Data Application Platform (CDAP) example demonstrating Spark and MapReduce in a Workflow to compute page ranks.

Overview
========

This example demonstrates Spark and MapReduce performing streaming log analysis, computing the page rank based on information about backlink URLs

Data from a sample file is sent to CDAP by the external script *inject-data*
to the *backlinkURLStream*, which stores the URL pair event in its entirety.

After these events are streamed, they are taken up by the *SparkPageRankProgram*, which
goes through the entries, calculates page rank and tabulates results in an ObjectStore dataset, *ranks*.

A MapReduce job uses the output of the Spark program from the *ranks* dataset,
computes the total number of pages for every unique page rank, and then tabulates
the results in another ObjectStore dataset, *rankscount*.

The *PageRankWorkflow* ties the Spark and MapReduce to run sequentially in this application.

Once the application completes, you can query the *ranks* dataset by using the ``rank`` endpoint of the *RanksService*.
It will send back a string result with page rank based on the ``url`` query parameter. You can also query the
*rankscount* dataset by using ``total`` endpoint of the *TotalPagesPRService*. It will send the total number of pages for the queried page rank as a string.

Let's look at some of these components, and then run the application and see the results.

The SparkPageRank Application
-----------------------------

As in the other `examples <index.html>`__, the components
of the application are tied together by the class ``SparkPageRankApp``:

.. literalinclude:: /../../../cdap-examples/SparkPageRank/src/main/java/co/cask/cdap/examples/sparkpagerank/SparkPageRankApp.java
   :language: java
   :lines: 50-89

The ``ranks`` and ``rankscount`` ObjectStore Data Storage
---------------------------------------------------------

The calculated page rank data is stored in an ObjectStore dataset, *ranks*,
with the total number of pages for a page rank stored in an additional ObjectStore dataset, *rankscount*.

The ``RanksService`` and ``TotalPagesPRService`` Service
--------------------------------------------------------

This ``RanksService`` service has a ``rank`` endpoint to obtain the page rank of a given URL.
This ``TotalPagesPRService`` service has a ``total`` endpoint to obtain the total number of pages with a given page rank.

Memory Requirements
-------------------
When a Spark program is running inside a workflow, the memory requirements configured for the Spark program may need increasing beyond the defaults:

.. literalinclude:: /../../../cdap-examples/SparkPageRank/src/main/java/co/cask/cdap/examples/sparkpagerank/SparkPageRankApp.java
   :language: java
   :lines: 113-114



.. |example| replace:: SparkPageRank
.. include:: building-starting-running-cdap.txt


Running the Example
===================

Starting the Services
---------------------

Once the application is deployed:

- Go to the *SparkPageRank* `application overview page 
  <http://localhost:9999/ns/default/apps/SparkPageRank/overview/status>`__,
  click ``RanksService`` to get to the service detail page, then click the *Start* button,
  and then do the same for the *GoogleTypePRService* and *TotalPagesPRService* services; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh start service SparkPageRank.RanksService
    $ cdap-cli.sh start service SparkPageRank.GoogleTypePRService
    $ cdap-cli.sh start service SparkPageRank.TotalPagesPRService
    
    Successfully started service 'RanksService' of application 'SparkPageRank' with stored runtime arguments '{}'
    Successfully started service 'GoogleTypePRService' of application 'SparkPageRank' with stored runtime arguments '{}'
    Successfully started service 'TotalPagesPRService' of application 'SparkPageRank' with stored runtime arguments '{}'

Injecting URL Pairs
-------------------

Inject a file of URL pairs to the stream *backlinkURLStream* by running this command from the
Standalone CDAP SDK directory, using the Command Line Interface::
  
  $ cdap-cli.sh load stream backlinkURLStream examples/SparkPageRank/resources/urlpairs.txt
  
  Successfully sent stream event to stream 'backlinkURLStream'

Running the Workflow
--------------------
There are three ways to start the workflow:

1. Go to the *SparkPageRank* `application overview page 
   <http://localhost:9999/ns/default/apps/SparkPageRank/overview/status>`__,
   click ``PageRankWorkflow`` to get to the Workflow detail page, then click the *Start* button; or
   
#. Send a query via an HTTP request using the ``curl`` command::

    $ curl -w'\n' -v  -d '{spark.SparkPageRankProgram.args=3}' \
        http://localhost:10000/v3/namespaces/default/apps/SparkPageRank/workflows/PageRankWorkflow/start

#. Use the Command Line Interface::

    $ cdap-cli.sh start workflow SparkPageRank.PageRankWorkflow "spark.SparkPageRankProgram.args='3'"

Querying the Results
--------------------

To query the *ranks* ObjectStore through the ``RanksService``, send a query via an HTTP
request using the ``curl`` command. For example::

  $ curl -w'\n' -X POST -d'{"url":"http://example.com/page1"}' http://localhost:10000/v3/namespaces/default/apps/SparkPageRank/services/RanksService/methods/rank

You can also use the Command Line Interface::

  $ cdap-cli.sh call service SparkPageRank.RanksService POST 'rank' body '{"url":"http://example.com/page1"}'

Similarly, to query the *rankscount* ObjectStore using the ``TotalPagesPRService``. For example, to get the total number of
pages with a page rank of 10 you can do the following:

curl::

  $ curl -w'\n' http://localhost:10000/v3/namespaces/default/apps/SparkPageRank/services/TotalPagesPRService/methods/total/10

Command Line Interface::

  $ cdap-cli.sh call service SparkPageRank.TotalPagesPRService GET 'total/10'


Stopping and Removing the Application
=====================================
Once done, you can stop the application as described in :ref:`Stopping an Application 
<cdap-building-running-stopping>`. Here is an example-specific description of the steps:

**Stopping the Workflow**

- Go to the *SparkPageRank* `application overview page 
  <http://localhost:9999/ns/default/apps/SparkPageRank/overview/status>`__,
  click ``PageRankWorkflow`` to get to the workflow detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh stop spark SparkPageRank.SparkPageRankProgram   

**Stopping the Service**

- Go to the *SparkPageRank* `application overview page 
  <http://localhost:9999/ns/default/apps/SparkPageRank/overview/status>`__,
  click ``RanksService`` to get to the service detail page, then click the *Stop* button,
  doing the same for the ``GoogleTypePRService`` and ``TotalPagesPRService`` services; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh stop service SparkPageRank.RanksService
    $ cdap-cli.sh stop service SparkPageRank.GoogleTypePRService
    $ cdap-cli.sh stop service SparkPageRank.TotalPagesPRService

**Removing the Application**

You can now remove the application as described in :ref:`Removing an Application <cdap-building-running-removing>`, or:

- Go to the *SparkPageRank* `application overview page 
  <http://localhost:9999/ns/default/apps/SparkPageRank/overview/status>`__,
  click the *Actions* menu on the right side and select *Manage* to go to the Management pane for the application,
  then click the *Actions* menu on the right side and select *Delete* to delete the application; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh delete app SparkPageRank
