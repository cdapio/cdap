.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform SparkPageRank Application
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _examples-spark-page-rank:

=======================
Spark Page Rank Example
=======================

A Cask Data Application Platform (CDAP) example demonstrating Spark and page ranking.

Overview
===========

This example demonstrates a Spark application performing streaming log analysis, computing the page rank based on
information about backlink URLs.

Data from a sample file is sent to CDAP by the external script *inject-data*
to the *backlinkURLStream*, which stores the URL pair event in its entirety.

After these events are streamed, they are taken up by the *SparkPageRankProgram*, which
goes through the entries, calculates page rank and tabulates results in an ObjectStore dataset, *ranks*.

Once the application completes, you can query the *ranks* dataset by using the ``rank`` endpoint of the *RanksService*.
It will send back a string result with page rank based on the ``url`` query parameter.

Let's look at some of these components, and then run the Application and see the results.

The SparkPageRank Application
------------------------------

As in the other `examples <index.html>`__, the components
of the Application are tied together by the class ``SparkPageRankApp``:

.. literalinclude:: /../../../cdap-examples/SparkPageRank/src/main/java/co/cask/cdap/examples/sparkpagerank/SparkPageRankApp.java
   :language: java
   :lines: 40-76

``ranks``: ObjectStore Data Storage
------------------------------------

The calculated page rank data is stored in an ObjectStore dataset, *ranks*.

``RanksService``: service
--------------------------

This service has a ``rank`` endpoint to obtain the page rank of a given URL.


Building and Starting
=====================

.. include:: building-and-starting.txt


Running CDAP Applications
============================================

.. |example| replace:: SparkPageRank

.. include:: /../../developers-manual/source/getting-started/building-apps.rst
   :start-line: 11


Running the Example
===================

Starting the Services
------------------------------

Once the application is deployed:

- Go to the *SparkPageRank* `application overview page 
  <http://localhost:9999/ns/default/apps/SparkPageRank/overview/status>`__,
  click ``RanksService`` to get to the service detail page, then click the *Start* button,
  and then do the same for the *GoogleTypePR* service; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh start service SparkPageRank.RanksService
    $ cdap-cli.sh start service SparkPageRank.GoogleTypePR
    
    Successfully started Service 'RanksService' of application 'SparkPageRank' with stored runtime arguments '{}'
    Successfully started Service 'GoogleTypePR' of application 'SparkPageRank' with stored runtime arguments '{}'

Injecting URL Pairs
------------------------------

Inject a file of URL pairs to the stream *backlinkURLStream* by running this command from the
Standalone CDAP SDK directory, using the Command Line Interface::
  
  $ cdap-cli.sh load stream backlinkURLStream examples/SparkPageRank/resources/urlpairs.txt
  
  Successfully sent stream event to stream 'pointsStream' 

Running the Spark program
------------------------------
There are three ways to start the Spark program:

1. Go to the *SparkPageRank* `application overview page 
   <http://localhost:9999/ns/default/apps/SparkPageRank/overview/status>`__,
   click ``SparkPageRankProgram`` to get to the Spark detail page, then click the *Start* button; or
   
#. Send a query via an HTTP request using the ``curl`` command::

    $ curl -w'\n' -v  -d '{args="3"}' \
        http://localhost:10000/v3/namespaces/default/apps/SparkPageRank/spark/SparkPageRankProgram/start

#. Use the Command Line Interface::

    $ cdap-cli.sh start spark SparkPageRank.SparkPageRankProgram "args='3'"

Querying the Results
------------------------------

To query the *ranks* ObjectStore through the ``RanksService``, send a query via an HTTP
request using the ``curl`` command. For example::

  $ curl -w'\n' http://localhost:10000/v3/namespaces/default/apps/SparkPageRank/services/RanksService/methods/rank?url=http://example.com/page1

You can also use the Command Line Interface::

  $ cdap-cli.sh call service SparkPageRank.RanksService GET 'rank?url=http://example.com/page1'


Stopping and Removing the Application
=====================================
Once done, you can stop the application as described above in `Stopping an Application. 
<#stopping-an-application>`__ Here is an example-specific description of the steps:

**Stopping the Spark Program**

- Go to the *SparkPageRank* `application overview page 
  <http://localhost:9999/ns/default/apps/SparkPageRank/overview/status>`__,
  click ``SparkPageRank`` to get to the spark detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh stop spark SparkPageRank.SparkPageRankProgram   

**Stopping the Service**

- Go to the *SparkPageRank* `application overview page 
  <http://localhost:9999/ns/default/apps/SparkPageRank/overview/status>`__,
  click ``RanksService`` to get to the service detail page, then click the *Stop* button,
  doing the same for the ``GoogleTypePR`` service; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh stop service SparkPageRank.RanksService
    $ cdap-cli.sh stop service SparkPageRank.GoogleTypePR

**Removing the Application**

You can now remove the application as described above, `Removing an Application <#removing-an-application>`__, or:

- Go to the *SparkPageRank* `application overview page 
  <http://localhost:9999/ns/default/apps/SparkPageRank/overview/status>`__,
  click the *Actions* menu on the right side and select *Manage* to go to the Management pane for the application,
  then click the *Actions* menu on the right side and select *Delete* to delete the application; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh delete app SparkPageRank
