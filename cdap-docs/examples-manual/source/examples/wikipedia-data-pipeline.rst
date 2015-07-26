.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform Wikipedia Pipeline Application
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _examples-wikipedia-data-pipeline:

==================
Wikipedia Pipeline
==================

A Cask Data Application Platform (CDAP) example demonstrating a typical data processing pipeline using CDAP Workflows.

Overview
========

This example demonstrates a CDAP application performing analysis on Wikipedia data using MapReduce and Spark programs
running within a CDAP Workflow - *WikipediaPipelineWorkflow*.

This example can be run in both online and offline modes. In the online mode, the custom action *DownloadWikiDataAction* reads the *pageTitleStream*
in which each event is one element from the output of the facebook likes API - https://developers.facebook.com/docs/graph-api/reference/v2.4/object/likes
For each event, it tries to download Wikipedia data for the page using the https://www.mediawiki.org/wiki/API:Main_page API.
It stores the downloaded data in the KeyValueTable dataset *wikiData*.
In the offline mode, it expects Wikipedia data formatted per the output of the MediaWiki API above in the stream *wikiStream*.
The MapReduce program *wikiDataToDataset* consumes this stream and stores it in the same KeyValueTable dataset *wikiData*.
Data can be uploaded to the *wikiStream* using the CDAP CLI.

Once raw Wikipedia data is available using in either online or offline modes, the *WikipediaPipelineWorkflow* runs a MapReduce program
*WikiContentValidatorAndNormalizer* that filters bad records from the raw data, as well as normalizes it by converting the wikitext-formatted
data to plain text. It then stores the output in another KeyValueTable dataset *normalized*

The *WikipediaPipelineWorkflow* then contains a fork, with two branches in parallel. One branch runs the Apache Spark program *SparkWikipediaAnalyzer*.
This program consumes normalized data and runs topic modeling on it using the Latent Dirichlet Allocation (LDA) algorithm. It stores its
output in the CDAP Table dataset *lda*, with one row for each iteration, and a column per topic containing the score for that topic.
The other branch contains a MapReduce program *TopNMapReduce* that consumes the normalized data and produces the top N words in the dataset *topn*




Once the application completes, you can query the *centers* dataset by using the ``centers/{index}`` endpoint
of the *CentersService*. It will respond with the center's coordinates based on the ``index`` parameter (e.g. "9.1,9.1,9.1").




Let's look at some of these components, and then run the application and see the results.

The SparkKMeans Application
---------------------------

As in the other `examples <index.html>`__, the components
of the application are tied together by the class ``WikipediaPipelineApp``:

.. literalinclude:: /../../../cdap-examples/WikipediaPipeline/src/main/java/co/cask/cdap/examples/wikipedia/WikipediaPipelineApp.java
   :language: java
   :lines: 49-82

The ``points`` and ``centers`` ObjectStore Data Storage
-------------------------------------------------------

The raw points data is stored in an ObjectStore dataset, *points*.
The calculated centers data is stored in a second ObjectStore dataset, *centers*.

The ``CentersService`` Service
------------------------------

This service has a ``centers/{index}`` endpoint to obtain the center's coordinates of a given index.


.. |example| replace:: SparkKMeans
.. include:: building-starting-running-cdap.txt


Running the Example
===================

.. highlight:: console

Starting the Flow
-----------------

Once the application is deployed:

- Go to the *SparkKMeans* `application overview page 
  <http://localhost:9999/ns/default/apps/SparkKMeans/overview/status>`__,
  click ``PointsFlow`` to get to the flow detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh start flow SparkKMeans.PointsFlow
  
    Successfully started flow 'PointsFlow' of application 'SparkKMeans' with stored runtime arguments '{}'

Starting the Service
--------------------

Once the application is deployed:

- Go to the *SparkKMeans* `application overview page 
  <http://localhost:9999/ns/default/apps/SparkKMeans/overview/status>`__,
  click ``CentersService`` to get to the service detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh start service SparkKMeans.CentersService
  
    Successfully started service 'CentersService' of application 'SparkKMeans' with stored runtime arguments '{}'

Injecting Points Data
---------------------

Inject a file of points data to the stream *pointsStream* by running this command from the
Standalone CDAP SDK directory, using the Command Line Interface::
  
  $ cdap-cli.sh load stream pointsStream examples/SparkKMeans/resources/points.txt 
  Successfully sent stream event to stream 'pointsStream' 

Running the Spark Program
-------------------------
There are three ways to start the Spark program:

1. Go to the *SparkKMeans* `application overview page 
   <http://localhost:9999/ns/default/apps/SparkKMeans/overview/status>`__,
   click ``CentersService`` to get to the service detail page, then click the *Start* button; or
   
#. Send a query via an HTTP request using the ``curl`` command::

    curl -w'\n' -v  -d '{args="3"}' \
      http://localhost:10000/v3/namespaces/default/apps/SparkKMeans/spark/SparkKMeansProgram/start

#. Use the Command Line Interface::

    $ cdap-cli.sh start spark SparkKMeans.SparkKMeansProgram "args='3'"

Querying the Results
--------------------

To query the *centers* ObjectStore using the ``CentersService``, you can:

- Send a query via an HTTP request using the ``curl`` command. For example::

    curl -w'\n' -v http://localhost:10000/v3/namespaces/default/apps/SparkKMeans/services/CentersService/methods/centers/1

- You can use the Command Line Interface::

    $ cdap-cli.sh call service SparkKMeans.CentersService GET centers/1

    +======================================================================================================+
    | status | headers                 | body size | body                                                  |
    +======================================================================================================+
    | 200    | Content-Length : 53     | 53        | 755.3206896551723,755.3206896551723,484.6722828459188 |
    |        | Connection : keep-alive |           |                                                       |
    |        | Content-Type : text/pla |           |                                                       |
    |        | in; charset=UTF-8       |           |                                                       |
    +======================================================================================================+


Stopping and Removing the Application
=====================================
Once done, you can stop the application as described in :ref:`Stopping an Application 
<cdap-building-running-stopping>`. Here is an example-specific description of the steps:

**Stopping the Flow**

- Go to the *SparkKMeans* `application overview page 
  <http://localhost:9999/ns/default/apps/SparkKMeans/overview/status>`__,
  click ``SparkKMeans`` to get to the flow detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh stop flow SparkKMeans.PointsFlow   

**Stopping the Service**

- Go to the *SparkKMeans* `application overview page 
  <http://localhost:9999/ns/default/apps/SparkKMeans/overview/status>`__,
  click ``CentersService`` to get to the service detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh stop service SparkKMeans.CentersService

**Removing the Application**

You can now remove the application as described in :ref:`Removing an Application <cdap-building-running-removing>`, or:

- Go to the *SparkKMeans* `application overview page 
  <http://localhost:9999/ns/default/apps/SparkKMeans/overview/status>`__,
  click the *Actions* menu on the right side and select *Manage* to go to the Management pane for the application,
  then click the *Actions* menu on the right side and select *Delete* to delete the application; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh delete app SparkKMeans
