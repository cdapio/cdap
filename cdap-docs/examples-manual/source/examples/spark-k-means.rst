.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform SparkKMeans Application
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _examples-spark-k-means:

=============
Spark K-Means
=============

A Cask Data Application Platform (CDAP) example demonstrating Spark.

Overview
=============

This example demonstrates a Spark application performing streaming analysis, computing the centers of points from an
input stream using the K-Means Clustering method.

Data from a sample file is sent to CDAP by the external script *inject-data* to the *pointsStream*. This data is
processed by the ``PointsFlow``, which stores the points coordinates event in its entirety in *points*, an ObjectStore dataset.

As these entries are created, they are taken up by the *SparkKMeansProgram*, which
goes through the entries, calculates centers and tabulates results in another ObjectStore dataset, *centers*.

Once the application completes, you can query the *centers* dataset by using the ``centers/{index}`` endpoint
of the *CentersService*. It will respond with the center's coordinates based on the ``index`` parameter (e.g. "9.1,9.1,9.1").

Let's look at some of these components, and then run the Application and see the results.

The SparkKMeans Application
------------------------------------------------------------

As in the other `examples <index.html>`__, the components
of the Application are tied together by the class ``SparkKMeansApp``:

.. literalinclude:: /../../../cdap-examples/SparkKMeans/src/main/java/co/cask/cdap/examples/sparkkmeans/SparkKMeansApp.java
   :language: java
   :lines: 49-82

``points`` and ``centers``: ObjectStore Data Storage
------------------------------------------------------------

The raw points data is stored in an ObjectStore dataset, *points*.
The calculated centers data is stored in a second ObjectStore dataset, *centers*.

``CentersService``: service
------------------------------------------------------------

This service has a ``centers/{index}`` endpoint to obtain the center's coordinates of a given index.


Building and Starting
=====================

.. include:: building-and-starting.txt


Running CDAP Applications
============================================

.. |example| replace:: SparkKMeans

.. include:: /../../developers-manual/source/getting-started/building-apps.rst
   :start-line: 11


Running the Example
===================

.. highlight:: console

Starting the Flow
------------------------------

Once the application is deployed:

- Go to the *SparkKMeans* `application overview page 
  <http://localhost:9999/ns/default/apps/SparkKMeans/overview/status>`__,
  click ``PointsFlow`` to get to the flow detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh start flow SparkKMeans.PointsFlow
  
    Successfully started Flow 'PointsFlow' of application 'SparkKMeans' with stored runtime arguments '{}'

Starting the Service
------------------------------

Once the application is deployed:

- Go to the *SparkKMeans* `application overview page 
  <http://localhost:9999/ns/default/apps/SparkKMeans/overview/status>`__,
  click ``CentersService`` to get to the service detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh start service SparkKMeans.CentersService
  
    Successfully started service 'CentersService' of application 'SparkKMeans' with stored runtime arguments '{}'

Injecting Points Data
------------------------------

Inject a file of points data to the stream *pointsStream* by running this command from the
Standalone CDAP SDK directory, using the Command Line Interface::
  
  $ cdap-cli.sh load stream pointsStream examples/SparkKMeans/resources/points.txt 
  Successfully sent stream event to stream 'pointsStream' 

Running the Spark program
------------------------------
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
------------------------------

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
Once done, you can stop the application as described above in `Stopping an Application. 
<#stopping-an-application>`__ Here is an example-specific description of the steps:

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

You can now remove the application as described above, `Removing an Application <#removing-an-application>`__, or:

- Go to the *SparkKMeans* `application overview page 
  <http://localhost:9999/ns/default/apps/SparkKMeans/overview/status>`__,
  click the *Actions* menu on the right side and select *Manage* to go to the Management pane for the application,
  then click the *Actions* menu on the right side and select *Delete* to delete the application; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh delete app SparkKMeans
