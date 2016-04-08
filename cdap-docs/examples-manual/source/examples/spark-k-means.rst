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
========
This example demonstrates a Spark application performing streaming analysis, computing the centers of points from an
input stream using the K-Means Clustering method.

Data from a sample file is sent to CDAP by a CDAP CLI command to the *pointsStream*. This data is
processed by the ``PointsFlow``, which stores the points coordinates event in its entirety in *points*, an ObjectStore dataset.

As these entries are created, they are taken up by the *SparkKMeansProgram*, which
goes through the entries, calculates centers and tabulates results in another ObjectStore dataset, *centers*.

Once the application completes, you can query the *centers* dataset by using the ``centers/{index}`` endpoint
of the *CentersService*. It will respond with the center's coordinates based on the ``index`` parameter (e.g. "9.1,9.1,9.1").

Let's look at some of these components, and then run the application and see the results.

The *SparkKMeans* Application
-----------------------------
As in the other `examples <index.html>`__, the components
of the application are tied together by the class ``SparkKMeansApp``:

.. literalinclude:: /../../../cdap-examples/SparkKMeans/src/main/java/co/cask/cdap/examples/sparkkmeans/SparkKMeansApp.java
   :language: java
   :lines: 51-82
   :append: . . .

The *points* and *centers* ObjectStore Data Storage
---------------------------------------------------
The raw points data is stored in an ObjectStore dataset, *points*.
The calculated centers data is stored in a second ObjectStore dataset, *centers*.

The *CentersService* Service
----------------------------
This service has a ``centers/{index}`` endpoint to obtain the center's coordinates of a given index.


.. Building and Starting
.. =====================
.. |example| replace:: SparkKMeans
.. |example-italic| replace:: *SparkKMeans*
.. |application-overview-page| replace:: :cdap-ui-apps-programs:`application overview page, programs tab <SparkKMeans>`

.. include:: _includes/_building-starting-running.txt


Running the Example
===================

.. Starting the Flow
.. -----------------
.. |example-flow| replace:: PointsFlow
.. |example-flow-italic| replace:: *PointsFlow*

.. include:: _includes/_starting-flow.txt

.. Starting the Service
.. --------------------
.. |example-service| replace:: CentersService
.. |example-service-italic| replace:: *CentersService*

.. include:: _includes/_starting-service.txt

Injecting Points Data
---------------------
Inject a file of points data to the stream *pointsStream* by running this command from the
Standalone CDAP SDK directory, using the Command Line Interface::
  
  $ cdap-cli.sh load stream pointsStream examples/SparkKMeans/resources/points.txt 
  Successfully sent stream event to stream 'pointsStream' 

Running the Spark Program
-------------------------
There are three ways to start the Spark program:

1. Go to the *SparkKMeans* `application overview page, programs tab 
   <http://localhost:9999/ns/default/apps/SparkKMeans/overview/programs>`__,
   click ``CentersService`` to get to the service detail page, then click the *Start* button; or
   
#. Send a query via an HTTP request using the ``curl`` command::

    $ curl -w'\n' -v  -d '{args="3"}' \
      http://localhost:10000/v3/namespaces/default/apps/SparkKMeans/spark/SparkKMeansProgram/start

#. Use the Command Line Interface::

    $ cdap-cli.sh start spark SparkKMeans.SparkKMeansProgram "args='3'"

Querying the Results
--------------------
To query the *centers* ObjectStore using the ``CentersService``, you can:

- Send a query via an HTTP request using the ``curl`` command. For example::

    $ curl -w'\n' -v http://localhost:10000/v3/namespaces/default/apps/SparkKMeans/services/CentersService/methods/centers/1

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


.. Stopping and Removing the Application
.. =====================================
.. include:: _includes/_stopping-flow-service-removing-application.txt
