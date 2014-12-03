.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform SparkKMeans Application
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _examples-spark-k-means:

=============
Spark K-Means
=============

A Cask Data Application Platform (CDAP) Example Demonstrating Spark.

Overview
=============

This example demonstrates a Spark application performing streaming analysis, computing the centers of points from an
input stream using the K-Means Clustering method.

Data from a sample file is sent to CDAP by the external script *inject-data* to the *pointsStream*. This data is
processed by the ``PointsFlow``, which stores the points coordinates event in its entirety in *points*, an ObjectStore Dataset.

As these entries are created, they are taken up by the *SparkKMeansProgram*, which
goes through the entries, calculates centers and tabulates results in another ObjectStore Dataset, *centers*.

Once the application completes, you can query the *centers* Dataset by using the ``centers/{index}`` endpoint
of the *CentersService*. It will respond with the center's coordinates based on the ``index`` parameter (e.g. "9.1,9.1,9.1").

Let's look at some of these elements, and then run the Application and see the results.

The SparkKMeans Application
------------------------------------------------------------

As in the other `examples <index.html>`__, the components
of the Application are tied together by the class ``SparkKMeansApp``:

.. literalinclude:: /../../../cdap-examples/SparkKMeans/src/main/java/co/cask/cdap/examples/sparkkmeans/SparkKMeansApp.java
   :language: java
   :lines: 48-80

``points`` and ``centers``: ObjectStore Data Storage
------------------------------------------------------------

The raw points data is stored in an ObjectStore Dataset, *points*.
The calculated centers data is stored in a second ObjectStore Dataset, *centers*.

``CentersService``: Service
------------------------------------------------------------

This service has a ``centers/{index}`` endpoint to obtain the center's coordinates of a given index.


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

.. highlight:: console

Running the Flow
============================================

Once an application is deployed:

- Click on the ``PointsFlow`` in the Application page of the CDAP Console to get to the
  Flow dialogue, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command-line Interface:

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start flow SparkKMeans.PointsFlow``
    * - On Windows:
      - ``> bin\cdap-cli.bat start flow SparkKMeans.PointsFlow``

Running the Spark program
============================================

Once an application is deployed:

- Click on the ``SparkKMeansProgram`` in the Application page of the CDAP Console to get to the
  Spark dialogue, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command-line Interface:

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start spark SparkKMeans.SparkKMeansProgram``
    * - On Windows:
      - ``> bin\cdap-cli.bat start spark SparkKMeans.SparkKMeansProgram``

Running the Service
============================================

Once an application is deployed:

- Click on the ``CentersService`` in the Application page of the CDAP Console to get to the
  Service dialogue, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command-line Interface:

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start service SparkKMeans.CentersService``
    * - On Windows:
      - ``> bin\cdap-cli.bat start service SparkKMeans.CentersService``

Injecting Points Data
------------------------------

Run this script to inject points data to the Stream named *pointsStream* in the
``SparkKMeans`` application::

  $ ./bin/inject-data.sh

On Windows::

  > bin\inject-data.bat

Querying the Results
------------------------------

If the Service has not already been started, you can start it either through the
CDAP Console or via an HTTP request using the ``curl`` command::

  curl -v -X POST 'http://localhost:10000/v2/apps/SparkKMeans/services/CentersService/start'

To query the *centers* ObjectStore using the ``CentersService``,
send a query via an HTTP request using the ``curl`` command. For example::

    curl -w '\n' -v 'http://localhost:10000/v2/apps/SparkKMeans/services/CentersService/methods/centers/1'

  On Windows, a copy of ``curl`` is located in the ``libexec`` directory of the SDK::

    libexec\curl...

Stopping the Flow
============================================

Once an application is deployed:

- Click on the ``PointsFlow`` in the Application page of the CDAP Console to get to the
  Flow dialogue, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command-line Interface:

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh stop flow SparkKMeans.PointsFlow``
    * - On Windows:
      - ``> bin\cdap-cli.bat stop flow SparkKMeans.PointsFlow``

Stopping the Spark program
============================================

Once an application is deployed:

- Click on the ``SparkKMeansProgram`` in the Application page of the CDAP Console to get to the
  Spark dialogue, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command-line Interface:

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh stop spark SparkKMeans.SparkKMeansProgram``
    * - On Windows:
      - ``> bin\cdap-cli.bat stop spark SparkKMeans.SparkKMeansProgram``

Stopping the Service
============================================

Once an application is deployed:

- Click on the ``CentersService`` in the Application page of the CDAP Console to get to the
  Service dialogue, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command-line Interface:

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh stop service SparkKMeans.CentersService``
    * - On Windows:
      - ``> bin\cdap-cli.bat stop service SparkKMeans.CentersService``

Stopping the Application
-------------------------------
Once done, you can stop the application as described above in `Stopping an Application. <#stopping-an-application>`__
