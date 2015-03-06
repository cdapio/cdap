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

The raw points data is stored in an ObjectStore Dataset, *points*.
The calculated centers data is stored in a second ObjectStore Dataset, *centers*.

``CentersService``: Service
------------------------------------------------------------

This service has a ``centers/{index}`` endpoint to obtain the center's coordinates of a given index.


Building and Starting
=================================

- You can either build the example (as described `below
  <#building-an-example-application>`__) or use the pre-built JAR file included in the CDAP SDK.
- Start CDAP, deploy and start the application and its components as described below in 
  `Running CDAP Applications`_\ .
  Make sure you start the Flow and Service as described below.
- Once the application has been deployed and started, you can `run the example. <#running-the-example>`__

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

- Click on the *Process* button in the left sidebar of the CDAP Console,
  then click *PointsFlow* in the *Process* page to get to the
  Flow detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start flow SparkKMeans.PointsFlow``
    * - On Windows:
      - ``> bin\cdap-cli.bat start flow SparkKMeans.PointsFlow``    

Starting the Service
------------------------------

Once the application is deployed:

- Click on *SparkKMeans* in the Overview page of the CDAP Console to get to the
  Application detail page, click *CentersService* in the *Service* pane to get to the
  Service detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start service SparkKMeans.CentersService``
    * - On Windows:
      - ``> bin\cdap-cli.bat start service SparkKMeans.CentersService``    

Injecting Points Data
------------------------------

Run this script to inject points data to the Stream named *pointsStream* in the
``SparkKMeans`` application:

.. list-table::
  :widths: 20 80
  :stub-columns: 1

  * - On Linux:
    - ``$ ./bin/inject-data.sh``
  * - On Windows:
    - ``> bin\inject-data.bat``    

Running the Spark program
------------------------------
There are three ways to start the Spark program:

1. Click on the *Process* button in the left sidebar of the CDAP Console,
   then click *SparkKMeansProgram* in the *Process* page to get to the
   Spark detail page, then click the *Start* button; or

#. Send a query via an HTTP request using the ``curl`` command::

    curl -w'\n' -v  -d '{args="3"}' \
      http://localhost:10000/v3/namespaces/default/apps/SparkKMeans/spark/SparkKMeansProgram/start

   **Note:** A version of ``curl`` that works with Windows is included in the CDAP Standalone
   SDK in ``libexec\bin\curl.exe``

#. Use the Command Line Interface:

   .. list-table::
     :widths: 20 80
     :stub-columns: 1

     * - On Linux:
       - ``$ ./bin/cdap-cli.sh start spark SparkKMeans.SparkKMeansProgram``
     * - On Windows:
       - ``> bin\cdap-cli.bat start spark SparkKMeans.SparkKMeansProgram``    

Querying the Results
------------------------------

To query the *centers* ObjectStore using the ``CentersService``,
send a query via an HTTP request using the ``curl`` command. For example::

    curl -w'\n' -v http://localhost:10000/v3/namespaces/default/apps/SparkKMeans/services/CentersService/methods/centers/1

**Note:** A version of ``curl`` that works with Windows is included in the CDAP Standalone
SDK in ``libexec\bin\curl.exe``

You can also use the Command Line Interface:

.. list-table::
  :widths: 20 80
  :stub-columns: 1

  * - On Linux:
    - ``$ ./bin/cdap-cli.sh call service SparkKMeans.CentersService GET centers/1``
  * - On Windows:
    - ``> bin\cdap-cli.bat call service SparkKMeans.CentersService GET centers/1``


Stopping the Application
-------------------------------
Once done, you can stop the application as described above in `Stopping an Application. 
<#stopping-an-application>`__ Here is an example-specific description of the steps:

**Stopping the Flow and Spark Program**

- Click on the *Process* button in the left sidebar of the CDAP Console,
  then click *PointsFlow* in the *Process* page to get to the
  Flow detail page, then click the *Stop* button; similarly for the *SparkKMeansProgram*: or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh stop flow SparkKMeans.PointsFlow``
    * -
      - ``$ ./bin/cdap-cli.sh stop flow SparkKMeans.SparkKMeansProgram``
    * - On Windows:
      - ``> bin\cdap-cli.bat stop flow SparkKMeans.PointsFlow``    
    * - 
      - ``> bin\cdap-cli.bat stop flow SparkKMeans.SparkKMeansProgram``    

**Stopping the Service**

- Click on *SparkKMeans* in the Overview page of the CDAP Console to get to the
  Application detail page, click *CentersService* in the *Service* pane to get to the
  Service detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start flow SparkKMeans.CentersService``
    * - On Windows:
      - ``> bin\cdap-cli.bat start flow SparkKMeans.CentersService``    
