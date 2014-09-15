.. :Author: Cask Data, Inc.
   :Description: Cask Data Application Platform SparkKMeans Application
   :Copyright: Copyright © 2014 Cask Data, Inc.
=================================
SparkKMeans Application Example
=================================

**A Cask Data Application Platform (CDAP) Example Demonstrating Spark**

Overview
========
This example demonstrates a Spark application performing streaming analysis, computing the centers of points from an
input stream using KMeans Clustering method.

Data from a sample file will be sent to the CDAP by an external script *inject-data* to the *pointsStream*. This data is
processed by the ``PointsFlow``, which stores the points coordinates event in its entirety in *points*, an ObjectStore Dataset.

As these entries are created, they are taken up by the *SparkKMeansProgram*, which
goes through the entries, calculates centers and tabulates results in another ObjectStore Dataset, *centers*.

Once the application completes, you can query the *centers* Dataset by using the ``centers`` method of the *CentersProcedure*. It will
send back a JSON-formatted result with center's coordinates based on ``index`` parameter.

Let's look at some of these elements, and then run the Application and see the results.

The SparkKMeans Application
--------------------------------
As in the other `examples <index.html>`__, the components
of the Application are tied together by the class ``SparkKMeansApp``::

  public class SparkKMeansApp extends AbstractApplication {

    public static final Charset UTF8 = Charset.forName("UTF-8");

    @Override
    public void configure() {
      setName("SparkKMeans");
      setDescription("Spark KMeans app");

      // Ingest data into the Application via a Stream
      addStream(new Stream("pointsStream"));

      // Process points data in real-time using a Flow
      addFlow(new PointsFlow());

      // Run a Spark program on the acquired data
      addSpark(new SparkKMeansSpecification());

      // Query the processed data using a Procedure
      addProcedure(new CentersProcedure());

      // Store input and processed data in ObjectStore Datasets
      try {
        ObjectStores.createObjectStore(getConfigurer(), "points", String.class);
        ObjectStores.createObjectStore(getConfigurer(), "centers", String.class);
      } catch (UnsupportedTypeException e) {
        // This exception is thrown by ObjectStore if its parameter type cannot be
        // (de)serialized (for example, if it is an interface and not a class, then there is
        // no auto-magic way deserialize an object.) In this case that will not happen
        // because String is an actual classes.
        throw new RuntimeException(e);
      }
    }
  }

``points`` and ``centers``: ObjectStore Data Storage
--------------------------------------------------------------
The raw points data is stored in an ObjectStore Dataset, *points*.
The calculated centers data is stored in an ObjectStore Dataset, *centers*.

``CentersProcedure``: Procedure
--------------------------------
This procedure has a ``centers`` method to obtain the center's coordinates of a given index.


Building and Running the Application and Example
================================================

.. highlight:: console

In the remainder of this document, we refer to the CDAP runtime as "CDAP", and the
example code that is running on it as an "Application".

We show the Windows prompt as ``~SDK>`` to indicate a command prompt opened in the SDK directory.

In this example, you need to build the app from source and then deploy the compiled JAR file.
You start the CDAP, deploy the app, start the Flow and then run the example by
injecting points data into the Stream.

When finished, stop the Application as described below.

Building the SparkKMeans Application
-----------------------------------------
From the project root, build ``SparkKMeans`` with the
`Apache Maven <http://maven.apache.org>`__ command::

	$ mvn clean package

(If you modify the code and would like to rebuild the Application, you can
skip the tests by using the command::

	$ mvn -Dmaven.test.skip=true clean package


Deploying and Starting the Application
--------------------------------------
Make sure an instance of the CDAP is running and available.
From within the SDK root directory, this command will start CDAP in local mode::

  On UNIX::

	$ ./bin/cdap.sh start

  On Windows::

	~SDK> bin\cdap.bat start

From within the CDAP Console (`http://localhost:9999/ <http://localhost:9999/>`__ in local mode):

#. Drag and drop the Application .JAR file (``target/SparkKMeans-<version>.jar``)
   onto your browser window.
   Alternatively, use the *Load App* button found on the *Overview* of the CDAP Console.
#. Once loaded, select the ``SparkKMeans`` Application from the list.
   On the Application's detail page, click the *Start* button on **both** the *Process* and *Query* lists.

On Windows:

#. To deploy the App JAR file, run ``~SDK> bin\app-manager.bat deploy`` or drag and drop the
   Application .JAR file (``target/SparkKMeans-<version>.jar`` onto your browser window.
   (Currently, the *Load App* button does not work under Windows.)
#. To start the App, run ``~SDK> bin\app-manager.bat start``

On UNIX:

#. To deploy the App JAR file, run ``$ ./bin/app-manager.sh --action deploy`` or drag and drop the
   Application .JAR file (``target/SparkKMeans-<version>.jar`` onto your browser window.
   (Currently, the *Load App* button does not work under Windows.)
#. To start the App, run ``$ ./bin/app-manager.sh --action start``

Running the Example
-------------------

Injecting points data
............................

Run this script to inject points data
to the Stream named *pointsStream* in the ``SparkKMeans`` application::

  On UNIX::

	$ ./bin/inject-data.sh [--host <hostname>]

:Note:	``[--host <hostname>]`` is not available for a *Local CDAP*.

  On Windows::

	~SDK> bin\inject-data.bat

Running Spark program
.............................

There are three ways to start Spark program:

1. Click on the ``SparkKMeansProgram`` in the Application page of the CDAP Console to get to the
   Spark dialogue, then click the *Start* button.

2. Send a query via an HTTP request using the ``curl`` command::

  On UNIX::

  curl -v -d '{args="3"}' \
    	  -X POST 'http://localhost:10000/v2/apps/SparkKMeansProgram/spark/SparkKMeansProgram/start'

  On Windows:

  Copy of ``curl`` is located in the ``libexec`` directory of the example::

	  libexec\curl...

3. Use command:

  On UNIX::

  $ ./bin/app-manager.sh --action run

  On Windows::

	~SDK> bin\app-manager.bat run

Querying the Results
....................
If the Procedure has not already been started, you start it either through the 
CDAP Console or via an HTTP request using the ``curl`` command::

	curl -v -X POST 'http://localhost:10000/v2/apps/SparkKMeans/procedures/CentersProcedure/start'
	
There are two ways to query the *centers* ObjectStore through the ``CentersProcedure`` procedure:

1. Send a query via an HTTP request using the ``curl`` command. For example::

  On UNIX::

	curl -v -d '{"index": "1"}' \
	  -X POST 'http://localhost:10000/v2/apps/SparkKMeans/procedures/CentersProcedure/methods/centers'

  On Windows:

  Copy of ``curl`` is located in the ``libexec`` directory of the example::

	  libexec\curl...

2. Click on the ``CentersProcedure`` in the Application page of the Console to get to the
   Procedure dialogue. Type in the method name ``centers``, and enter the index in the parameters
   field, such as::

	{ "index" : "1" }

   Then click the *Execute* button. The center's coordinates will be displayed in the
   Console in JSON format, for example::

	"9.1,9.1,9.1"

Stopping the Application
---------------------------
Either:

- On the Application detail page of the CDAP Console, click the *Stop* button on **both** the *Process* and *Query* lists; 

or:

- Run ``$ ./bin/app-manager.sh --action stop [--host <hostname>]``

  :Note:	[--host <hostname>] is not available for a *Local CDAP*.

  On Windows, run ``~SDK> bin\app-manager.bat stop``

.. highlight:: java

Downloading the Example
=======================
This example (and more!) is included with our `software development kit <http://cask.co/download>`__.
