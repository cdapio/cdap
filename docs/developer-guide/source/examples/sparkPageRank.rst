.. :Author: Cask Data, Inc.
   :Description: Cask Data Application Platform SparkPageRank Application

============================
SparkPageRank Application Example
============================

**A Cask Data Application Platform (CDAP) Example demonstrating Spark**

Overview
========
This example demonstrates use of each of the CDAP elements: Streams, Flows, Flowlets,
Datasets, Procedures and Spark in a single Application.

This example demonstrates an application of streaming log analysis.
It computes the page rank based on information about neighbor URLs.

Data from a sample file will be sent to the CDAP by an external script *inject-data*
to the *neighborURLStream*. The data are processed by the
``URLPairFlow``, which stores the url pair event in its entirety in *neighborURLs*, an ObjectStore Dataset.

As these entries are created, they are taken up by the *SparkPageRankProgram* Spark job, which
goes through the entries, calculates page rank and tabulates results in another ObjectStore Dataset, *ranks*.

Finally, you can query the *ranks* Dataset by using the ``rank`` method of the *RanksProcedure*. It will
send back a JSON-formatted result with page rank based on ``url`` parameter.

Let's look at some of these elements, and then run the Application and see the results.

The SparkPageRank Application
------------------------
As in the other `examples <index.html>`__, the components
of the Application are tied together by the class ``SparkPageRankApp``::

  public class SparkPageRankApp extends AbstractApplication {

    public static final Charset UTF8 = Charset.forName("UTF-8");

    @Override
    public void configure() {
      setName("SparkPageRank");
      setDescription("Spark page rank app");

      // Ingest data into the Application via Streams
      addStream(new Stream("neighborURLStream"));

      // Process url pairs in real-time using Flows
      addFlow(new URLPairFlow());

      // Run a Spark program on the acquired data
      addSpark(new SparkPageRankSpecification());

      // Query the processed data using a Procedure
      addProcedure(new RanksProcedure());

      // Store input and processed data in ObjectStore Datasets
      try {
        ObjectStores.createObjectStore(getConfigurer(), "neighborURLs", String.class);
        ObjectStores.createObjectStore(getConfigurer(), "ranks", Double.class);
      } catch (UnsupportedTypeException e) {
        // This exception is thrown by ObjectStore if its parameter type cannot be
        // (de)serialized (for example, if it is an interface and not a class, then there is
        // no auto-magic way deserialize an object.) In this case that will not happen
        // because String and Double are actual classes.
        throw new RuntimeException(e);
      }
    }
  }

``neighborURLs`` and ``ranks``: ObjectStore Data Storage
--------------------------------------------------------------
The raw url pair data is stored in an ObjectStore Dataset, *neighborURLs*.
The calculated page rank data is stored in an ObjectStore Dataset, *ranks*.

``RanksProcedure``: Procedure
--------------------------------
This procedure has a ``rank`` method to obtain the page rank of a given url.


Building and Running the Application and Example
================================================

.. highlight:: console

In this remainder of this document, we refer to the CDAP runtime as "CDAP", and the
example code that is running on it as an "Application".

We show the Windows prompt as ``~SDK>`` to indicate a command prompt opened in the SDK directory.

In this example, you need to build the app from source and then deploy the compiled JAR file.
You start the CDAP, deploy the app, start the Flow and then run the example by
injecting sentence entries into the stream.

When finished, stop the Application as described below.

Building the SparkPageRank Application
----------------------------------
From the project root, build ``SparkPageRank`` with the
`Apache Maven <http://maven.apache.org>`__ command::

	$ mvn clean package

(If you modify the code and would like to rebuild the Application, you can
skip the tests by using the command::

	$ mvn -Dmaven.test.skip=true clean package


Deploying and Starting the Application
--------------------------------------
Make sure an instance of the CDAP is running and available.
From within the SDK root directory, this command will start CDAP in local mode::

	$ ./bin/cdap.sh start

On Windows::

	~SDK> bin\cdap.bat start

From within the CDAP Console (`http://localhost:9999/ <http://localhost:9999/>`__ in local mode):

#. Drag and drop the Application .JAR file (``target/SparkPageRank-<version>.jar``)
   onto your browser window.
   Alternatively, use the *Load App* button found on the *Overview* of the CDAP Console.
#. Once loaded, select the ``SparkPageRank`` Application from the list.
   On the Application's detail page, click the *Start* button on **both** the *Process* and *Query* lists.

On Windows:

#. To deploy the App JAR file, run ``~SDK> bin\app-manager.bat deploy`` or drag and drop the
   Application .JAR file (``target/SparkPageRank-<version>.jar`` onto your browser window.
   (Currently, the *Load App* button does not work under Windows.)
#. To start the App, run ``~SDK> bin\app-manager.bat start``

Running the Example
-------------------

Injecting Sentences
............................

Run this script to inject sentences 
to the Stream named *neighborURLStream* in the ``SparkPageRank`` application::

	$ ./bin/inject-data.sh [--host <hostname>]

:Note:	``[--host <hostname>]`` is not available for a *Local CDAP*.

On Windows::

	~SDK> bin\inject-data.bat


Querying the Results
....................
If the Procedure has not already been started, you start it either through the 
CDAP Console or via an HTTP request using the ``curl`` command::

	curl -v -X POST 'http://localhost:10000/v2/apps/SparkPageRank/procedures/RanksProcedure/start'
	
There are two ways to query the *ranks* ObjectStore through the ``RanksProcedure`` procedure:

1. Send a query via an HTTP request using the ``curl`` command. For example::

	curl -v -d '{"url": "http://example.com/page1"}' \
	  -X POST 'http://localhost:10000/v2/apps/SparkPageRank/procedures/RanksProcedure/methods/rank'

  On Windows, a copy of ``curl`` is located in the ``libexec`` directory of the example::

	  libexec\curl...

2. Click on the ``RanksProcedure`` in the Application page of the Console to get to the
   Procedure dialogue. Type in the method name ``rank``, and enter the url in the parameters
   field, such as::

	{ "url" : "http://example.com/page1" }

   Then click the *Execute* button. The rank for that url will be displayed in the
   Console in JSON format, for example [reformatted to fit]::

	"1.7555683262150241"

Stopping the Application
------------------------
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
