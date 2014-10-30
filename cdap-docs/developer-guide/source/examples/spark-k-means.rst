.. :Author: Cask Data, Inc.
   :Description: Cask Data Application Platform SparkKMeans Application
   :Copyright: Copyright © 2014 Cask Data, Inc.

Spark K-Means
-------------

A Cask Data Application Platform (CDAP) Example Demonstrating Spark.

Overview
........

This example demonstrates a Spark application performing streaming analysis, computing the centers of points from an
input stream using the K-Means Clustering method.

Data from a sample file is sent to CDAP by the external script *inject-data* to the *pointsStream*. This data is
processed by the ``PointsFlow``, which stores the points coordinates event in its entirety in *points*, an ObjectStore Dataset.

As these entries are created, they are taken up by the *SparkKMeansProgram*, which
goes through the entries, calculates centers and tabulates results in another ObjectStore Dataset, *centers*.

Once the application completes, you can query the *centers* Dataset by using the ``centers`` method of the *CentersProcedure*. It will
send back a JSON-formatted result with the center's coordinates based on the ``index`` parameter.

Let's look at some of these elements, and then run the Application and see the results.

The SparkKMeans Application
...........................

As in the other `examples <index.html>`__, the components
of the Application are tied together by the class ``SparkKMeansApp``::

  public class SparkKMeansApp extends AbstractApplication {

    public static final Charset UTF8 = Charset.forName("UTF-8");

    @Override
    public void configure() {
      setName("SparkKMeans");
      setDescription("Spark K-Means app");

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
        // because String is an actual class.
        throw new RuntimeException(e);
      }
    }
  }

``points`` and ``centers``: ObjectStore Data Storage
++++++++++++++++++++++++++++++++++++++++++++++++++++

The raw points data is stored in an ObjectStore Dataset, *points*.
The calculated centers data is stored in a second ObjectStore Dataset, *centers*.

``CentersProcedure``: Procedure
+++++++++++++++++++++++++++++++

This procedure has a ``centers`` method to obtain the center's coordinates of a given index.


Deploy and start the application as described in :ref:`Building and Running Applications <cdap-building-running>`

Running the Example
+++++++++++++++++++

Injecting points data
#####################

Run this script to inject points data
to the Stream named *pointsStream* in the ``SparkKMeans`` application::

	$ ./bin/inject-data.sh

On Windows::

	~SDK> bin\inject-data.bat

Running the Spark program
#########################

There are three ways to start the Spark program:

1. Click on the ``SparkKMeansProgram`` in the Application page of the CDAP Console to get to the
   Spark dialogue, then click the *Start* button.

2. Send a query via an HTTP request using the ``curl`` command::

     curl -v -d '{args="3"}' \
    	 'http://localhost:10000/v2/apps/SparkKMeansProgram/spark/SparkKMeansProgram/start'

   On Windows, a copy of ``curl`` is located in the ``libexec`` directory of the SDK::

	  libexec\curl...

3. Use the command::

    $ ./bin/app-manager.sh --action run

  On Windows::

	~SDK> bin\app-manager.bat run

Querying the Results
####################

If the Procedure has not already been started, you start it either through the 
CDAP Console or via an HTTP request using the ``curl`` command::

	curl -v -d 'http://localhost:10000/v2/apps/SparkKMeans/procedures/CentersProcedure/start'
	
There are two ways to query the *centers* ObjectStore through the ``CentersProcedure`` procedure:

1. Send a query via an HTTP request using the ``curl`` command. For example::

	 curl -v -d '{"index": "1"}' \
	   'http://localhost:10000/v2/apps/SparkKMeans/procedures/CentersProcedure/methods/centers'

   On Windows, a copy of ``curl`` is located in the ``libexec`` directory of the SDK::

	  libexec\curl...

2. Type a Procedure method name, in this case CentersProcedure, in the Query page of the CDAP Console:

   In the CDAP Console:

   #. Click the *Query* button.
   #. Click on the *CentersProcedure* Procedure.
   #. Type ``centers`` in the *Method* text box.
   #. Type the parameters required for this method, a JSON string with the name *index* and
      value of the index "1"::

        { "index" : "1" }

   #. Click the *Execute* button.
   #. The center's coordinates will be displayed in the Console in JSON format. For example::

	   "9.1,9.1,9.1"

Once done, you can stop the application as described in :ref:`Building and Running Applications. <cdap-building-running>`

.. highlight:: java
