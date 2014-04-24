.. :Author: John Jackson
   :Description: Introduction to Programming Applications for the Continuuity Reactor

===============================================
Continuuity Reactor Testing and Debugging Guide
===============================================

-------------------------------------------------------------------------------------
Introduction to Testing and Trouble-Shooting Applications for the Continuuity Reactor
-------------------------------------------------------------------------------------

.. reST Editor: section-numbering::

.. reST Editor: contents::

Testing Reactor Applications
============================

Strategies in Testing Applications
----------------------------------

The Reactor comes with a convenient way to unit test your Applications.
The base for these tests is ReactorTestBase, which is packaged
separately from the API in its own artifact because it depends on the
Reactor’s runtime classes. You can include it in your test dependencies
in one of two ways:

- include all JAR files in the lib directory of the Reactor Development Kit installation,
  or
- include the continuuity-test artifact in your Maven test dependencies
  (see the ``pom.xml`` file of the *WordCount* example).

Note that for building an application, you only need to include the
Reactor API in your dependencies. For testing, however, you need the
Reactor run-time. To build your test case, extend the
``ReactorTestBase`` class.

Strategies in Testing Flows
---------------------------
Let’s write a test case for the *WordCount* example::

	public class WordCountTest extends ReactorTestBase {
	  @Test
	  public void testWordCount() throws Exception {


The first thing we do in this test is deploy the application,
then we’ll start the Flow and the Procedure::

	  // Deploy the Application
	  ApplicationManager appManager = deployApplication(WordCount.class);
	  // Start the Flow and the Procedure
	  FlowManager flowManager = appManager.startFlow("WordCounter");
	  ProcedureManager procManager = appManager.startProcedure("RetrieveCount");

Now that the Flow is running, we can send some events to the Stream::

	  // Send a few events to the Stream
	  StreamWriter writer = appManager.getStreamWriter("wordStream");
	  writer.send("hello world");
	  writer.send("a wonderful world");
	  writer.send("the world says hello");

To wait for all events to be processed, we can get a metrics observer
for the last Flowlet in the pipeline (the word associator) and wait for
its processed count to either reach 3 or time out after 5 seconds::

	  // Wait for the events to be processed, or at most 5 seconds
	  RuntimeMetrics metrics = RuntimeStats.
	    getFlowletMetrics("WordCount", "WordCounter", "associator");
	  metrics.waitForProcessed(3, 5, TimeUnit.SECONDS);

Now we can start verifying that the processing was correct by obtaining
a client for the Procedure, and then submitting a query for the global
statistics::

	  // Call the Procedure
	  ProcedureClient client = procManager.getClient();
	  // Query global statistics
	  String response = client.query("getStats", Collections.EMPTY_MAP);

If the query fails for any reason this method would throw an exception.
In case of success, the response is a JSON string. We must deserialize
the JSON string to verify the results::

	  Map<String, String> map = new Gson().fromJson(response, stringMapType);
	  Assert.assertEquals("9", map.get("totalWords"));
	  Assert.assertEquals("6", map.get("uniqueWords"));
	  Assert.assertEquals(((double)42)/9,
	    (double)Double.valueOf(map.get("averageLength")), 0.001);

Then we ask for the statistics of one of the words in the test events.
The verification is a little more complex, because we have a nested map
as a response, and the value types in the top-level map are not uniform::

	  // Verify some statistics for one of the words
	  response = client.query("getCount", ImmutableMap.of("word","world"));
	  Map<String, Object> omap = new Gson().fromJson(response, objectMapType);
	  Assert.assertEquals("world", omap.get("word"));
	  Assert.assertEquals(3.0, omap.get("count"));
	  // The associations are a map within the map
	  Map<String, Double> assocs = (Map<String, Double>) omap.get("assocs");
	  Assert.assertEquals(2.0, (double)assocs.get("hello"), 0.000001);
	  Assert.assertTrue(assocs.containsKey("hello"));
	}

Strategies in Testing MapReduce
-------------------------------
In a fashion similar to `Strategies in Testing Flows`_, we can write
unit testing for MapReduce jobs. Let's write a test case for an
application that uses MapReduce. Complete source code and test can be
found under `TrafficAnalytics </developers/examples/TrafficAnalytics/>`__.

The ``TrafficAnalyticsTest`` class should extend from
``ReactorTestBase`` similar to `Strategies in Testing Flows`.

::

	public class TrafficAnalyticsTest extends ReactorTestBase {
	  @Test
  	  public void test() throws Exception {

The ``TrafficAnalytics`` application can be deployed using the ``deployApplication`` 
method from the ``ReactorTestBase`` class::

	// Deploy an Application
	ApplicationManager appManager = deployApplication(TrafficAnalyticsApp.class);

The MapReduce job reads from the ``logEventTable`` DataSet. As a first
step, the data to the ``logEventTable`` should be populated by running
the ``RequestCountFlow`` and sending the data to the ``logEventStream``
Stream::

	FlowManager flowManager = appManager.startFlow("RequestCountFlow");
	// Send data to the Stream
	sendData(appManager, now);
	// Wait for the last Flowlet to process 3 events or at most 5 seconds
	RuntimeMetrics metrics = RuntimeStats.
	    getFlowletMetrics("TrafficAnalytics", "RequestCountFlow", "collector");
	metrics.waitForProcessed(3, 5, TimeUnit.SECONDS);

Start the MapReduce job and wait for a maximum of 60 seconds::

	// Start the MapReduce job.
	MapReduceManager mrManager = appManager.startMapReduce("RequestCountMapReduce");
	mrManager.waitForFinish(60, TimeUnit.SECONDS);

We can start verifying that the MapReduce job was run correctly by
obtaining a client for the Procedure, and then submitting a query for
the counts::

	ProcedureClient client = procedureManager.getClient();

	// Verify the query.
	String response = client.query("getCounts", Collections.<String, String>emptyMap());
	// Deserialize the JSON string.
	Map<Long, Integer> result = GSON.
	    fromJson(response, new TypeToken<Map<Long, Integer>>(){}.getType());
	Assert.assertEquals(2, result.size());

The assertion will verify that the correct result was received.

Debugging Reactor Applications
==============================

Debugging an Application in Local Reactor
-----------------------------------------
Any Continuuity Reactor Application can be debugged in the Local Reactor
by attaching a remote debugger to the Reactor JVM. To enable remote
debugging:

#. Start the Local Reactor with the ``--enable-debug`` option specifying ``port 5005``.

   The Reactor should confirm that the debugger port is open with a message such as
   ``Remote debugger agent started on port 5005``.

#. Deploy the *HelloWorld* Application to the Reactor by dragging and dropping the
   ``HelloWorld.jar`` file from the ``/examples/HelloWorld`` directory onto the Reactor
   Dashboard.

#. Open the *HelloWorld* Application in an IDE and connect to the remote debugger.

For more information, see `Attaching a Debugger`_.

:Note:	Currently, debugging is not supported under Windows.

Debugging an Application in Distributed Reactor
-----------------------------------------------

In distributed mode, an application does not run in a single JVM. Instead, its programs
are dispersed over multiple if not many containers in the Hadoop cluster. Therefore
there is no single place to debug the entire application. You can, however, debug every
individual container by attaching a remote debugger to it. This is supported for each
flowlet of a flow and each instances of a procedure. In order to debug a container, you
need to start the program with debugging enabled, by making an HTTP request to the
program’s URL, for example, the following will start a flow for debugging::

	POST <base-url>/apps/WordCount/flows/WordCounter/debug

Note that this URL differs from the URL for starting the flow only by the last path
component (``debug`` instead of ``start``,
see `Reactor Client HTTP API <developer/rest#reactor-client-http-api>`_),
and you can pass in runtime arguments in the exact same way as if you normally start a flow.
Once the flow is running, each flowlet
will detect an available port in its container and open that port for attaching a debugger.
To find out the address of a container’s host and the container’s debug port, you can query
the reactor for the flow’s live info via HTTP::

	GET <base-url>/apps/WordCount/flows/WordCounter/live-info

The response is formatted in JSON and - pretty-printed - looks similar to this::

  {
    "app": "WordCount",
    "containers": [
      {
        "container": "container_1397069870124_0010_01_000002",
        "debugPort": 42071,
        "host": "node-1004.my.cluster.net",
        "instance": 0,
        "memory": 512,
        "name": "unique",
        "type": "flowlet",
        "virtualCores": 1
      },
      ...
      {
        "container": "container_1397069870124_0010_01_000005",
        "debugPort": 37205,
        "host": "node-1003.my.cluster.net",
        "instance": 0,
        "memory": 512,
        "name": "splitter",
        "type": "flowlet",
        "virtualCores": 1
      }
    ],
    "id": "WordCounter",
    "runtime": "distributed",
    "type": "Flow",
    "yarnAppId": "application_1397069870124_0010"
  }

You see the YARN application id and also the YARN container IDs of each flowlet. More importantly,
see the host name and debugging port for each flowlet. For example, here the only instance of the
splitter flowlet run on node-1003.my.cluster.net and the debugging port is 37205. You can now
attach your debugger to the container’s JVM (see `Attaching a Debugger`_).

The corresponding HTTP requests for the RetrieveCounts procedure of this application are::

	POST <base-url>/apps/WordCount/procedures/RetrieveCounts/debug
	GET <base-url>/apps/WordCount/procedures/RetrieveCounts/live-info

Attaching a Debugger
--------------------

Debugging with IntelliJ
.......................

#. From the *IntelliJ* toolbar, select ``Run -> Edit Configurations``.
#. Click ``+`` and choose ``Remote Configuration``:

   .. image:: _images/IntelliJ_1.png

#. Create a debug configuration by entering a name, for example, ``Continuuity``.
#. Enter the host name, for example, ``localhost`` or ``node-1003.my.cluster.net``
   in the Port field:
#. Enter the debugging port, for example, ``5005`` in the Port field:

   .. image:: _images/IntelliJ_2.png

#. To start the debugger, select ``Run -> Debug -> Continuuity``.
#. Set a breakpoint in any code block, for example, a Flowlet method:

   .. image:: _images/IntelliJ_3.png

#. Start the Flow in the Dashboard.
#. Send an event to the Stream. The control will stop at the breakpoint
   and you can proceed with debugging.


Debugging with Eclipse
......................

#. In Eclipse, select ``Run-> Debug`` configurations.
#. In the pop-up, select ``Remote Java application``.
#. Enter a name, for example, ``Continuuity``.
#. Enter the host name, for example, ``localhost`` or ``node-1003.my.cluster.net``
   in the Port field:
#. Enter the debugging port, for example, ``5005`` in the Port field:
#. Click ``Debug`` to start the debugger:

   .. image:: _images/Eclipse_1.png

#. Set a breakpoint in any code block, for example, a Flowlet method:

   .. image:: _images/Eclipse_2.png

#. Start the Flow in the Dashboard.
#. Send an event to the Stream.
#. The control stops at the breakpoint and you can proceed with debugging.

.. Unit testing [rev 2]
.. ------------

.. Local Continuuity Reactor [rev 2]
.. -------------------------

Where to Go Next
================
Now that you've had an introduction to Continuuity Reactor, take a look at:

- `Operating a Continuuity Reactor <operations>`__,
  which covers putting Continuuity Reactor into production.
