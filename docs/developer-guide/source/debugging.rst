.. :Author: Continuuity, Inc.
   :Description: Introduction to Testing, Debugging, and Troubleshooting Continuuity Reactor Applications

===============================================
Testing and Debugging Guide
===============================================

**Introduction to Testing, Debugging, and Troubleshooting Continuuity Reactor Applications**

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

Testing Reactor Applications
============================

Strategies in Testing Applications
----------------------------------

The Reactor comes with a convenient way to unit test your Applications.
The base for these tests is ``ReactorTestBase``, which is packaged
separately from the API in its own artifact because it depends on the
Reactor’s runtime classes. You can include it in your test dependencies
in one of two ways:

- include all JAR files in the ``lib`` directory of the Continuuity Reactor Development Kit installation,
  or
- include the ``continuuity-test`` artifact in your Maven test dependencies
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
for the last Flowlet in the pipeline (the "word associator") and wait for
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

Strategies in Testing MapReduce Jobs
------------------------------------
In a fashion similar to `Strategies in Testing Flows`_, we can write
unit testing for MapReduce jobs. Let's write a test case for an
application that uses MapReduce. Complete source code and test can be
found under `TrafficAnalytics </examples/TrafficAnalytics/index.html>`__.

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

The MapReduce job reads from the ``logEventTable`` Dataset. As a first
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

Validating Test Data with SQL
-----------------------------
Often the easiest way to verify that a test produced the right data is to run a SQL query - if the data sets involved
in the test case are record-scannable as described in `Querying Datasets with SQL <query.html>`__. 
This can be done using a JDBC connection obtained from the test base::


  // Obtain a JDBC connection
  Connection connection = getQueryClient();
  try {
      // Run a query over the dataset
      results = connection.prepareStatement("SELECT key FROM mytable WHERE value = '1'").executeQuery();
      Assert.assertTrue(results.next());
      Assert.assertEquals("a", results.getString(1));
      Assert.assertTrue(results.next());
      Assert.assertEquals("c", results.getString(1));
      Assert.assertFalse(results.next());

    } finally {
      results.close();
      connection.close();
    }

The JDBC connection does not implement the full JDBC functionality: it does not allow variable replacement and
will not allow you to make any changes to datasets. But it is sufficient to perform test validation: you can create
or prepare statements and execute queries, then iterate over the results set and validate its correctness.


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

#. Deploy (for example) the *HelloWorld* Application to the Reactor by dragging and dropping the
   ``HelloWorld.jar`` file from the ``/examples/HelloWorld`` directory onto the Reactor
   Dashboard.

#. Open the *HelloWorld* Application in an IDE and connect to the remote debugger.

For more information, see `Attaching a Debugger`_.

:Note:	Currently, debugging is not supported under Windows.

Debugging an Application in Distributed Reactor
-----------------------------------------------

In distributed mode, an application does not run in a single JVM. Instead, its programs
are dispersed over multiple—if not many—containers in the Hadoop cluster. There is no 
single place to debug the entire application. 

You can, however, debug every individual container by attaching a remote debugger to it. 
This is supported for each Flowlet of a Flow and each instance of a Procedure. In order
to debug a container, you need to start the element with debugging enabled by making 
an HTTP request to the element’s URL. For example, the following will start a Flow for debugging::

	POST <base-url>/apps/WordCount/flows/WordCounter/debug

Note that this URL differs from the URL for starting the Flow only by the last path
component (``debug`` instead of ``start``; see 
`Reactor Client HTTP API <rest.html#reactor-client-http-api>`__). You can pass in 
runtime arguments in the exact same way as you normally would start a Flow.

Once the Flow is running, each Flowlet will detect an available port in its container
and open that port for attaching a debugger.
To find out the address of a container’s host and the container’s debug port, you can query
the Reactor for a Procedure or Flow’s live info via HTTP::

	GET <base-url>/apps/WordCount/flows/WordCounter/live-info

The response is formatted in JSON and—pretty-printed— would look similar to this::

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

You see the YARN application id and the YARN container IDs of each Flowlet. More importantly, you
can see the host name and debugging port for each Flowlet. For example, the only instance of the
splitter Flowlet is running on ``node-1003.my.cluster.net`` and the debugging port is 37205. You can now
attach your debugger to the container’s JVM (see `Attaching a Debugger`_).

The corresponding HTTP requests for the ``RetrieveCounts`` Procedure of this application would be::

	POST <base-url>/apps/WordCount/procedures/RetrieveCounts/debug
	GET <base-url>/apps/WordCount/procedures/RetrieveCounts/live-info

Analysis of the response would give you the host names and debugging ports for all instances of the Procedure.

Attaching a Debugger
--------------------

Debugging with IntelliJ
.......................

#. From the *IntelliJ* toolbar, select ``Run -> Edit Configurations``.
#. Click ``+`` and choose ``Remote Configuration``:

   .. image:: _images/IntelliJ_1.png

#. Create a debug configuration by entering a name, for example, ``Continuuity``.
#. Enter the host name, for example, ``localhost`` or ``node-1003.my.cluster.net``
   in the Port field.
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
#. Enter the debugging port, for example, ``5005`` in the Port field.
#. Click ``Debug`` to start the debugger:

   .. image:: _images/Eclipse_1.png

#. Set a breakpoint in any code block, for example, a Flowlet method:

   .. image:: _images/Eclipse_2.png

#. Start the Flow in the Dashboard.
#. Send an event to the Stream.
#. The control stops at the breakpoint and you can proceed with debugging.


Debugging the Transaction Manager (Advanced Use)
------------------------------------------------
In this advanced use section, we will explain in depth how transactions work internally.
Transactions are introduced in the `Advanced Features <advanced.html>`__ guide.

A transaction is defined by an identifier, which contains the time stamp, in milliseconds,
of its creation. This identifier—also called the `write pointer`—represents the version
that this transaction will use for all of its writes. It is also used to determine
the order between transactions. A transaction with a smaller write pointer than
another transaction must have been started earlier.

The `Transaction Manager` (or TM) uses the write pointers to implement `Optimistic Concurrency Control`
by maintaining state for all transactions that could be facing concurrency issues.

Transaction Manager States
..........................
The `state` of the TM is defined by these structures and rules:

- The `in-progress set`, which contains all the write pointers of transactions
  which have neither committed nor aborted.
- The `invalid set`, which contains the write pointers of the transactions
  considered invalid, and which will never be committed. A transaction
  becomes invalid only if either it times out or, for a long-running transaction,
  it is being aborted.
- A transaction's write pointer cannot be in the `in-progress set`
  and in the `invalid set` at the same time.
- The `invalid set` and the `in-progress set` together form the `excluded set`.
  When a transaction starts, a copy of this set is given to the transaction so that
  it excludes from its reads any writes performed by transactions in that set.
- The `committing change sets`, which maps write pointers of the transactions
  which have requested to commit their writes and which have passed a first round of
  conflict check to a list of keys in which they have performed those writes.
- The `committed change sets`, which has the same structure as the `committing change sets`,
  but where the write pointers refer to transactions which are already committed and
  which have passed a second round of conflict check.


Transaction Lifecycle States
............................
Here are the states a transaction goes through in its lifecycle:

- When a transaction starts, the TM creates a new write pointer
  and saves it in the `in-progress set`.
  A copy of the current excluded set is given to the transaction,
  as well as a `read pointer`. The pointer
  is an upper bound for the version of writes the transaction is allowed to read.
  It prevents the transaction from reading committed writes performed after the transaction
  started.
- The transaction then performs writes to one or more rows, with the version of those writes
  being the write pointer of the transaction.
- When the transaction wants to commit its writes, it passes to the TM all the keys where
  those writes took place. If the transaction is not in the `excluded set`, the
  TM will use the `committed change sets` structure to detect
  a conflict. A conflict happens in cases where the transaction tries to modify a
  row which, after the start of the transaction, has been modified by one
  of the transactions present in the structure.
- If there are no conflicts, all the writes of the transaction along with its write pointer
  are stored in the `committing change sets` structure.
- The client—namely, a Dataset—can then ask the TM to commit the writes. These are retrieved from the
  `committing change sets` structure. Since the `committed change sets` structure might
  have evolved since the last conflict check, another one is performed. If the
  transaction is in the `excluded set`, the commit will fail regardless
  of conflicts.
- If the second conflict check finds no overlapping transactions, the transaction's
  write pointer is removed from the `in-progress set`, and it is placed in
  the `committed change sets` structure, along with the keys it has
  written to. The writes of this transaction will now be seen by all new transactions.
- If something went wrong in one or other of the committing steps, we distinguish
  between normal and long-running transactions:

  - For a normal transaction, the cause could be that the transaction
    was found in the excluded set or that a conflict was detected.
    The client ensures rolling back the writes the transaction has made,
    and it then asks the TM to abort the transaction.
    This will remove the transaction's write pointer from either the
    `in-progress set` or the `excluded set`, and optionally from the
    `committing change sets` structure.
    
  - For a long-running transaction, the only possible cause is that a conflict
    was detected. Since it is assumed that the writes will not be rolled back
    by the client, the TM aborts the transaction by storing its
    write pointer into the `excluded set`. It is the only way to
    make other transactions exclude the writes performed by this transaction.

The `committed change sets` structure determines how fast conflict detections
are performed. Fortunately, not all the committed writes need to be
remembered; only those which may create a conflict with in-progress
transactions. This is why only the writes committed after the start of the oldest,
in-progress, not-long-running transaction are stored in this structure,
and why transactions which participate in conflict detection must remain
short in duration. The older they are, the bigger the `committed change sets`
structure will be and the longer conflict detection will take.

When conflict detection takes longer, so does committing a transaction
and the transaction stays longer in the `in-progress set`. The whole transaction
system can become slow if such a situation occurs.

Dumping the Transaction Manager
...............................
Reactor comes bundled with a script that allows you to dump the state of the internal
transaction manager into a local file to allow further investigation. If your Reactor
tends to become slow, you can use this tool to detect the incriminating transactions.
This script is called ``tx-debugger`` (on Windows, it is ``tx-debugger.bat``).

To download a snapshot of the state of the TM of a Reactor, use the command::

	$ tx-debugger view --host <name> [--save <filename>]

where `name` is the host name of your Reactor instance, and the optional `filename`
specifies where the snapshot should be saved. This command will
print statistics about all the structures that define the state of the TM.

You can also load a snapshot that has already been saved locally
with the command::

  $ tx-debugger view --filename <filename>

where `filename` specifies the location where the snapshot has been saved.

Here are options that you can use with the ``tx-debugger view`` commands:

- Use the ``--ids`` option to print all the transaction write pointers
  that are stored in the different structures.
- Use the ``--transaction <writePtr>`` option to specify the write pointer
  of a transaction you would like information on. If the transaction is found
  in the committing change sets or the committed change sets
  structures, this will print the keys where the transaction has
  performed writes.

While transactions don't inform you about the tasks that launched them—whether
it was a Flowlet, a MapReduce job, etc.—you can match the time
they were started with the activity of your Reactor to track potential
issues.

If you really know what you are doing and you spot a transaction in the
in-progress set that should be in the excluded set, you can
use this command to invalidate it::

  $ tx-debugger invalidate --host <name> --transaction <writePtr>

Invalidating a transaction when we know for sure that its writes should
be invalidated is useful, because those writes will then be removed
from the concerned Tables.

Where to Go Next
================
Now that you've fixed all your bugs with Continuuity Reactor, take a look at:

- `Reactor Security <security.html>`__,
  which covers enabling security in a production Continuuity Reactor.
