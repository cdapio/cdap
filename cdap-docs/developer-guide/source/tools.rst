.. :author: Cask Data, Inc.
   :description: Cask Data Application Platform - Tools
         :copyright: Copyright © 2014 Cask Data, Inc.

================================================
Cask Data Application Platform - Available Tools
================================================

Tools Info
==========
CDAP comes with a bunch of tools to make developer's life easier. These tools help you to debug CDAP applications,
interact with them and ingest data into them,etc:

.. list-table::
    :widths: 10 30 60
    :header-rows: 1

    * - Tool Name
      - Description
      - Quick Link
    * - CLI
      - ``The Command-Line Interface (CLI) to interact with the CDAP server from within a shell, similar to HBase shell or bash``
      - CLI_
    * - Test Framework
      - ``How you can take advantage of the Powerful Test Framework to test your CDAP applications before deploying. This makes catching bugs early and easy``
      - TestFramework_
    * - Debugging
      - ``How you can debug CDAP applications in standalone mode and debugging app containers in distributed mode``
      - DebugCDAP_
    * - Transactions Debugger
      - ``Snapshot state of Transaction manager``
      - TxDebugger_
    * - Ingestion tools
      - ``Ways to Ingest data into CDAP``
      - Ingest_

.. _CLI:

Command-Line Interface
======================

Introduction
------------

The Command-Line Interface (CLI) provides methods to interact with the CDAP server from within a shell,
similar to HBase shell or ``bash``. It is located within the SDK, at ``bin/cdap-cli`` as either a bash
script or a Windows ``.bat`` file. It is also packaged in the SDK as a JAR file, at ``bin/cdap-cli.jar``.

Usage
-----

The CLI may be used in two ways: interactive mode and non-interactive mode.

Interactive Mode
----------------

.. highlight:: console

To run the CLI in interactive mode, run the ``cdap-cli`` executable with no arguments from the terminal::

  $ /bin/cdap-cli

or, on Windows::

  ~SDK> bin\cdap-cli.bat

The executable should bring you into a shell, with this prompt::

  cdap (localhost:10000)>

This indicates that the CLI is currently set to interact with the CDAP server at ``localhost``.
There are two ways to interact with a different CDAP server:

- To interact with a different CDAP server by default, set the environment variable ``CDAP_HOST`` to a hostname.
- To change the current CDAP server, run the command ``connect example.com``.

For example, with ``CDAP_HOST`` set to ``example.com``, the Shell Client would be interacting with
a CDAP instance at ``example.com``, port ``10000``::

  cdap (example.com:10000)>

To list all of the available commands, enter ``help``::

  cdap (localhost:10000)> help

Non-Interactive Mode
--------------------

To run the CLI in non-interactive mode, run the ``cdap-cli`` executable, passing the command you want executed
as the argument. For example, to list all applications currently deployed to CDAP, execute::

  cdap list apps

Available Commands
------------------

These are the available commands:

.. csv-table::
   :header: Command,Description
   :widths: 50, 50

     **General**
   ``help``,Prints this helper text
   ``version``,Prints the version
   ``exit``,Exits the shell
   **Calling and Executing**
   ``call procedure <app-id>.<procedure-id> <method-id> <parameters-map>``,"Calls a Procedure, passing in the parameters as a JSON String map"
   ``execute <query>``,Executes a Dataset query
   **Creating**
   ``create dataset instance <type-name> <new-dataset-name>``,Creates a Dataset
   ``create stream <new-stream-id>``,Creates a Stream
   **Deleting**
   ``delete app <app-id>``,Deletes an Application
   ``delete dataset instance <dataset-name>``,Deletes a Dataset
   ``delete dataset module <module-name>``,Deletes a Dataset module
   **Deploying**
   ``deploy app <app-jar-file>``,Deploys an application
   ``deploy dataset module <module-jar-file> <module-name> <module-jar-classname>``,Deploys a Dataset module
   **Describing**
   ``describe app <app-id>``,Shows detailed information about an application
   ``describe dataset module <module-name>``,Shows information about a Dataset module
   ``describe dataset type <type-name>``,Shows information about a Dataset type
   **Retrieving Information**
   ``get history flow <app-id>.<program-id>``,Gets the run history of a Flow
   ``get history mapreduce <app-id>.<program-id>``,Gets the run history of a MapReduce job
   ``get history procedure <app-id>.<program-id>``,Gets the run history of a Procedure
   ``get history runnable <app-id>.<program-id>``,Gets the run history of a Runnable
   ``get history workflow <app-id>.<program-id>``,Gets the run history of a Workflow
   ``get instances flowlet <app-id>.<program-id>``,Gets the instances of a Flowlet
   ``get instances procedure <app-id>.<program-id>``,Gets the instances of a Procedure
   ``get instances runnable <app-id>.<program-id>``,Gets the instances of a Runnable
   ``get live flow <app-id>.<program-id>``,Gets the live info of a Flow
   ``get live procedure <app-id>.<program-id>``,Gets the live info of a Procedure
   ``get logs flow <app-id>.<program-id> [<start-time> <end-time>]``,Gets the logs of a Flow
   ``get logs mapreduce <app-id>.<program-id> [<start-time> <end-time>]``,Gets the logs of a MapReduce job
   ``get logs procedure <app-id>.<program-id> [<start-time> <end-time>]``,Gets the logs of a Procedure
   ``get logs runnable <app-id>.<program-id> [<start-time> <end-time>]``,Gets the logs of a Runnable
   ``get status flow <app-id>.<program-id>``,Gets the status of a Flow
   ``get status mapreduce <app-id>.<program-id>``,Gets the status of a MapReduce job
   ``get status procedure <app-id>.<program-id>``,Gets the status of a Procedure
   ``get status service <app-id>.<program-id>``,Gets the status of a Service
   ``get status workflow <app-id>.<program-id>``,Gets the status of a Workflow
   **Listing Elements**
   ``list apps``,Lists all applications
   ``list dataset instances``,Lists all Datasets
   ``list dataset modules``,Lists Dataset modules
   ``list dataset types``,Lists Dataset types
   ``list flows``,Lists Flows
   ``list mapreduce``,Lists MapReduce jobs
   ``list procedures``,Lists Procedures
   ``list programs``,Lists all programs
   ``list streams``,Lists Streams
   ``list workflows``,Lists Workflows
   **Sending Events**
   ``send stream <stream-id> <stream-event>``,Sends an event to a Stream
   **Setting**
   ``set instances flowlet <program-id> <num-instances>``,Sets the instances of a Flowlet
   ``set instances procedure <program-id> <num-instances>``,Sets the instances of a Procedure
   ``set instances runnable <program-id> <num-instances>``,Sets the instances of a Runnable
   ``set stream ttl <stream-id> <ttl-in-seconds>``,Sets the Time-to-Live (TTL) of a Stream
   **Starting**
   ``start flow <program-id>``,Starts a Flow
   ``start mapreduce <program-id>``,Starts a MapReduce job
   ``start procedure <program-id>``,Starts a Procedure
   ``start service <program-id>``,Starts a Service
   ``start workflow <program-id>``,Starts a Workflow
   **Stopping**
   ``stop flow <program-id>``,Stops a Flow
   ``stop mapreduce <program-id>``,Stops a MapReduce job
   ``stop procedure <program-id>``,Stops a Procedure
   ``stop service <program-id>``,Stops a Service
   ``stop workflow <program-id>``,Stops a Workflow
   **Truncating**
   ``truncate dataset instance``,Truncates a Dataset
   ``truncate stream``,Truncates a Stream

.. highlight:: java

.. _TestFramework:

Testing CDAP
============

Strategies in Testing Applications
----------------------------------

CDAP comes with a convenient way to unit test your Applications.
The base for these tests is ``TestBase``, which is packaged
separately from the API in its own artifact because it depends on the
CDAP’s runtime classes. You can include it in your test dependencies
in one of two ways:

- include all JAR files in the ``lib`` directory of the CDAP SDK installation,
  or
- include the ``cdap-unit-test`` artifact in your Maven test dependencies
  (see the ``pom.xml`` file of the *WordCount* example).

Note that for building an application, you only need to include the
CDAP API in your dependencies. For testing, however, you need the
CDAP run-time. To build your test case, extend the
``TestBase`` class.

Strategies in Testing Flows
---------------------------
Let’s write a test case for the *WordCount* example::

  public class WordCountTest extends TestBase {
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
found under `Purchase </examples/Purchase/index.html>`__.

The ``PurchaseTest`` class should extend from
``AppTestBase`` similar to `Strategies in Testing Flows`.

::

  public class PurchaseTest extends AppTestBase {
    @Test
    public void test() throws Exception {

The ``PurchaseApp`` application can be deployed using the ``deployApplication``
method from the ``AppTestBase`` class::

  // Deploy an Application
  ApplicationManager appManager = deployApplication(PurchaseApp.class);

The MapReduce job reads from the ``purchases`` Dataset. As a first
step, the data to the ``purchases`` should be populated by running
the ``PurchaseFlow`` and sending the data to the ``purchaseStream``
Stream::

  FlowManager flowManager = appManager.startFlow("PurchaseFlow");
  // Send data to the Stream
  sendData(appManager, now);

  // Wait for the last Flowlet to process 3 events or at most 5 seconds
  RuntimeMetrics metrics = RuntimeStats.
      getFlowletMetrics("PurchaseApp", "PurchaseFlow", "collector");
  metrics.waitForProcessed(3, 5, TimeUnit.SECONDS);

Start the MapReduce job and wait for a maximum of 60 seconds::

  // Start the MapReduce job.
  MapReduceManager mrManager = appManager.startMapReduce("PurchaseHistoryBuilder");
  mrManager.waitForFinish(60, TimeUnit.SECONDS);

We can start verifying that the MapReduce job was run correctly by
obtaining a client for the Procedure, and then submitting a query for
the counts::

  ProcedureClient client = procedureManager.getClient();

  // Verify the query.
  String response = client.query("history", ImmutableMap.of("customer", "joe"));

  // Deserialize the JSON string.
  PurchaseHistory result = GSON.fromJson(response, PurchaseHistory.class);
  Assert.assertEquals(2, result.getPurchases().size());

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

.. _DebugCDAP:

Debugging CDAP
==============

Debugging an Application in Standalone CDAP
-------------------------------------------
Any CDAP Application can be debugged in the Standalone CDAP
by attaching a remote debugger to the CDAP JVM. To enable remote
debugging:

#. Start the Standalone CDAP with ``--enable-debug``, optionally specifying a port (default is ``5005``).

   The CDAP should confirm that the debugger port is open with a message such as
   ``Remote debugger agent started on port 5005``.

#. Deploy (for example) the *HelloWorld* Application to the CDAP by dragging and dropping the
   ``HelloWorld.jar`` file from the ``/examples/HelloWorld`` directory onto the CDAP Console.

#. Open the *HelloWorld* Application in an IDE and connect to the remote debugger.

For more information, see `Attaching a Debugger`_.

:Note:  Currently, debugging is not supported under Windows.

Debugging an Application in Distributed CDAP
-----------------------------------------------

.. highlight:: console

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
`CDAP Client HTTP API <rest.html#cdap-client-http-api>`__). You can pass in
runtime arguments in the exact same way as you normally would start a Flow.

Once the Flow is running, each Flowlet will detect an available port in its container
and open that port for attaching a debugger.
To find out the address of a container’s host and the container’s debug port, you can query
the CDAP for a Procedure or Flow’s live info via HTTP::

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

.. highlight:: java

Attaching a Debugger
--------------------

Debugging with IntelliJ
.......................

*Note:* These instructions were developed with *IntelliJ v13.1.2.*
You may need to adjust them for your installation or version.

#. From the *IntelliJ* toolbar, select ``Run -> Edit Configurations``.
#. Click ``+`` and choose ``Remote``:

   .. image:: _images/debugging/intellij_1.png

#. Create a debug configuration by entering a name, for example, ``CDAP``.
#. Enter the host name, for example, ``localhost`` or ``node-1003.my.cluster.net``
   in the Host field.
#. Enter the debugging port, for example, ``5005`` in the Port field:

   .. image:: _images/debugging/intellij_2.png

#. To start the debugger, select ``Run -> Debug -> CDAP``.
#. Set a breakpoint in any code block, for example, a Flowlet method:

   .. image:: _images/debugging/intellij_3.png

#. Start the Flow in the Console.
#. Send an event to the Stream. The control will stop at the breakpoint
   and you can proceed with debugging.


Debugging with Eclipse
......................

*Note:* These instructions were developed with *Eclipse IDE for Java Developers v4.4.0.*
You may need to adjust them for your installation or version.

#. In Eclipse, select ``Run-> Debug`` configurations.
#. In the list on the left of the window, double-click ``Remote Java Application`` to create
   a new launch configuration.

   .. image:: _images/debugging/eclipse_1.png

#. Enter a name and project, for example, ``CDAP``.

   .. image:: _images/debugging/eclipse_2.png

#. Enter the host name, for example, ``localhost`` or ``node-1003.my.cluster.net``
   in the Port field:
#. Enter the debugging port, for example, ``5005`` in the Port field:


#. In your project, click ``Debug`` to start the debugger.

#. Set a breakpoint in any code block, for example, a Flowlet method:

   .. image:: _images/debugging/eclipse_3.png

#. Start the Flow in the Console.
#. Send an event to the Stream.
#. The control stops at the breakpoint and you can proceed with debugging.


.. _TxDebugger:

Dumping the Transaction Manager
===============================

.. highlight:: console

CDAP comes bundled with a script that allows you to dump the state of the internal
transaction manager into a local file to allow further investigation. If your CDAP Instance
tends to become slow, you can use this tool to detect the incriminating transactions.
This script is called ``tx-debugger`` (on Windows, it is ``tx-debugger.bat``).

To download a snapshot of the state of the TM of the CDAP, use the command::

  $ tx-debugger view --host <name> [--save <filename>]

where `name` is the host name of your CDAP instance, and the optional `filename`
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
they were started with the activity of your CDAP to track potential
issues.

If you really know what you are doing and you spot a transaction in the
in-progress set that should be in the excluded set, you can
use this command to invalidate it::

  $ tx-debugger invalidate --host <name> --transaction <writePtr>

Invalidating a transaction when we know for sure that its writes should
be invalidated is useful, because those writes will then be removed
from the concerned Tables.

.. highlight:: java

.. _Ingest:

Ingesting Data Into CDAP
========================

.. highlight:: console

CDAP Flume Sink
---------------

