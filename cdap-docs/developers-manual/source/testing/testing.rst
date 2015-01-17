.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _test-cdap:

================================================
Testing a CDAP Application
================================================

.. _test-framework:

Strategies in Testing Applications: Test Framework
==================================================

CDAP comes with a convenient way to unit test your Applications with CDAP’s Test Framework.
The base for these tests is ``TestBase``, which is packaged
separately from the API in its own artifact because it depends on the
CDAP’s runtime classes. You can include it in your test dependencies
in one of two ways:

.. highlight:: xml

- include all JAR files in the ``lib`` directory of the CDAP SDK installation,
  or
- include the ``cdap-unit-test`` artifact in your Maven test dependencies
  (as shown in the ``pom.xml`` file of the :ref:`CDAP SDK examples <standalone-index>`)::
  
    . . .
    <dependency>
      <groupId>co.cask.cdap</groupId>
      <artifactId>cdap-unit-test</artifactId>
      <version>${project.version}</version>
      <scope>test</scope>
    </dependency>
    . . .

Note that for building an application, you only need to include the CDAP API in your
dependencies. For testing, however, you need the CDAP run-time. To build your test case,
extend the ``TestBase`` class.

.. highlight:: console

Running Tests from an IDE
--------------------------
When running tests from an IDE such IntelliJ or Eclipse, set the memory setting for the
``JUnit`` tests that are run from the IDE to an increased amount of memory. We suggest
starting with::

  -Xmx1024m -XX:MaxPermSize=128m

.. highlight:: java

Strategies in Testing Flows
===========================
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

Strategies in Testing MapReduce Programs
========================================
In a fashion similar to `Strategies in Testing Flows`_, we can write
unit testing for MapReduce programs. Let's write a test case for an
application that uses MapReduce. Complete source code and test can be
found in the :ref:`Purchase Example <examples-purchase>` included in the CDAP SDK.

The ``PurchaseTest`` class should extend from
``TestBase`` similar to `Strategies in Testing Flows`::

  public class PurchaseTest extends TestBase {
    @Test
    public void test() throws Exception {

The ``PurchaseApp`` application can be deployed using the ``deployApplication``
method from the ``TestBase`` class::

      // Deploy an Application
      ApplicationManager appManager = deployApplication(PurchaseApp.class);

The MapReduce reads from the ``purchases`` Dataset. As a first
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

Start the MapReduce and wait for a maximum of 60 seconds::

      // Start the MapReduce
      MapReduceManager mrManager = appManager.startMapReduce("PurchaseHistoryBuilder");
      mrManager.waitForFinish(60, TimeUnit.SECONDS);

We can start verifying that the MapReduce was run correctly by
obtaining a client for the Procedure, and then submitting a query for
the counts::

      ProcedureClient client = procedureManager.getClient();

      // Verify the query.
      String response = client.query("history", ImmutableMap.of("customer", "joe"));

      // Deserialize the JSON string.
      PurchaseHistory result = GSON.fromJson(response, PurchaseHistory.class);
      Assert.assertEquals(2, result.getPurchases().size());

The assertion will verify that the correct result was received.

Strategies in Testing Spark Programs
====================================
Let's write a test case for an application that uses a Spark program.
Complete source code for this test can be found at :ref:`Spark PageRank<examples-spark-page-rank>`.

The ``SparkPageRankTest`` class should extend from
``TestBase`` similar to `Strategies in Testing Flows`::

  public class SparkPageRankTest extends TestBase {
    @Test
    public void test() throws Exception {

The ``SparkPageRankTest`` application can be deployed using the ``deployApplication``
method from the ``TestBase`` class::

  // Deploy an Application
  ApplicationManager appManager = deployApplication(SparkPageRankApp.class);

The Spark program reads from the ``backlinkURLs`` Dataset. As a first
step, data in the ``backlinkURLs`` should be populated by running
the ``BackLinkFlow`` and sending the data to the Stream ``backlinkURLStream``::

  FlowManager flowManager = appManager.startFlow("BackLinkFlow");
  // Send data to the Stream
  sendData(appManager);

  // Wait for the last Flowlet to process 4 events or at most 5 seconds
  RuntimeMetrics metrics = RuntimeStats.
      getFlowletMetrics("SparkPageRank", "BackLinkFlow", "reader");
  metrics.waitForProcessed(4, 5, TimeUnit.SECONDS);

Start the Spark program and wait for a maximum of 60 seconds::

  // Start the Spark program.
  SparkManager sparkManager = appManager.startSpark("SparkPageRankProgram");
  sparkManager.waitForFinish(60, TimeUnit.SECONDS);

We verify that the Spark program ran correctly by
obtaining a client for the Procedure, and then submitting a query for
the ranks::

  ProcedureClient client = procedureManager.getClient();

  // Verify the query.
  String response = client.query("rank", ImmutableMap.of("url", "http://example.com/page1"));
  Assert.assertEquals("1.3690036520596678", response);

The assertion will verify that the correct result was received.


Validating Test Data with SQL
=============================
Often the easiest way to verify that a test produced the right data is to run a SQL query—if the data sets involved
in the test case are record-scannable as described in :ref:`data-exploration`.
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
