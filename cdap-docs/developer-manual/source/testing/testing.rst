.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. _test-cdap:

==========================
Testing a CDAP Application
==========================

.. _test-framework:

Strategies in Testing Applications: Test Framework
==================================================

CDAP comes with a convenient way to unit-test your applications with CDAP’s *Test Framework.*
This framework starts an :ref:`in-memory CDAP runtime <in-memory-data-application-platform>`
and lets you deploy an application; start, stop and monitor programs; access datasets to
validate processing results; and retrieve metrics from the application.

The base class for such tests is ``TestBase``, which is packaged
separately from the API in its own artifact because it depends on the
CDAP’s runtime classes. You can include it in your test dependencies
in one of two ways:

.. highlight:: xml

- include all JAR files in the ``lib`` directory of the CDAP Sandbox installation,
  or
- include the ``cdap-unit-test`` artifact in your Maven test dependencies
  (as shown in the ``pom.xml`` file of the :ref:`CDAP Sandbox examples <local-sandbox-index>`)::

    . . .
    <dependency>
      <groupId>co.cask.cdap</groupId>
      <artifactId>cdap-unit-test</artifactId>
      <version>${cdap.version}</version>
      <scope>test</scope>
    </dependency>
    . . .

Note that for building an application, you only need to include the CDAP API in your
dependencies. For testing, however, you need the CDAP run-time. To build your test case,
extend the ``TestBase`` class.

Running Tests with Spark2
-------------------------
The ``TestBase`` class included in the ``cdap-unit-test`` dependency will run programs using
Spark1 and Scala 2.10. If you need to unit test a program that uses Spark2 and Scala 2.11,
you must remove the ``cdap-unit-test`` artifact from your Maven test dependencies and replace it
with ``cdap-unit-test2_2.11``::

    . . .
    <dependency>
      <groupId>co.cask.cdap</groupId>
      <artifactId>cdap-unit-test2_2.11</artifactId>
      <version>${cdap.version}</version>
      <scope>test</scope>
    </dependency>
    . . .

In addition, instead of extending the ``TestBase`` class, you must extend the ``TestBaseWithSpark2`` class.

.. highlight:: console

Running Tests from an IDE
--------------------------
When running tests from an IDE such IntelliJ or Eclipse, set the memory setting for the
``JUnit`` tests that are run from the IDE to an increased amount of memory. We suggest
starting with::

  -Xmx1024m -XX:MaxPermSize=128m

.. _test-framework-strategies-flow:

Strategies in Testing Flows
===========================
.. highlight:: java

Let’s write a test case for the *WordCount* example::

  public class WordCountTest extends TestBase {
    static Type stringMapType = new TypeToken<Map<String, String>>() { }.getType();
    static Type objectMapType = new TypeToken<Map<String, Object>>() { }.getType();

    @Test
    public void testWordCount() throws Exception {


The first thing we do in this test is deploy the application,
then we’ll start the flow and the service::

      // Deploy the application
      ApplicationManager appManager = deployApplication(WordCount.class);

      // Start the flow and the service
      FlowManager flowManager = appManager.getFlowManager("WordCounter").start();
      ServiceManager serviceManager = appManager.getServiceManager("RetrieveCounts").start();
      serviceManager.waitForStatus(true);

This flow counts the words in the events that it receives on the "wordStream". Before
sending data to the stream, let's validate that we are starting with a clean state::

      // validate that the wordCount table is empty, and that it has no entry for "world"
      DataSetManager<KeyValueTable> datasetManager = getDataset(config.getWordCountTable());
      KeyValueTable wordCounts = datasetManager.get();
      Assert.assertNull(wordCounts.read("world"));

Note that the dataset manager implicitly starts a transaction for the dataset when it is
initialized. Under this transaction, read operations can only see changes that were
committed before the transaction was started; and any writes performed within this
transaction only become visible to other transactions after this transaction is committed.

This can be done by calling the dataset manager's ``flush()`` method, which commits the current
transaction and starts a new one. ``flush()`` also needs to be called to make changes visible
to the dataset, if those changes were committed after the current transaction was started. We
will see an example for this below.

Now that the flow and service are running, we can send some events to the stream::

      // Send a few events to the stream
      StreamManager streamManager = getStreamManager("words");
      streamManager.send("hello world");
      streamManager.send("a wonderful world");
      streamManager.send("the world says hello");

To wait for all events to be processed, we can get a metrics observer
for the last flowlet in the pipeline (the "word associator") and wait for
its processed count to either reach 3 or time out after 5 seconds::

      // Wait for the events to be processed, or at most 5 seconds
      RuntimeMetrics metrics = flowManager.getFlowletMetrics("associator");
      metrics.waitForProcessed(3, 5, TimeUnit.SECONDS);

Now we can start verifying that the processing was correct by reading the datasets
used by the flow. For example, we can validate the correct count for the word "world".
Note that first we have to start a new transaction by calling ``flush()``::

      // start a new transaction so that the "wordCounts" dataset sees changes made by the flow
      datasetManager.flush();
      Assert.assertEquals(3L, Bytes.toLong(wordCounts.read("world")));

We can also validate the processing results by obtaining a client for the service,
and then submitting queries. We'll add a private method to the ``WordCountTest``
class to help us::

  private String requestService(URL url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    try {
      return new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
    } finally {
      conn.disconnect();
    }
  }

We can then use this to query for the global statistics::

      // Query global statistics
      String response = requestService(new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS), "stats"));

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
      response = requestService(new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS), "count/world"));
      Map<String, Object> omap = new Gson().fromJson(response, stringMapType);
      Assert.assertEquals("world", omap.get("word"));
      Assert.assertEquals(3.0, omap.get("count"));

      // The associations are a map within the map
      @SuppressWarnings("unchecked")
      Map<String, Double> assocs = (Map<String, Double>) omap.get("assocs");
      Assert.assertEquals(2.0, (double)assocs.get("hello"), 0.000001);
      Assert.assertTrue(assocs.containsKey("hello"));

.. _test-framework-strategies-mapreduce:

Strategies in Testing MapReduce Programs
========================================
In a fashion similar to `Strategies in Testing Flows`_, we can write
unit testing for MapReduce programs. Let's write a test case for an
application that uses MapReduce. Complete source code and test can be
found in the :ref:`Purchase Example <examples-purchase>` included in the CDAP Sandbox.

The ``PurchaseTest`` class should extend from
``TestBase`` similar to `Strategies in Testing Flows`::

  public class PurchaseTest extends TestBase {
    @Test
    public void test() throws Exception {

The ``PurchaseApp`` application can be deployed using the ``deployApplication``
method from the ``TestBase`` class::

      // Deploy an application
      ApplicationManager appManager = deployApplication(PurchaseApp.class);

The MapReduce reads from the ``purchases`` dataset. As a first
step, the data to the ``purchases`` should be populated by running
the ``PurchaseFlow`` and sending the data to the ``purchaseStream``
stream::

      FlowManager flowManager = appManager.getFlowManager("PurchaseFlow").start();

      // Send stream events to the "purchaseStream" Stream
      StreamManager streamManager = getStreamManager("purchaseStream");
      streamManager.send("bob bought 3 apples for $30");
      streamManager.send("joe bought 1 apple for $100");
      streamManager.send("joe bought 10 pineapples for $20");
      streamManager.send("cat bought 3 bottles for $12");
      streamManager.send("cat bought 2 pops for $14");

      // Wait for the last flowlet to process 5 events or at most 15 seconds
      RuntimeMetrics metrics = flowManager.getFlowletMetrics("collector");
      metrics.waitForProcessed(5, 15, TimeUnit.SECONDS);

Start the MapReduce and wait for a maximum of 60 seconds::

      // Start the MapReduce
      MapReduceManager mrManager = appManager.getMapReduceManager("PurchaseHistoryBuilder").start();
      mrManager.waitForFinish(60, TimeUnit.SECONDS);

We can start verifying that the MapReduce was run correctly by
using the ``PurchaseHistoryService`` to retrieve a customer's purchase history::

    // Start PurchaseHistoryService
    ServiceManager purchaseHistoryServiceManager =
      appManager.getServiceManager(PurchaseHistoryService.SERVICE_NAME).start();

    // Wait for service startup
    purchaseHistoryServiceManager.waitForStatus(true);

    // Test service to retrieve a customer's purchase history
    URL url = new URL(purchaseHistoryServiceManager.getServiceURL(15, TimeUnit.SECONDS), "history/joe");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    String historyJson;
    try {
      historyJson = new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
    } finally {
      conn.disconnect();
    }
    PurchaseHistory history = GSON.fromJson(historyJson, PurchaseHistory.class);
    Assert.assertEquals("joe", history.getCustomer());
    Assert.assertEquals(2, history.getPurchases().size());

The assertion will verify that the correct result was received.

.. _test-framework-strategies-spark:

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

  // Deploy an application
  ApplicationManager appManager = deployApplication(SparkPageRankApp.class);

The Spark program reads from the ``backlinkURLs`` dataset. As a first
step, data in the ``backlinkURLs`` should be populated by running
the ``BackLinkFlow`` and sending the data to the stream ``backlinkURLStream``::

  FlowManager flowManager = appManager.getFlowManager("BackLinkFlow").start();
  // Send data to the stream
  sendData();

  // Wait for the last flowlet to process 4 events or at most 5 seconds
  RuntimeMetrics metrics = flowManager.getFlowletMetrics("reader");
  metrics.waitForProcessed(4, 5, TimeUnit.SECONDS);

Start the Spark program and wait for a maximum of 60 seconds::

  // Start the Spark program.
  SparkManager sparkManager = appManager.getSparkManager("SparkPageRankProgram").start();
  sparkManager.waitForFinish(60, TimeUnit.SECONDS);

We verify that the Spark program ran correctly by
using the Ranks service to check the results::

  // Wait for ranks service to start
  serviceManager.waitForStatus(true);

  String response = requestService(new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS),
                                           "rank?url=http://example.com/page1"));
  Assert.assertEquals("14", response);

The assertion will verify that the correct result was received.


Strategies in Testing Artifacts
===============================

.. _test-framework-strategies-artifacts:

.. highlight:: java

The Test Framework provides methods to create and deploy JAR files as artifacts. This lets you
test the creation of multiple applications from the same artifact, as well as the use of plugin artifacts.

To add an artifact containing an application class::

  // Add the artifact for a Data Pipeline app
  addAppArtifact(new ArtifactId(NamespaceId.DEFAULT.getNamespace(), "data-pipeline", "3.5.0"),
    DataPipelineApp.class,
    BatchSource.class.getPackage().getName(),
    Action.class.getPackage().getName(),
    PipelineConfigurable.class.getPackage().getName(),
    "org.apache.avro.mapred", "org.apache.avro", "org.apache.avro.generic");

The first argument is the ``id`` of the artifact; the second is the application class; and
the remainder of the arguments are packages that should be included in the
``Export-Packages`` manifest attribute bundled in the JAR. The framework will trace the
dependencies of the specified application class to create a JAR with those dependencies.
This will mimic what happens when you actually build your application JAR using maven.

An application can then be deployed using that artifact::

  // Create application create request
  ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transformList);
  AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(new ArtifactSummary("etlbatch", "3.5.0"), etlConfig);
  ApplicationId appId = NamespaceId.DEFAULT.app("KVToKV");

  // Deploy the application
  ApplicationManager appManager = deployApplication(appId, appRequest);

Plugins extending the artifact can also be added::

  // Add some test plugins
  addPluginArtifact(new ArtifactId(NamespaceId.DEFAULT.getNamespace(), "spark-plugins", "1.0.0"),
                    APP_ARTIFACT_ID,
                    NaiveBayesTrainer.class, NaiveBayesClassifier.class);

The first argument is the ``id`` of the plugin artifact; the second is the parent artifact
it is extending; and the remainder of the arguments are classes that should be bundled in
the JAR. The packages of all these classes are included in the ``Export-Packages``
manifest attribute bundled in the JAR. When adding a plugin artifact this way, it is
important to include all classes in your plugin packages, even if they are not used in
your test case. This is to ensure that the JAR can trace all required dependencies to
correctly build the JAR.

The examples are taken from the ``DataPipelineTest`` and ``HydratorTestBase`` classes of CDAP pipelines.

.. _test-framework-validating-sql:

Validating Test Data with SQL
=============================
Often the easiest way to verify that a test produced the right data is to run a SQL query |---| if the data sets involved
in the test case are record-scannable, as described in :ref:`data-exploration`.
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

.. _test-framework-configuring-cdap:

Configuring CDAP Runtime for Test Framework
===========================================
The ``TestBase`` class inherited by your test class starts an in-memory CDAP runtime before executing any test methods.
Sometimes you may need to configure the CDAP runtime to suit your specific requirements. For example, if your test
does not involve usage of SQL queries, you can turn off the explore service to reduce startup and shutdown times.

You alter the configurations for the CDAP runtime by applying a JUnit ``@ClassRule`` on a ``TestConfiguration``
instance. For example::

  // Disable the SQL query support
  // Set the transaction timeout to 60 seconds
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false,
                                                                       "data.tx.timeout", 60);

Refer to the :ref:`cdap-site.xml <appendix-cdap-site.xml>` for the available set of configurations used by CDAP.

