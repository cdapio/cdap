.. :author: Cask Data, Inc.
   :description: A Java Client for Interacting With the Cask Data Application Platform 

==============================================
Cask Data Application Platform Java Client API
==============================================

Introduction
============

The Cask Data Application Platform (CDAP) Java Client API provides methods for interacting
with CDAP from Java applications.

Maven Dependency
================

.. highlight:: console

To use the Java Client API in your project, add this Maven dependency::

  <dependency>
    <groupId>co.cask.cdap</groupId>
    <artifactId>client</artifactId>
    <version>${cdap.version}</version>
  </dependency>

.. highlight:: java

Components
==========

The Java Client API allows you to interact with these CDAP components:

- `ApplicationClient`_: interacting with applications
- `DatasetClient`_: interacting with Datasets
- `DatasetModuleClient`_: interacting with Dataset Modules
- `DatasetTypeClient`_: interacting with Dataset Types
- `MetricsClient`_: interacting with Metrics
- `MonitorClient`_: monitoring System Services
- `ProcedureClient`_: interacting with Procedures
- `ProgramClient`_: interacting with Flows, Procedures, MapReduce Jobs, User Services, and Workflows
- `QueryClient`_: querying Datasets
- `ServiceClient`_: interacting with User Services
- `StreamClient`_: interacting with Streams

Sample Usage
============

ApplicationClient
-----------------
::

  // Interact with the CDAP instance located at example.com, port 10000
  ClientConfig clientConfig = new ClientConfig("example.com", 10000);

  // Construct the client used to interact with CDAP
  ApplicationClient appClient = new ApplicationClient(clientConfig);

  // Fetch the list of applications
  List<ApplicationRecord> apps = appClient.list();

  // Deploy an application
  File appJarFile = ...;
  appClient.deploy(appJarFile);

  // Delete an application
  appClient.delete("Purchase");

  // List programs belonging to an application
  appClient.listPrograms("Purchase");


DatasetClient
-------------
::

  // Interact with the CDAP instance located at example.com, port 10000
  ClientConfig clientConfig = new ClientConfig("example.com", 10000);

  // Construct the client used to interact with CDAP
  DatasetClient datasetClient = new DatasetClient(clientConfig);

  // Fetch the list of Datasets
  List<DatasetSpecification> datasets = datasetClient.list();

  // Create a Dataset
  datasetClient.create("someDataset", "someDatasetType");

  // Delete a Dataset
  datasetClient.delete("someDataset");


MetricsClient
-------------
::

  // Interact with the CDAP instance located at example.com, port 10000
  ClientConfig clientConfig = new ClientConfig("example.com", 10000);

  // Construct the client used to interact with CDAP
  MetricsClient metricsClient = new MetricsClient(clientConfig);

  // Fetch the total number of events that have been processed by a Flow
  JsonObject metric = metricsClient.getMetric("user", "/apps/HelloWorld/flows", 
                                              "process.events.processed", "aggregate=true");

MonitorClient
-------------
::

  // Interact with the CDAP instance located at example.com, port 10000
  ClientConfig clientConfig = new ClientConfig("example.com", 10000);

  // Construct the client used to interact with CDAP
  MonitorClient monitorClient = new MonitorClient(clientConfig);

  // Fetch the list of System Services
  List<SystemServiceMeta> services = monitorClient.listSystemServices();

  // Fetch status of System Transaction Service 
  String serviceStatus = monitorClient.getSystemServiceStatus("transaction");

  // Fetch the number of instances of the System Transaction Service
  int systemServiceInstances = monitorClient.getSystemServiceInstances("transaction");


ProcedureClient
---------------
::

  // Interact with the CDAP instance located at example.com, port 10000
  ClientConfig clientConfig = new ClientConfig("example.com", 10000);

  // Construct the client used to interact with CDAP
  ProcedureClient procedureClient = new ProcedureClient(clientConfig);

  // Call procedure in the HelloWorld example
  String result = procedureClient.call(FakeApp.NAME, FakeProcedure.NAME, FakeProcedure.METHOD_NAME,
                                       ImmutableMap.of("customer", "joe"));


ProgramClient
-------------
::

  // Interact with the CDAP instance located at example.com, port 10000
  ClientConfig clientConfig = new ClientConfig("example.com", 10000);

  // Construct the client used to interact with CDAP
  ProgramClient programClient = new ProgramClient(clientConfig);

  // Call procedure in the HelloWorld example
    LOG.info("Starting procedure");
    programClient.start(FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME);
    assertProgramRunning(programClient, FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME);

    LOG.info("Getting live info");
    programClient.getLiveInfo(FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME);

    LOG.info("Getting program logs");
    programClient.getProgramLogs(FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME, 0, Long.MAX_VALUE);

    LOG.info("Scaling procedure");
    Assert.assertEquals(1, programClient.getProcedureInstances(FakeApp.NAME, FakeProcedure.NAME));
    programClient.setProcedureInstances(FakeApp.NAME, FakeProcedure.NAME, 3);
    assertProcedureInstances(programClient, FakeApp.NAME, FakeProcedure.NAME, 3);

    LOG.info("Stopping procedure");
    programClient.stop(FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME);
    assertProgramStopped(programClient, FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME);

    // start, scale, and stop flow
    verifyProgramNames(FakeApp.FLOWS, appClient.listPrograms(FakeApp.NAME, ProgramType.FLOW));

    LOG.info("Starting flow");
    programClient.start(FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);
    assertProgramRunning(programClient, FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);

    LOG.info("Getting flow history");
    programClient.getProgramHistory(FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);

    LOG.info("Scaling flowlet");
    Assert.assertEquals(1, programClient.getFlowletInstances(FakeApp.NAME, FakeFlow.NAME, FakeFlow.FLOWLET_NAME));
    programClient.setFlowletInstances(FakeApp.NAME, FakeFlow.NAME, FakeFlow.FLOWLET_NAME, 3);
    assertFlowletInstances(programClient, FakeApp.NAME, FakeFlow.NAME, FakeFlow.FLOWLET_NAME, 3);

    LOG.info("Stopping flow");
    programClient.stop(FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);
    assertProgramStopped(programClient, FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);

QueryClient
-------------
::

  // Interact with the CDAP instance located at example.com, port 10000
  ClientConfig clientConfig = new ClientConfig("example.com", 10000);

  // Construct the client used to interact with CDAP
  QueryClient queryClient = new QueryClient(clientConfig);


ServiceClient
-------------
::

  // Interact with the CDAP instance located at example.com, port 10000
  ClientConfig clientConfig = new ClientConfig("example.com", 10000);

  // Construct the client used to interact with CDAP
  ServiceClient serviceClient = new ServiceClient(clientConfig);

      ServiceMeta serviceMeta = serviceClient.get(FakeApp.NAME, FakeService.NAME);

StreamClient
-------------
::

  // Interact with the CDAP instance located at example.com, port 10000
  ClientConfig clientConfig = new ClientConfig("example.com", 10000);

  // Construct the client used to interact with CDAP
  StreamClient streamClient = new StreamClient(clientConfig);

    String testStreamId = "teststream";
    LOG.info("Getting stream list");
    int baseStreamCount = streamClient.list().size();
    Assert.assertEquals(baseStreamCount, streamClient.list().size());
    LOG.info("Creating stream");
    streamClient.create(testStreamId);
    LOG.info("Checking stream list");
    Assert.assertEquals(baseStreamCount + 1, streamClient.list().size());
    StreamProperties config = streamClient.getConfig(testStreamId);

  /**
   * Tests for the get events call
   */
  @Test
  public void testStreamEvents() throws IOException, BadRequestException, StreamNotFoundException,
    UnAuthorizedAccessTokenException {

    String streamId = "testEvents";

    streamClient.create(streamId);
    for (int i = 0; i < 10; i++) {
      streamClient.sendEvent(streamId, "Testing " + i);
    }

    // Read all events
    List<StreamEvent> events = streamClient.getEvents(streamId, 0, Long.MAX_VALUE,
                                                      Integer.MAX_VALUE, Lists.<StreamEvent>newArrayList());
    Assert.assertEquals(10, events.size());

    // Read first 5 only
    events.clear();
    streamClient.getEvents(streamId, 0, Long.MAX_VALUE, 5, events);
    Assert.assertEquals(5, events.size());

    // Read 2nd and 3rd only
    long startTime = events.get(1).getTimestamp();
    long endTime = events.get(2).getTimestamp() + 1;
    events.clear();
    streamClient.getEvents(streamId, startTime, endTime, Integer.MAX_VALUE, events);

    Assert.assertEquals(2, events.size());

    for (int i = 1; i < 3; i++) {
      Assert.assertEquals("Testing " + i, Charsets.UTF_8.decode(events.get(i - 1).getBody()).toString());
    }
  }

  /**
   * Tests for async write to stream.
   */
  @Test
  public void testAsyncWrite() throws Exception {
    String streamId = "testAsync";

    streamClient.create(streamId);

    // Send 10 async writes
    int msgCount = 10;
    for (int i = 0; i < msgCount; i++) {
      streamClient.asyncSendEvent(streamId, "Testing " + i);
    }

    // Reads them back to verify. Needs to do it multiple times as the writes happens async.
    List<StreamEvent> events = Lists.newArrayList();

    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    while (events.size() != msgCount && stopwatch.elapsedTime(TimeUnit.SECONDS) < 10L) {
      events.clear();
      streamClient.getEvents(streamId, 0, Long.MAX_VALUE, msgCount, events);
    }

    Assert.assertEquals(msgCount, events.size());
    long lastTimestamp = 0L;
    for (int i = 0; i < msgCount; i++) {
      Assert.assertEquals("Testing " + i, Charsets.UTF_8.decode(events.get(i).getBody()).toString());
      lastTimestamp = events.get(i).getTimestamp();
    }

    // No more events
    stopwatch = new Stopwatch();
    stopwatch.start();
    events.clear();
    while (events.isEmpty() && stopwatch.elapsedTime(TimeUnit.SECONDS) < 1L) {
      events.clear();
      streamClient.getEvents(streamId, lastTimestamp + 1, Long.MAX_VALUE, msgCount, events);
    }





