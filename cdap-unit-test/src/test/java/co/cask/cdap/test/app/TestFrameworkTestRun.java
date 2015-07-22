/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.test.app;

import co.cask.cdap.ConfigTestApp;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.WorkerManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
@Category(SlowTests.class)
public class TestFrameworkTestRun extends TestFrameworkTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestFrameworkTestRun.class);

  private final Id.Namespace testSpace = Id.Namespace.from("testspace");

  @Before
  public void setUp() throws Exception {
    createNamespace(testSpace);
  }

  @Test
  public void testFlowRuntimeArguments() throws Exception {
    ApplicationManager applicationManager = deployApplication(FilterApp.class);
    Map<String, String> args = Maps.newHashMap();
    args.put("threshold", "10");
    applicationManager.getFlowManager("FilterFlow").start(args);

    StreamManager input = getStreamManager("input");
    input.send("1");
    input.send("11");

    ServiceManager serviceManager = applicationManager.getServiceManager("CountService").start();
    serviceManager.waitForStatus(true, 2, 1);

    Assert.assertEquals("1", new Gson().fromJson(
      callServiceGet(serviceManager.getServiceURL(), "result"), String.class));
  }

  @Test
  public void testNewFlowRuntimeArguments() throws Exception {
    ApplicationManager applicationManager = deployApplication(FilterAppWithNewFlowAPI.class);
    Map<String, String> args = Maps.newHashMap();
    args.put("threshold", "10");
    applicationManager.getFlowManager("FilterFlow").start(args);

    StreamManager input = getStreamManager("input");
    input.send("2");
    input.send("21");

    ServiceManager serviceManager = applicationManager.getServiceManager("CountService").start();
    serviceManager.waitForStatus(true, 2, 1);

    Assert.assertEquals("1", new Gson().fromJson(
      callServiceGet(serviceManager.getServiceURL(), "result"), String.class));
  }

  @Test
  public void testServiceManager() throws Exception {
    ApplicationManager applicationManager = deployApplication(FilterApp.class);
    ServiceManager countService = applicationManager.getServiceManager("CountService");
    countService.setInstances(2);
    Assert.assertEquals(0, countService.getProvisionedInstances());
    Assert.assertEquals(2, countService.getRequestedInstances());
    Assert.assertFalse(countService.isRunning());
    countService.start();
    Assert.assertTrue(countService.isRunning());
    Assert.assertEquals(2, countService.getProvisionedInstances());
  }

  @Test
  public void testAppConfigWithNull() throws Exception {
    testAppConfig(null);
  }

  @Test
  public void testAppConfig() throws Exception {
    testAppConfig(new ConfigTestApp.ConfigClass("testStream", "testDataset"));
  }

  private void testAppConfig(ConfigTestApp.ConfigClass object) throws Exception {
    String streamName = ConfigTestApp.DEFAULT_STREAM;
    String datasetName = ConfigTestApp.DEFAULT_TABLE;

    ApplicationManager appManager;
    if (object != null) {
      streamName = object.getStreamName();
      datasetName = object.getTableName();
      appManager = deployApplication(ConfigTestApp.class, object);
    } else {
      appManager = deployApplication(ConfigTestApp.class);
    }

    FlowManager flowManager = appManager.getFlowManager(ConfigTestApp.FLOW_NAME);
    flowManager.start();
    StreamManager streamManager = getStreamManager(streamName);
    streamManager.send("abcd");
    streamManager.send("xyz");
    RuntimeMetrics metrics = flowManager.getFlowletMetrics(ConfigTestApp.FLOWLET_NAME);
    metrics.waitForProcessed(2, 1, TimeUnit.MINUTES);
    flowManager.stop();
    DataSetManager<KeyValueTable> dsManager = getDataset(datasetName);
    KeyValueTable table = dsManager.get();
    Assert.assertEquals("abcd", Bytes.toString(table.read("abcd")));
    Assert.assertEquals("xyz", Bytes.toString(table.read("xyz")));
  }

  @Category(SlowTests.class)
  @Test
  public void testMapperDatasetAccess() throws Exception {
    addDatasetInstance("keyValueTable", "table1").create();
    addDatasetInstance("keyValueTable", "table2").create();
    DataSetManager<KeyValueTable> tableManager = getDataset("table1");
    KeyValueTable inputTable = tableManager.get();
    inputTable.write("hello", "world");
    tableManager.flush();

    ApplicationManager appManager = deployApplication(DatasetWithMRApp.class);
    Map<String, String> argsForMR = ImmutableMap.of(DatasetWithMRApp.INPUT_KEY, "table1",
                                                    DatasetWithMRApp.OUTPUT_KEY, "table2");
    MapReduceManager mrManager = appManager.getMapReduceManager(DatasetWithMRApp.MAPREDUCE_PROGRAM).start(argsForMR);
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    appManager.stopAll();

    DataSetManager<KeyValueTable> outTableManager = getDataset("table2");
    KeyValueTable outputTable = outTableManager.get();
    Assert.assertEquals("world", Bytes.toString(outputTable.read("hello")));
  }

  @Category(SlowTests.class)
  @Test
  public void testCustomActionDatasetAccess() throws Exception {
    addDatasetInstance("keyValueTable", DatasetWithCustomActionApp.CUSTOM_TABLE).create();
    addDatasetInstance("keyValueTable", DatasetWithCustomActionApp.CUSTOM_TABLE1).create();

    ApplicationManager appManager = deployApplication(DatasetWithCustomActionApp.class);
    ServiceManager serviceManager = appManager.getServiceManager(DatasetWithCustomActionApp.CUSTOM_SERVICE).start();
    serviceManager.waitForStatus(true);

    WorkflowManager workflowManager = appManager.getWorkflowManager(DatasetWithCustomActionApp.CUSTOM_WORKFLOW).start();
    workflowManager.waitForFinish(2, TimeUnit.MINUTES);
    appManager.stopAll();

    DataSetManager<KeyValueTable> outTableManager = getDataset(DatasetWithCustomActionApp.CUSTOM_TABLE);
    KeyValueTable outputTable = outTableManager.get();

    DataSetManager<KeyValueTable> outTableManager1 = getDataset(DatasetWithCustomActionApp.CUSTOM_TABLE1);
    KeyValueTable outputTable1 = outTableManager1.get();

    Assert.assertEquals("world", Bytes.toString(outputTable.read("hello")));
    Assert.assertEquals("service", Bytes.toString(outputTable.read("hi")));
    Assert.assertEquals("another", Bytes.toString(outputTable1.read("test")));
  }

  @Category(XSlowTests.class)
  @Test
  public void testDeployWorkflowApp() throws InterruptedException {
    ApplicationManager applicationManager = deployApplication(testSpace, AppWithSchedule.class);
    WorkflowManager wfmanager = applicationManager.getWorkflowManager("SampleWorkflow");
    List<ScheduleSpecification> schedules = wfmanager.getSchedules();
    Assert.assertEquals(1, schedules.size());
    String scheduleName = schedules.get(0).getSchedule().getName();
    Assert.assertNotNull(scheduleName);
    Assert.assertFalse(scheduleName.isEmpty());
    wfmanager.getSchedule(scheduleName).resume();

    List<RunRecord> history;
    int workflowRuns;
    workFlowHistoryCheck(5, wfmanager, 0);

    String status = wfmanager.getSchedule(scheduleName).status(200);
    Assert.assertEquals("SCHEDULED", status);

    wfmanager.getSchedule(scheduleName).suspend();
    workFlowStatusCheck(5, scheduleName, wfmanager, "SUSPENDED");

    TimeUnit.SECONDS.sleep(3);
    history = wfmanager.getHistory();
    workflowRuns = history.size();

    //Sleep for some time and verify there are no more scheduled jobs after the suspend.
    TimeUnit.SECONDS.sleep(10);
    int workflowRunsAfterSuspend = wfmanager.getHistory().size();
    Assert.assertEquals(workflowRuns, workflowRunsAfterSuspend);

    wfmanager.getSchedule(scheduleName).resume();

    //Check that after resume it goes to "SCHEDULED" state
    workFlowStatusCheck(5, scheduleName, wfmanager, "SCHEDULED");

    workFlowHistoryCheck(5, wfmanager, workflowRunsAfterSuspend);

    //check scheduled state
    Assert.assertEquals("SCHEDULED", wfmanager.getSchedule(scheduleName).status(200));

    //check status of non-existent schedule
    Assert.assertEquals("NOT_FOUND", wfmanager.getSchedule("doesnt exist").status(404));

    //suspend the schedule
    wfmanager.getSchedule(scheduleName).suspend();

    //Check that after suspend it goes to "SUSPENDED" state
    workFlowStatusCheck(5, scheduleName, wfmanager, "SUSPENDED");
  }

  private void workFlowHistoryCheck(int retries, WorkflowManager wfmanager, int expected) throws InterruptedException {
    int trial = 0;
    List<RunRecord> history;
    int workflowRuns = 0;
    while (trial++ < retries) {
      history = wfmanager.getHistory();
      workflowRuns = history.size();
      if (workflowRuns > expected) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertTrue(workflowRuns > expected);
  }

  private void workFlowStatusCheck(int retries, String scheduleId, WorkflowManager wfmanager,
                                   String expected) throws InterruptedException {
    int trial = 0;
    String status = null;
    while (trial++ < retries) {
      status = wfmanager.getSchedule(scheduleId).status(200);
      if (status.equals(expected)) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertEquals(status, expected);
  }


  @Category(XSlowTests.class)
  @Test(timeout = 240000)
  @Ignore
  // TODO: Investigate why this fails in Bamboo, but not locally
  public void testMultiInput() throws Exception {
    ApplicationManager applicationManager = deployApplication(JoinMultiStreamApp.class);
    applicationManager.getFlowManager("JoinMultiFlow").start();

    StreamManager s1 = getStreamManager("s1");
    StreamManager s2 = getStreamManager("s2");
    StreamManager s3 = getStreamManager("s3");

    s1.send("testing 1");
    s2.send("testing 2");
    s3.send("testing 3");

    RuntimeMetrics terminalMetrics = RuntimeStats.getFlowletMetrics("JoinMulti",
                                                                    "JoinMultiFlow", "Terminal");
    terminalMetrics.waitForProcessed(3, 60, TimeUnit.SECONDS);
    TimeUnit.SECONDS.sleep(1);

    ServiceManager queryManager = applicationManager.getServiceManager("QueryService").start();
    queryManager.waitForStatus(true, 2, 1);
    URL serviceURL = queryManager.getServiceURL();
    Gson gson = new Gson();

    Assert.assertEquals("testing 1", gson.fromJson(callServiceGet(serviceURL, "input1"), String.class));
    Assert.assertEquals("testing 2", gson.fromJson(callServiceGet(serviceURL, "input2"), String.class));
    Assert.assertEquals("testing 3", gson.fromJson(callServiceGet(serviceURL, "input3"), String.class));
  }

  @Category(XSlowTests.class)
  @Test(timeout = 360000)
  public void testApp() throws Exception {
    testApp(WordCountApp.class, "text");
  }

  @Category(SlowTests.class)
  @Test
  public void testGetServiceURL() throws Exception {
    ApplicationManager applicationManager = deployApplication(AppUsingGetServiceURL.class);
    ServiceManager centralServiceManager =
      applicationManager.getServiceManager(AppUsingGetServiceURL.CENTRAL_SERVICE).start();
    centralServiceManager.waitForStatus(true);

    WorkerManager pingingWorker = applicationManager.getWorkerManager(AppUsingGetServiceURL.PINGING_WORKER).start();
    pingingWorker.waitForStatus(true);

    // Test service's getServiceURL
    ServiceManager serviceManager = applicationManager.getServiceManager(AppUsingGetServiceURL.FORWARDING).start();
    String result = callServiceGet(serviceManager.getServiceURL(), "ping");
    String decodedResult = new Gson().fromJson(result, String.class);
    // Verify that the service was able to hit the CentralService and retrieve the answer.
    Assert.assertEquals(AppUsingGetServiceURL.ANSWER, decodedResult);

    result = callServiceGet(serviceManager.getServiceURL(), "read/" + AppUsingGetServiceURL.DATASET_KEY);
    decodedResult = new Gson().fromJson(result, String.class);
    Assert.assertEquals(AppUsingGetServiceURL.ANSWER, decodedResult);

    serviceManager.stop();

    pingingWorker.stop();
    pingingWorker.waitForStatus(false);

    centralServiceManager.stop();
    centralServiceManager.waitForStatus(false);
  }

  /**
   * Checks to ensure that a particular runnable of the {@param serviceManager} has {@param expected} number of
   * instances. If the initial check fails, it performs {@param retries} more attempts, sleeping 1 second before each
   * successive attempt.
   */
  private void workerInstancesCheck(WorkerManager workerManager, int expected,
                                    int retries) throws InterruptedException {
    for (int i = 0; i <= retries; i++) {
      int actualInstances = workerManager.getInstances();
      if (actualInstances == expected) {
        return;
      }
      if (i == retries) {
        Assert.assertEquals(expected, actualInstances);
      }
      TimeUnit.SECONDS.sleep(1);
    }
  }

  @Category(SlowTests.class)
  @Test
  public void testWorkerInstances() throws Exception {
    ApplicationManager applicationManager = deployApplication(testSpace, AppUsingGetServiceURL.class);
    WorkerManager workerManager = applicationManager.getWorkerManager(AppUsingGetServiceURL.PINGING_WORKER).start();
    workerManager.waitForStatus(true);

    int retries = 5;

    // Should be 5 instances when first started.
    workerInstancesCheck(workerManager, 5, retries);

    // Test increasing instances.
    workerManager.setInstances(10);
    workerInstancesCheck(workerManager, 10, retries);

    // Test decreasing instances.
    workerManager.setInstances(2);
    workerInstancesCheck(workerManager, 2, retries);

    // Test requesting same number of instances.
    workerManager.setInstances(2);
    workerInstancesCheck(workerManager, 2, retries);

    WorkerManager lifecycleWorkerManager =
      applicationManager.getWorkerManager(AppUsingGetServiceURL.LIFECYCLE_WORKER).start();
    lifecycleWorkerManager.waitForStatus(true);

    // Set 5 instances for the LifecycleWorker
    lifecycleWorkerManager.setInstances(5);
    workerInstancesCheck(lifecycleWorkerManager, 5, retries);

    lifecycleWorkerManager.stop();
    lifecycleWorkerManager.waitForStatus(false);

    workerManager.stop();
    workerManager.waitForStatus(false);

    // Should be same instances after being stopped.
    workerInstancesCheck(lifecycleWorkerManager, 5, retries);
    workerInstancesCheck(workerManager, 2, retries);

    // Assert the LifecycleWorker dataset writes
    // 3 workers should have started with 3 total instances. 2 more should later start with 5 total instances.
    assertWorkerDatasetWrites(applicationManager, Bytes.toBytes("init"),
                              Bytes.stopKeyForPrefix(Bytes.toBytes("init.2")), 3, 3);
    assertWorkerDatasetWrites(applicationManager, Bytes.toBytes("init.3"),
                              Bytes.stopKeyForPrefix(Bytes.toBytes("init")), 2, 5);

    // Test that the worker had 5 instances when stopped, and each knew that there were 5 instances
    byte[] startRow = Bytes.toBytes("stop");
    assertWorkerDatasetWrites(applicationManager, startRow, Bytes.stopKeyForPrefix(startRow), 5, 5);
  }

  private void assertWorkerDatasetWrites(ApplicationManager applicationManager, byte[] startRow, byte[] endRow,
                                         int expectedCount, int expectedTotalCount) throws Exception {
    DataSetManager<KeyValueTable> datasetManager = applicationManager.getDataSet(
      AppUsingGetServiceURL.WORKER_INSTANCES_DATASET);
    KeyValueTable instancesTable = datasetManager.get();
    CloseableIterator<KeyValue<byte[], byte[]>> instancesIterator = instancesTable.scan(startRow, endRow);
    try {
      List<KeyValue<byte[], byte[]>> workerInstances = Lists.newArrayList(instancesIterator);
      // Assert that the worker starts with expectedCount instances
      Assert.assertEquals(expectedCount, workerInstances.size());
      // Assert that each instance of the worker knows the total number of instances
      for (KeyValue<byte[], byte[]> keyValue : workerInstances) {
        Assert.assertEquals(expectedTotalCount, Bytes.toInt(keyValue.getValue()));
      }
    } finally {
      instancesIterator.close();
    }
  }

  @Category(SlowTests.class)
  @Test(expected = IllegalArgumentException.class)
  public void testServiceWithInvalidHandler() throws Exception {
    deployApplication(AppWithInvalidHandler.class);
  }

  @Category(SlowTests.class)
  @Test
  public void testAppWithWorker() throws Exception {
    ApplicationManager applicationManager = deployApplication(testSpace, AppWithWorker.class);
    LOG.info("Deployed.");
    WorkerManager manager = applicationManager.getWorkerManager(AppWithWorker.WORKER).start();
    TimeUnit.MILLISECONDS.sleep(200);
    manager.stop();
    applicationManager.stopAll();
    DataSetManager<KeyValueTable> dataSetManager = getDataset(testSpace, AppWithWorker.DATASET);
    KeyValueTable table = dataSetManager.get();
    Assert.assertEquals(AppWithWorker.INITIALIZE, Bytes.toString(table.read(AppWithWorker.INITIALIZE)));
    Assert.assertEquals(AppWithWorker.RUN, Bytes.toString(table.read(AppWithWorker.RUN)));
    Assert.assertEquals(AppWithWorker.STOP, Bytes.toString(table.read(AppWithWorker.STOP)));
  }

  @Category(SlowTests.class)
  @Test
  public void testAppWithServices() throws Exception {
    ApplicationManager applicationManager = deployApplication(AppWithServices.class);
    LOG.info("Deployed.");
    ServiceManager serviceManager = applicationManager.getServiceManager(AppWithServices.SERVICE_NAME).start();
    serviceManager.waitForStatus(true);

    LOG.info("Service Started");

    URL serviceURL = serviceManager.getServiceURL(15, TimeUnit.SECONDS);
    Assert.assertNotNull(serviceURL);

    // Call the ping endpoint
    URL url = new URL(serviceURL, "ping2");
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());

    // Call the failure endpoint
    url = new URL(serviceURL, "failure");
    request = HttpRequest.get(url).build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(500, response.getResponseCode());
    Assert.assertTrue(response.getResponseBodyAsString().contains("Transaction failure"));

    // Call the verify ClassLoader endpoint
    url = new URL(serviceURL, "verifyClassLoader");
    request = HttpRequest.get(url).build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());

    RuntimeMetrics serviceMetrics = RuntimeStats.getServiceMetrics(AppWithServices.APP_NAME,
                                                                   AppWithServices.SERVICE_NAME);
    serviceMetrics.waitForinput(3, 5, TimeUnit.SECONDS);
    Assert.assertEquals(3, serviceMetrics.getInput());
    Assert.assertEquals(2, serviceMetrics.getProcessed());
    Assert.assertEquals(1, serviceMetrics.getException());

    // in the AppWithServices the handlerName is same as the serviceName - "ServerService" handler
    RuntimeMetrics handlerMetrics = RuntimeStats.getServiceHandlerMetrics(AppWithServices.APP_NAME,
                                                                          AppWithServices.SERVICE_NAME,
                                                                          AppWithServices.SERVICE_NAME);
    handlerMetrics.waitForinput(3, 5, TimeUnit.SECONDS);
    Assert.assertEquals(3, handlerMetrics.getInput());
    Assert.assertEquals(2, handlerMetrics.getProcessed());
    Assert.assertEquals(1, handlerMetrics.getException());

    // we can verify metrics, by adding getServiceMetrics in RuntimeStats and then disabling the system scope test in
    // TestMetricsCollectionService

    LOG.info("DatasetUpdateService Started");
    Map<String, String> args
      = ImmutableMap.of(AppWithServices.WRITE_VALUE_RUN_KEY, AppWithServices.DATASET_TEST_VALUE,
                        AppWithServices.WRITE_VALUE_STOP_KEY, AppWithServices.DATASET_TEST_VALUE_STOP);
    ServiceManager datasetWorkerServiceManager = applicationManager
      .getServiceManager(AppWithServices.DATASET_WORKER_SERVICE_NAME).start(args);
    WorkerManager datasetWorker =
      applicationManager.getWorkerManager(AppWithServices.DATASET_UPDATE_WORKER).start(args);
    datasetWorkerServiceManager.waitForStatus(true);

    ServiceManager noopManager = applicationManager.getServiceManager("NoOpService").start();
    serviceManager.waitForStatus(true, 2, 1);

    String result = callServiceGet(noopManager.getServiceURL(), "ping/" + AppWithServices.DATASET_TEST_KEY);
    String decodedResult = new Gson().fromJson(result, String.class);
    Assert.assertEquals(AppWithServices.DATASET_TEST_VALUE, decodedResult);

    handlerMetrics = RuntimeStats.getServiceHandlerMetrics(AppWithServices.APP_NAME,
                                                                          "NoOpService",
                                                                          "NoOpHandler");
    handlerMetrics.waitForinput(1, 5, TimeUnit.SECONDS);
    Assert.assertEquals(1, handlerMetrics.getInput());
    Assert.assertEquals(1, handlerMetrics.getProcessed());
    Assert.assertEquals(0, handlerMetrics.getException());

    // Test that a service can discover another service
    String path = String.format("discover/%s/%s",
                                AppWithServices.APP_NAME, AppWithServices.DATASET_WORKER_SERVICE_NAME);
    url = new URL(serviceURL, path);
    request = HttpRequest.get(url).build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());

    datasetWorker.stop();
    datasetWorkerServiceManager.stop();
    datasetWorkerServiceManager.waitForStatus(false);
    LOG.info("DatasetUpdateService Stopped");
    serviceManager.stop();
    serviceManager.waitForStatus(false);
    LOG.info("ServerService Stopped");

    result = callServiceGet(noopManager.getServiceURL(), "ping/" + AppWithServices.DATASET_TEST_KEY_STOP);
    decodedResult = new Gson().fromJson(result, String.class);
    Assert.assertEquals(AppWithServices.DATASET_TEST_VALUE_STOP, decodedResult);

    result = callServiceGet(noopManager.getServiceURL(), "ping/" + AppWithServices.DATASET_TEST_KEY_STOP_2);
    decodedResult = new Gson().fromJson(result, String.class);
    Assert.assertEquals(AppWithServices.DATASET_TEST_VALUE_STOP_2, decodedResult);
  }

  @Test
  public void testTransactionHandlerService() throws Exception {
    ApplicationManager applicationManager = deployApplication(testSpace, AppWithServices.class);
    LOG.info("Deployed.");
    ServiceManager serviceManager =
      applicationManager.getServiceManager(AppWithServices.TRANSACTIONS_SERVICE_NAME).start();
    serviceManager.waitForStatus(true);

    LOG.info("Service Started");


    final URL baseUrl = serviceManager.getServiceURL(15, TimeUnit.SECONDS);
    Assert.assertNotNull(baseUrl);

    // Make a request to write in a separate thread and wait for it to return.
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future<Integer> requestFuture = executorService.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        try {
          URL url = new URL(String.format("%s/write/%s/%s/%d",
                                          baseUrl,
                                          AppWithServices.DATASET_TEST_KEY,
                                          AppWithServices.DATASET_TEST_VALUE,
                                          10000));
          HttpRequest request = HttpRequest.get(url).build();
          HttpResponse response = HttpRequests.execute(request);
          return response.getResponseCode();
        } catch (Exception e) {
          LOG.error("Request thread got exception.", e);
          throw Throwables.propagate(e);
        }
      }
    });

    // The dataset should not be written by the time this request is made, since the transaction to write
    // has not been committed yet.
    URL url = new URL(String.format("%s/read/%s", baseUrl, AppWithServices.DATASET_TEST_KEY));
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(204, response.getResponseCode());

    // Wait for the transaction to commit.
    Integer writeStatusCode = requestFuture.get();
    Assert.assertEquals(200, writeStatusCode.intValue());

    // Make the same request again. By now the transaction should've completed.
    request = HttpRequest.get(url).build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(AppWithServices.DATASET_TEST_VALUE,
                        new Gson().fromJson(response.getResponseBodyAsString(), String.class));

    executorService.shutdown();
    serviceManager.stop();
    serviceManager.waitForStatus(false);

    DataSetManager<KeyValueTable> dsManager = getDataset(testSpace, AppWithServices.TRANSACTIONS_DATASET_NAME);
    String value = Bytes.toString(dsManager.get().read(AppWithServices.DESTROY_KEY));
    Assert.assertEquals(AppWithServices.VALUE, value);
  }

  // todo: passing stream name as a workaround for not cleaning up streams during reset()
  private void testApp(Class<? extends Application> app, String streamName) throws Exception {

    ApplicationManager applicationManager = deployApplication(app);
    applicationManager.getFlowManager("WordCountFlow").start();

    // Send some inputs to streams
    StreamManager streamManager = getStreamManager(streamName);
    for (int i = 0; i < 100; i++) {
      streamManager.send(ImmutableMap.of("title", "title " + i), "testing message " + i);
    }

    // Check the flowlet metrics
    RuntimeMetrics flowletMetrics = RuntimeStats.getFlowletMetrics("WordCountApp",
                                                                   "WordCountFlow",
                                                                   "CountByField");
    flowletMetrics.waitForProcessed(500, 10, TimeUnit.SECONDS);
    Assert.assertEquals(0L, flowletMetrics.getException());

    // Query the result
    ServiceManager serviceManager = applicationManager.getServiceManager("WordFrequency").start();
    serviceManager.waitForStatus(true, 2, 1);

    // Verify the query result
    Type resultType = new TypeToken<Map<String, Long>>() { }.getType();
    Map<String, Long> result = new Gson().fromJson(
      callServiceGet(serviceManager.getServiceURL(), "wordfreq/" + streamName + ":testing"), resultType);
    Assert.assertEquals(100L, result.get(streamName + ":testing").longValue());

    // check the metrics
    RuntimeMetrics serviceMetrics = RuntimeStats.getServiceMetrics("WordCountApp", "WordFrequency");
    serviceMetrics.waitForProcessed(1, 5, TimeUnit.SECONDS);
    Assert.assertEquals(0L, serviceMetrics.getException());

    // Run mapreduce job
    MapReduceManager mrManager = applicationManager.getMapReduceManager("countTotal").start();
    mrManager.waitForFinish(1800L, TimeUnit.SECONDS);

    long totalCount = Long.valueOf(callServiceGet(serviceManager.getServiceURL(), "total"));
    // every event has 5 tokens
    Assert.assertEquals(5 * 100L, totalCount);

    // Run mapreduce from stream
    mrManager = applicationManager.getMapReduceManager("countFromStream").start();
    mrManager.waitForFinish(120L, TimeUnit.SECONDS);

    totalCount = Long.valueOf(callServiceGet(serviceManager.getServiceURL(), "stream_total"));
    // The stream MR only consume the body, not the header.
    Assert.assertEquals(3 * 100L, totalCount);

    DataSetManager<MyKeyValueTableDefinition.KeyValueTable> mydatasetManager = getDataset("mydataset");
    Assert.assertEquals(100L, Long.valueOf(mydatasetManager.get().get("title:title")).longValue());
  }

  @Category(SlowTests.class)
  @Test
  public void testGenerator() throws InterruptedException, IOException, TimeoutException {
    ApplicationManager applicationManager = deployApplication(testSpace, GenSinkApp2.class);
    applicationManager.getFlowManager("GenSinkFlow").start();

    // Check the flowlet metrics
    RuntimeMetrics genMetrics = RuntimeStats.getFlowletMetrics(testSpace.getId(),
                                                               "GenSinkApp",
                                                               "GenSinkFlow",
                                                               "GenFlowlet");

    RuntimeMetrics sinkMetrics = RuntimeStats.getFlowletMetrics(testSpace.getId(),
                                                                "GenSinkApp",
                                                                "GenSinkFlow",
                                                                "SinkFlowlet");

    RuntimeMetrics batchSinkMetrics = RuntimeStats.getFlowletMetrics(testSpace.getId(),
                                                                     "GenSinkApp",
                                                                     "GenSinkFlow",
                                                                     "BatchSinkFlowlet");

    // Generator generators 99 events + 99 batched events
    sinkMetrics.waitFor("system.process.events.in", 198, 5, TimeUnit.SECONDS);
    sinkMetrics.waitForProcessed(198, 5, TimeUnit.SECONDS);
    Assert.assertEquals(0L, sinkMetrics.getException());

    // Batch sink only get the 99 batch events
    batchSinkMetrics.waitFor("system.process.events.in", 99, 5, TimeUnit.SECONDS);
    batchSinkMetrics.waitForProcessed(99, 5, TimeUnit.SECONDS);
    Assert.assertEquals(0L, batchSinkMetrics.getException());

    Assert.assertEquals(1L, genMetrics.getException());
  }

  @Test
  public void testAppRedeployKeepsData() throws Exception {
    deployApplication(testSpace, AppWithTable.class);
    DataSetManager<Table> myTableManager = getDataset(testSpace, "my_table");
    myTableManager.get().put(new Put("key1", "column1", "value1"));
    myTableManager.flush();

    // Changes should be visible to other instances of datasets
    DataSetManager<Table> myTableManager2 = getDataset(testSpace, "my_table");
    Assert.assertEquals("value1", myTableManager2.get().get(new Get("key1", "column1")).getString("column1"));

    // Even after redeploy of an app: changes should be visible to other instances of datasets
    deployApplication(AppWithTable.class);
    DataSetManager<Table> myTableManager3 = getDataset(testSpace, "my_table");
    Assert.assertEquals("value1", myTableManager3.get().get(new Get("key1", "column1")).getString("column1"));

    // Calling commit again (to test we can call it multiple times)
    myTableManager.get().put(new Put("key1", "column1", "value2"));
    myTableManager.flush();

    Assert.assertEquals("value1", myTableManager3.get().get(new Get("key1", "column1")).getString("column1"));
  }

  @Test(timeout = 60000L)
  public void testFlowletMetricsReset() throws Exception {
    ApplicationManager appManager = deployApplication(DataSetInitApp.class);
    FlowManager flowManager = appManager.getFlowManager("DataSetFlow").start();
    RuntimeMetrics flowletMetrics = RuntimeStats.getFlowletMetrics("DataSetInitApp", "DataSetFlow", "Consumer");
    flowletMetrics.waitForProcessed(1, 5, TimeUnit.SECONDS);
    flowManager.stop();
    Assert.assertEquals(1, flowletMetrics.getProcessed());
    RuntimeStats.resetAll();
    // check the metrics were deleted after reset
    Assert.assertEquals(0, flowletMetrics.getProcessed());
  }

  @Test(timeout = 60000L)
  public void testFlowletInitAndSetInstances() throws Exception {
    ApplicationManager appManager = deployApplication(testSpace, DataSetInitApp.class);
    FlowManager flowManager = appManager.getFlowManager("DataSetFlow").start();

    RuntimeMetrics flowletMetrics = RuntimeStats.getFlowletMetrics(testSpace.getId(), "DataSetInitApp",
                                                                   "DataSetFlow", "Consumer");

    flowletMetrics.waitForProcessed(1, 5, TimeUnit.SECONDS);

    String generator = "Generator";
    Assert.assertEquals(1, flowManager.getFlowletInstances(generator));
    // Now change generator to 3 instances
    flowManager.setFlowletInstances(generator, 3);
    Assert.assertEquals(3, flowManager.getFlowletInstances(generator));

    // Now should have 3 processed from the consumer flowlet
    flowletMetrics.waitForProcessed(3, 10, TimeUnit.SECONDS);

    // Now reset to 1 instances
    flowManager.setFlowletInstances(generator, 1);
    Assert.assertEquals(1, flowManager.getFlowletInstances(generator));

    // Shouldn't have new item
    TimeUnit.SECONDS.sleep(3);
    Assert.assertEquals(3, flowletMetrics.getProcessed());

    // Now set to 2 instances again. Since there is a new instance, expect one new item emitted
    flowManager.setFlowletInstances(generator, 2);
    Assert.assertEquals(2, flowManager.getFlowletInstances(generator));
    flowletMetrics.waitForProcessed(4, 10, TimeUnit.SECONDS);

    flowManager.stop();

    DataSetManager<Table> dataSetManager = getDataset(testSpace, "conf");
    Table confTable = dataSetManager.get();

    Assert.assertEquals("generator", confTable.get(new Get("key", "column")).getString("column"));

    dataSetManager.flush();
  }

  @Test(timeout = 60000L)
  public void testAppWithAutoDeployDatasetModule() throws Exception {
    testAppWithDataset(AppsWithDataset.AppWithAutoDeploy.class, "MyService");
  }

  @Test(timeout = 60000L)
  public void testAppWithAutoDeployDataset() throws Exception {
    deployDatasetModule("my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    // we should be fine if module is already there. Deploy of module should not happen
    testAppWithDataset(AppsWithDataset.AppWithAutoDeploy.class, "MyService");
  }

  @Test(timeout = 60000L)
  public void testAppWithAutoCreateDataset() throws Exception {
    deployDatasetModule("my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    testAppWithDataset(AppsWithDataset.AppWithAutoCreate.class, "MyService");
  }

  @Test(timeout = 60000L)
  public void testAppWithExistingDataset() throws Exception {
    deployDatasetModule("my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    addDatasetInstance("myKeyValueTable", "myTable", DatasetProperties.EMPTY).create();
    testAppWithDataset(AppsWithDataset.AppWithExisting.class, "MyService");
  }

  @Test(timeout = 60000L)
  public void testAppWithExistingDatasetInjectedByAnnotation() throws Exception {
    deployDatasetModule("my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    addDatasetInstance("myKeyValueTable", "myTable", DatasetProperties.EMPTY).create();
    testAppWithDataset(AppsWithDataset.AppUsesAnnotation.class, "MyServiceWithUseDataSetAnnotation");
  }

  @Test(timeout = 60000L)
  public void testDatasetWithoutApp() throws Exception {
    // TODO: Although this has nothing to do with this testcase, deploying a dummy app to create the default namespace
    deployApplication(testSpace, DummyApp.class);
    deployDatasetModule(testSpace, "my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    addDatasetInstance(testSpace, "myKeyValueTable", "myTable", DatasetProperties.EMPTY).create();
    DataSetManager<AppsWithDataset.KeyValueTableDefinition.KeyValueTable> dataSetManager =
      getDataset(testSpace, "myTable");
    AppsWithDataset.KeyValueTableDefinition.KeyValueTable kvTable = dataSetManager.get();
    kvTable.put("test", "hello");
    dataSetManager.flush();
    Assert.assertEquals("hello", dataSetManager.get().get("test"));
  }

  @Test(timeout = 60000L)
  public void testAppWithAutoDeployDatasetType() throws Exception {
    testAppWithDataset(AppsWithDataset.AppWithAutoDeployType.class, "MyService");
  }

  @Test(timeout = 60000L)
  public void testAppWithAutoDeployDatasetTypeShortcut() throws Exception {
    testAppWithDataset(AppsWithDataset.AppWithAutoDeployTypeShortcut.class, "MyService");
  }

  private void testAppWithDataset(Class<? extends Application> app, String serviceName) throws Exception {
    ApplicationManager applicationManager = deployApplication(app);
    // Query the result
    ServiceManager serviceManager = applicationManager.getServiceManager(serviceName).start();
    serviceManager.waitForStatus(true, 2, 1);
    callServicePut(serviceManager.getServiceURL(), "key1", "value1");
    String response = callServiceGet(serviceManager.getServiceURL(), "key1");
    Assert.assertEquals("value1", new Gson().fromJson(response, String.class));
  }

  @Test(timeout = 90000L)
  public void testSQLQuery() throws Exception {
    // Deploying app makes sure that the default namespace is available.
    deployApplication(testSpace, DummyApp.class);
    deployDatasetModule(testSpace, "my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    deployApplication(testSpace, AppsWithDataset.AppWithAutoCreate.class);
    DataSetManager<AppsWithDataset.KeyValueTableDefinition.KeyValueTable> myTableManager =
      getDataset(testSpace, "myTable");
    AppsWithDataset.KeyValueTableDefinition.KeyValueTable kvTable = myTableManager.get();
    kvTable.put("a", "1");
    kvTable.put("b", "2");
    kvTable.put("c", "1");
    myTableManager.flush();

    try (
      Connection connection = getQueryClient(testSpace);
      ResultSet results = connection.prepareStatement("select first from dataset_mytable where second = '1'")
                                    .executeQuery()
    ) {
      // run a query over the dataset
      Assert.assertTrue(results.next());
      Assert.assertEquals("a", results.getString(1));
      Assert.assertTrue(results.next());
      Assert.assertEquals("c", results.getString(1));
      Assert.assertFalse(results.next());
    }
  }


  @Category(XSlowTests.class)
  @Test
  public void testByteCodeClassLoader() throws Exception {
    // This test verify bytecode generated classes ClassLoading

    ApplicationManager appManager = deployApplication(testSpace, ClassLoaderTestApp.class);
    FlowManager flowManager = appManager.getFlowManager("BasicFlow").start();

    // Wait for at least 10 records being generated
    RuntimeMetrics flowMetrics = RuntimeStats.getFlowletMetrics(testSpace.getId(), "ClassLoaderTestApp",
                                                                "BasicFlow", "Sink");
    flowMetrics.waitForProcessed(10, 5000, TimeUnit.MILLISECONDS);
    flowManager.stop();

    ServiceManager serviceManager = appManager.getServiceManager("RecordQuery").start();
    URL serviceURL = serviceManager.getServiceURL(15, TimeUnit.SECONDS);
    Assert.assertNotNull(serviceURL);

    // Query record
    URL url = new URL(serviceURL, "query?type=public");
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());

    long count = Long.parseLong(response.getResponseBodyAsString());
    serviceManager.stop();

    // Verify the record count with dataset
    DataSetManager<KeyValueTable> recordsManager = getDataset(testSpace, "records");
    KeyValueTable records = recordsManager.get();
    Assert.assertTrue(count == Bytes.toLong(records.read("PUBLIC")));
  }

  private String callServiceGet(URL serviceURL, String path) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL(serviceURL.toString() + path).openConnection();
    try (
      BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), Charsets.UTF_8))
    ) {
      Assert.assertEquals(200, connection.getResponseCode());
      return reader.readLine();
    }
  }

  private String callServicePut(URL serviceURL, String path, String body) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL(serviceURL.toString() + path).openConnection();
    connection.setDoOutput(true);
    connection.setRequestMethod("PUT");
    OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
    out.write(body);
    out.close();
    try (
      BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), Charsets.UTF_8))
    ) {
      Assert.assertEquals(200, connection.getResponseCode());
      return reader.readLine();
    }
  }
}

