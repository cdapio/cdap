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
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ProcedureClient;
import co.cask.cdap.test.ProcedureManager;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.WorkerManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collections;
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

  @Test
  public void testFlowRuntimeArguments() throws Exception {
    ApplicationManager applicationManager = deployApplication(FilterApp.class);
    Map<String, String> args = Maps.newHashMap();
    args.put("threshold", "10");
    applicationManager.startFlow("FilterFlow", args);

    StreamWriter input = applicationManager.getStreamWriter("input");
    input.send("1");
    input.send("11");

    ProcedureManager queryManager = applicationManager.startProcedure("Count");
    ProcedureClient client = queryManager.getClient();
    Gson gson = new Gson();

    //Adding sleep so that the test does not fail if the procedure takes sometime to start on slow machines.
    //TODO : Can be removed after fixing JIRA - CDAP-15
    TimeUnit.SECONDS.sleep(2);

    Assert.assertEquals("1",
                        gson.fromJson(client.query("result", ImmutableMap.of("type", "highpass")), String.class));
  }

  @Category(XSlowTests.class)
  @Test
  public void testDeployWorkflowApp() throws InterruptedException {
    ApplicationManager applicationManager = deployApplication(AppWithSchedule.class);
    WorkflowManager wfmanager = applicationManager.startWorkflow("SampleWorkflow", null);
    List<ScheduleSpecification> schedules = wfmanager.getSchedules();
    Assert.assertEquals(1, schedules.size());
    String scheduleName = schedules.get(0).getSchedule().getName();
    Assert.assertNotNull(scheduleName);
    Assert.assertFalse(scheduleName.isEmpty());

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
  public void testMultiInput() throws InterruptedException, IOException, TimeoutException {
    ApplicationManager applicationManager = deployApplication(JoinMultiStreamApp.class);
    applicationManager.startFlow("JoinMultiFlow");

    StreamWriter s1 = applicationManager.getStreamWriter("s1");
    StreamWriter s2 = applicationManager.getStreamWriter("s2");
    StreamWriter s3 = applicationManager.getStreamWriter("s3");

    s1.send("testing 1");
    s2.send("testing 2");
    s3.send("testing 3");

    RuntimeMetrics terminalMetrics = RuntimeStats.getFlowletMetrics("JoinMulti", "JoinMultiFlow", "Terminal");

    terminalMetrics.waitForProcessed(3, 5, TimeUnit.SECONDS);

    TimeUnit.SECONDS.sleep(1);

    ProcedureManager queryManager = applicationManager.startProcedure("Query");
    Gson gson = new Gson();

    ProcedureClient client = queryManager.getClient();
    Assert.assertEquals("testing 1",
                        gson.fromJson(client.query("get", ImmutableMap.of("key", "input1")), String.class));
    Assert.assertEquals("testing 2",
                        gson.fromJson(client.query("get", ImmutableMap.of("key", "input2")), String.class));
    Assert.assertEquals("testing 3",
                        gson.fromJson(client.query("get", ImmutableMap.of("key", "input3")), String.class));
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
    ServiceManager centralServiceManager = applicationManager.startService(AppUsingGetServiceURL.CENTRAL_SERVICE);
    centralServiceManager.waitForStatus(true);

    // Test procedure's getServiceURL
    ProcedureManager procedureManager = applicationManager.startProcedure(AppUsingGetServiceURL.PROCEDURE);
    ProcedureClient procedureClient = procedureManager.getClient();
    String result = procedureClient.query("ping", Collections.<String, String>emptyMap());
    String decodedResult = new Gson().fromJson(result, String.class);
    // Verify that the procedure was able to hit the CentralService and retrieve the answer.
    Assert.assertEquals(AppUsingGetServiceURL.ANSWER, decodedResult);


    // Test serviceWorker's getServiceURL
    ServiceManager serviceWithWorker = applicationManager.startService(AppUsingGetServiceURL.SERVICE_WITH_WORKER);
    serviceWithWorker.waitForStatus(true);
    // Since the worker is passive (we can not ping it), allow the service worker 2 seconds to ping
    // the CentralService, get the appropriate response, and write to to a dataset.
    Thread.sleep(2000);
    serviceWithWorker.stop();
    serviceWithWorker.waitForStatus(false);

    result = procedureClient.query("readDataSet", ImmutableMap.of(AppUsingGetServiceURL.DATASET_WHICH_KEY,
                                                                  AppUsingGetServiceURL.DATASET_KEY));
    decodedResult = new Gson().fromJson(result, String.class);
    Assert.assertEquals(AppUsingGetServiceURL.ANSWER, decodedResult);
    procedureManager.stop();

    centralServiceManager.stop();
    centralServiceManager.waitForStatus(false);
  }

  /**
   * Checks to ensure that a particular runnable of the {@param serviceManager} has {@param expected} number of
   * instances. If the initial check fails, it performs {@param retries} more attempts, sleeping 1 second before each
   * successive attempt.
   */
  private void runnableInstancesCheck(ServiceManager serviceManager, String runnableName,
                                      int expected, int retries, String instanceType) throws InterruptedException {
    for (int i = 0; i <= retries; i++) {
      int actualInstances;
      if ("requested".equals(instanceType)) {
        actualInstances = serviceManager.getRequestedInstances(runnableName);
      } else if ("provisioned".equals(instanceType)) {
        actualInstances = serviceManager.getProvisionedInstances(runnableName);
      } else {
        String error = String.format("instanceType can be 'requested' or 'provisioned'. Found %s.", instanceType);
        throw new IllegalArgumentException(error);
      }
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
  public void testServiceRunnableInstances() throws Exception {
    ApplicationManager applicationManager = deployApplication(AppUsingGetServiceURL.class);
    ServiceManager serviceManager = applicationManager.startService(AppUsingGetServiceURL.SERVICE_WITH_WORKER);
    serviceManager.waitForStatus(true);

    String runnableName = AppUsingGetServiceURL.SERVICE_WITH_WORKER;
    int retries = 5;

    // Should be 1 instance when first started.
    runnableInstancesCheck(serviceManager, runnableName, 1, retries, "provisioned");

    // Test increasing instances.
    serviceManager.setRunnableInstances(runnableName, 5);
    runnableInstancesCheck(serviceManager, runnableName, 5, retries, "provisioned");

    // Test decreasing instances.
    serviceManager.setRunnableInstances(runnableName, 2);
    runnableInstancesCheck(serviceManager, runnableName, 2, retries, "provisioned");

    // Test requesting same number of instances.
    serviceManager.setRunnableInstances(runnableName, 2);
    runnableInstancesCheck(serviceManager, runnableName, 2, retries, "provisioned");

    // Set 5 instances for the LifecycleWorker
    serviceManager.setRunnableInstances(AppUsingGetServiceURL.LIFECYCLE_WORKER, 5);
    runnableInstancesCheck(serviceManager, AppUsingGetServiceURL.LIFECYCLE_WORKER, 5, retries, "provisioned");

    serviceManager.stop();
    serviceManager.waitForStatus(false);

    // Should be 0 instances when stopped.
    runnableInstancesCheck(serviceManager, runnableName, 0, retries, "provisioned");
    runnableInstancesCheck(serviceManager, AppUsingGetServiceURL.LIFECYCLE_WORKER, 0, retries, "provisioned");

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
    DataSetManager<KeyValueTable> datasetManager = applicationManager
      .getDataSet(AppUsingGetServiceURL.WORKER_INSTANCES_DATASET);
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
    ApplicationManager applicationManager = deployApplication(AppWithWorker.class);
    LOG.info("Deployed.");
    WorkerManager manager = applicationManager.startWorker(AppWithWorker.WORKER);
    TimeUnit.MILLISECONDS.sleep(200);
    manager.stop();
    applicationManager.stopAll();
    DataSetManager<KeyValueTable> dataSetManager = applicationManager.getDataSet(AppWithWorker.DATASET);
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
    ServiceManager serviceManager = applicationManager.startService(AppWithServices.SERVICE_NAME);
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

    // we can verify metrics, by adding getServiceMetrics in RuntimeStats and then disabling the system scope test in
    // TestMetricsCollectionService

    LOG.info("DatasetUpdateService Started");
    Map<String, String> args
      = ImmutableMap.of(AppWithServices.WRITE_VALUE_RUN_KEY, AppWithServices.DATASET_TEST_VALUE,
                        AppWithServices.WRITE_VALUE_STOP_KEY, AppWithServices.DATASET_TEST_VALUE_STOP);
    ServiceManager datasetWorkerServiceManager = applicationManager
      .startService(AppWithServices.DATASET_WORKER_SERVICE_NAME, args);
    datasetWorkerServiceManager.waitForStatus(true);

    ProcedureManager procedureManager = applicationManager.startProcedure("NoOpProcedure");
    ProcedureClient procedureClient = procedureManager.getClient();

    String result = procedureClient.query("ping", ImmutableMap.of(AppWithServices.PROCEDURE_DATASET_KEY,
                                                                  AppWithServices.DATASET_TEST_KEY));
    String decodedResult = new Gson().fromJson(result, String.class);
    Assert.assertEquals(AppWithServices.DATASET_TEST_VALUE, decodedResult);

    // Test that a service can discover another service
    String path = String.format("discover/%s/%s",
                                AppWithServices.APP_NAME, AppWithServices.DATASET_WORKER_SERVICE_NAME);
    url = new URL(serviceURL, path);
    request = HttpRequest.get(url).build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());

    datasetWorkerServiceManager.stop();
    datasetWorkerServiceManager.waitForStatus(false);
    LOG.info("DatasetUpdateService Stopped");
    serviceManager.stop();
    serviceManager.waitForStatus(false);
    LOG.info("ServerService Stopped");

    result = procedureClient.query("ping", ImmutableMap.of(AppWithServices.PROCEDURE_DATASET_KEY,
                                                           AppWithServices.DATASET_TEST_KEY_STOP));
    decodedResult = new Gson().fromJson(result, String.class);
    Assert.assertEquals(AppWithServices.DATASET_TEST_VALUE_STOP, decodedResult);

    result = procedureClient.query("ping", ImmutableMap.of(AppWithServices.PROCEDURE_DATASET_KEY,
                                                           AppWithServices.DATASET_TEST_KEY_STOP_2));
    decodedResult = new Gson().fromJson(result, String.class);
    Assert.assertEquals(AppWithServices.DATASET_TEST_VALUE_STOP_2, decodedResult);
  }

  @Test
  public void testTransactionHandlerService() throws Exception {
    ApplicationManager applicationManager = deployApplication(AppWithServices.class);
    LOG.info("Deployed.");
    ServiceManager serviceManager = applicationManager.startService(AppWithServices.TRANSACTIONS_SERVICE_NAME);
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

    DataSetManager<KeyValueTable> dsManager
      = applicationManager.getDataSet(AppWithServices.TRANSACTIONS_DATASET_NAME);
    String value = Bytes.toString(dsManager.get().read(AppWithServices.DESTROY_KEY));
    Assert.assertEquals(AppWithServices.VALUE, value);
  }

  // todo: passing stream name as a workaround for not cleaning up streams during reset()
  private void testApp(Class<? extends Application> app, String streamName) throws Exception {

    ApplicationManager applicationManager = deployApplication(app);
    applicationManager.startFlow("WordCountFlow");

    // Send some inputs to streams
    StreamWriter streamWriter = applicationManager.getStreamWriter(streamName);
    for (int i = 0; i < 100; i++) {
      streamWriter.send(ImmutableMap.of("title", "title " + i), "testing message " + i);
    }

    // Check the flowlet metrics
    RuntimeMetrics flowletMetrics = RuntimeStats.getFlowletMetrics("WordCountApp",
                                                                   "WordCountFlow",
                                                                   "CountByField");
    flowletMetrics.waitForProcessed(500, 10, TimeUnit.SECONDS);
    Assert.assertEquals(0L, flowletMetrics.getException());

    // Query the result
    ProcedureManager procedureManager = applicationManager.startProcedure("WordFrequency");
    ProcedureClient procedureClient = procedureManager.getClient();

    // Verify the query result
    Type resultType = new TypeToken<Map<String, Long>>() { }.getType();
    Gson gson = new Gson();
    Map<String, Long> result = gson.fromJson(procedureClient.query("wordfreq",
                                                                   ImmutableMap.of("word", streamName + ":testing")),
                                             resultType);

    Assert.assertEquals(100L, result.get(streamName + ":testing").longValue());

    // check the metrics
    RuntimeMetrics procedureMetrics = RuntimeStats.getProcedureMetrics("WordCountApp", "WordFrequency");
    procedureMetrics.waitForProcessed(1, 5, TimeUnit.SECONDS);
    Assert.assertEquals(0L, procedureMetrics.getException());

    // Run mapreduce job
    MapReduceManager mrManager = applicationManager.startMapReduce("countTotal");
    mrManager.waitForFinish(1800L, TimeUnit.SECONDS);

    long totalCount = Long.valueOf(procedureClient.query("total", Collections.<String, String>emptyMap()));
    // every event has 5 tokens
    Assert.assertEquals(5 * 100L, totalCount);

    // Run mapreduce from stream
    mrManager = applicationManager.startMapReduce("countFromStream");
    mrManager.waitForFinish(120L, TimeUnit.SECONDS);

    totalCount = Long.valueOf(procedureClient.query("stream_total", Collections.<String, String>emptyMap()));

    // The stream MR only consume the body, not the header.
    Assert.assertEquals(3 * 100L, totalCount);

    DataSetManager<MyKeyValueTableDefinition.KeyValueTable> mydatasetManager =
      applicationManager.getDataSet("mydataset");
    Assert.assertEquals(100L, Long.valueOf(mydatasetManager.get().get("title:title")).longValue());
  }

  @Category(SlowTests.class)
  @Test
  public void testGenerator() throws InterruptedException, IOException, TimeoutException {
    ApplicationManager applicationManager = deployApplication(GenSinkApp2.class);
    applicationManager.startFlow("GenSinkFlow");

    // Check the flowlet metrics
    RuntimeMetrics genMetrics = RuntimeStats.getFlowletMetrics("GenSinkApp",
                                                               "GenSinkFlow",
                                                               "GenFlowlet");

    RuntimeMetrics sinkMetrics = RuntimeStats.getFlowletMetrics("GenSinkApp",
                                                                "GenSinkFlow",
                                                                "SinkFlowlet");

    RuntimeMetrics batchSinkMetrics = RuntimeStats.getFlowletMetrics("GenSinkApp",
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
    ApplicationManager appManager = deployApplication(AppWithTable.class);
    DataSetManager<Table> myTableManager = appManager.getDataSet("my_table");
    myTableManager.get().put(new Put("key1", "column1", "value1"));
    myTableManager.flush();

    // Changes should be visible to other instances of datasets
    DataSetManager<Table> myTableManager2 = appManager.getDataSet("my_table");
    Assert.assertEquals("value1", myTableManager2.get().get(new Get("key1", "column1")).getString("column1"));

    // Even after redeploy of an app: changes should be visible to other instances of datasets
    appManager = deployApplication(AppWithTable.class);
    DataSetManager<Table> myTableManager3 = appManager.getDataSet("my_table");
    Assert.assertEquals("value1", myTableManager3.get().get(new Get("key1", "column1")).getString("column1"));

    // Calling commit again (to test we can call it multiple times)
    myTableManager.get().put(new Put("key1", "column1", "value2"));
    myTableManager.flush();

    Assert.assertEquals("value1", myTableManager3.get().get(new Get("key1", "column1")).getString("column1"));
  }


  @Test(timeout = 60000L)
  public void testFlowletInitAndSetInstances() throws Exception {
    ApplicationManager appManager = deployApplication(DataSetInitApp.class);
    FlowManager flowManager = appManager.startFlow("DataSetFlow");

    RuntimeMetrics flowletMetrics = RuntimeStats.getFlowletMetrics("DataSetInitApp", "DataSetFlow", "Consumer");

    flowletMetrics.waitForProcessed(1, 5, TimeUnit.SECONDS);

    // Now change generator to 3 instances
    flowManager.setFlowletInstances("Generator", 3);

    // Now should have 3 processed from the consumer flowlet
    flowletMetrics.waitForProcessed(3, 10, TimeUnit.SECONDS);

    // Now reset to 1 instances
    flowManager.setFlowletInstances("Generator", 1);

    // Shouldn't have new item
    TimeUnit.SECONDS.sleep(3);
    Assert.assertEquals(3, flowletMetrics.getProcessed());

    // Now set to 2 instances again. Since there is a new instance, expect one new item emitted
    flowManager.setFlowletInstances("Generator", 2);
    flowletMetrics.waitForProcessed(4, 10, TimeUnit.SECONDS);

    flowManager.stop();

    DataSetManager<Table> dataSetManager = appManager.getDataSet("conf");
    Table confTable = dataSetManager.get();

    Assert.assertEquals("generator", confTable.get(new Get("key", "column")).getString("column"));

    dataSetManager.flush();
  }

  @Test(timeout = 60000L)
  public void testAppWithAutoDeployDatasetModule() throws Exception {
    testAppWithDataset(AppsWithDataset.AppWithAutoDeploy.class, "MyProcedure");
  }

  @Test(timeout = 60000L)
  public void testAppWithAutoDeployDataset() throws Exception {
    deployDatasetModule("my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    // we should be fine if module is already there. Deploy of module should not happen
    testAppWithDataset(AppsWithDataset.AppWithAutoDeploy.class, "MyProcedure");
  }

  @Test(timeout = 60000L)
  public void testAppWithAutoCreateDataset() throws Exception {
    deployDatasetModule("my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    testAppWithDataset(AppsWithDataset.AppWithAutoCreate.class, "MyProcedure");
  }

  @Test(timeout = 60000L)
  public void testAppWithExistingDataset() throws Exception {
    deployDatasetModule("my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    addDatasetInstance("myKeyValueTable", "myTable", DatasetProperties.EMPTY).create();
    testAppWithDataset(AppsWithDataset.AppWithExisting.class, "MyProcedure");
  }

  @Test(timeout = 60000L)
  public void testAppWithExistingDatasetInjectedByAnnotation() throws Exception {
    deployDatasetModule("my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    addDatasetInstance("myKeyValueTable", "myTable", DatasetProperties.EMPTY).create();
    testAppWithDataset(AppsWithDataset.AppUsesAnnotation.class, "MyProcedureWithUseDataSetAnnotation");
  }

  @Test(timeout = 60000L)
  public void testDatasetWithoutApp() throws Exception {
    // TODO: Although this has nothing to do with this testcase, deploying a dummy app to create the default namespace
    deployApplication(DummyApp.class);
    deployDatasetModule("my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    addDatasetInstance("myKeyValueTable", "myTable", DatasetProperties.EMPTY).create();
    DataSetManager<AppsWithDataset.KeyValueTableDefinition.KeyValueTable> dataSetManager = getDataset("myTable");
    AppsWithDataset.KeyValueTableDefinition.KeyValueTable kvTable = dataSetManager.get();
    kvTable.put("test", "hello");
    dataSetManager.flush();
    Assert.assertEquals("hello", dataSetManager.get().get("test"));
  }

  @Test(timeout = 60000L)
  public void testAppWithAutoDeployDatasetType() throws Exception {
    testAppWithDataset(AppsWithDataset.AppWithAutoDeployType.class, "MyProcedure");
  }

  @Test(timeout = 60000L)
  public void testAppWithAutoDeployDatasetTypeShortcut() throws Exception {
    testAppWithDataset(AppsWithDataset.AppWithAutoDeployTypeShortcut.class, "MyProcedure");
  }

  private void testAppWithDataset(Class<? extends Application> app, String procedureName) throws Exception {
    ApplicationManager applicationManager = deployApplication(app);
    // Query the result
    ProcedureManager procedureManager = applicationManager.startProcedure(procedureName);
    ProcedureClient procedureClient = procedureManager.getClient();

    procedureClient.query("set", ImmutableMap.of("key", "key1", "value", "value1"));

    String response = procedureClient.query("get", ImmutableMap.of("key", "key1"));
    Assert.assertEquals("value1", new Gson().fromJson(response, String.class));
  }

  @Test(timeout = 90000L)
  public void testSQLQuery() throws Exception {
    // Deploying app makes sure that the default namespace is available.
    deployApplication(DummyApp.class);
    deployDatasetModule("my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    ApplicationManager appManager = deployApplication(AppsWithDataset.AppWithAutoCreate.class);
    DataSetManager<AppsWithDataset.KeyValueTableDefinition.KeyValueTable> myTableManager =
      appManager.getDataSet("myTable");
    AppsWithDataset.KeyValueTableDefinition.KeyValueTable kvTable = myTableManager.get();
    kvTable.put("a", "1");
    kvTable.put("b", "2");
    kvTable.put("c", "1");
    myTableManager.flush();

    Connection connection = getQueryClient();
    try {

      // run a query over the dataset
      ResultSet results = connection.prepareStatement("select first from dataset_mytable where second = '1'")
        .executeQuery();
      Assert.assertTrue(results.next());
      Assert.assertEquals("a", results.getString(1));
      Assert.assertTrue(results.next());
      Assert.assertEquals("c", results.getString(1));
      Assert.assertFalse(results.next());

    } finally {
      connection.close();
    }
  }


  @Category(XSlowTests.class)
  @Test
  public void testByteCodeClassLoader() throws Exception {
    // This test verify bytecode generated classes ClassLoading

    ApplicationManager appManager = deployApplication(ClassLoaderTestApp.class);
    FlowManager flowManager = appManager.startFlow("BasicFlow");

    // Wait for at least 10 records being generated
    RuntimeMetrics flowMetrics = RuntimeStats.getFlowletMetrics("ClassLoaderTestApp", "BasicFlow", "Sink");
    flowMetrics.waitForProcessed(10, 5000, TimeUnit.MILLISECONDS);
    flowManager.stop();

    ServiceManager serviceManager = appManager.startService("RecordQuery");
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
    KeyValueTable records = appManager.<KeyValueTable>getDataSet("records").get();
    Assert.assertTrue(count == Bytes.toLong(records.read("PUBLIC")));
  }

  @Category(XSlowTests.class)
  @Test
  public void testDatasetUncheckedUpgrade() throws Exception {
    ApplicationManager applicationManager = deployApplication(DatasetUncheckedUpgradeApp.class);
    DataSetManager<DatasetUncheckedUpgradeApp.RecordDataset> datasetManager =
      applicationManager.getDataSet(DatasetUncheckedUpgradeApp.DATASET_NAME);
    DatasetUncheckedUpgradeApp.Record expectedRecord = new DatasetUncheckedUpgradeApp.Record("0AXB", "john", "doe");
    datasetManager.get().writeRecord("key", expectedRecord);
    datasetManager.flush();

    DatasetUncheckedUpgradeApp.Record actualRecord =
      (DatasetUncheckedUpgradeApp.Record) datasetManager.get().getRecord("key");
    Assert.assertEquals(expectedRecord, actualRecord);

    // Test compatible upgrade
    applicationManager = deployApplication(CompatibleDatasetUncheckedUpgradeApp.class);
    datasetManager = applicationManager.getDataSet(DatasetUncheckedUpgradeApp.DATASET_NAME);
    CompatibleDatasetUncheckedUpgradeApp.Record compatibleRecord =
      (CompatibleDatasetUncheckedUpgradeApp.Record) datasetManager.get().getRecord("key");
    Assert.assertEquals(new CompatibleDatasetUncheckedUpgradeApp.Record("0AXB", "john", false), compatibleRecord);

    // Test in-compatible upgrade
    applicationManager = deployApplication(IncompatibleDatasetUncheckedUpgradeApp.class);
    datasetManager = applicationManager.getDataSet(DatasetUncheckedUpgradeApp.DATASET_NAME);
    try {
      datasetManager.get().getRecord("key");
      Assert.fail("Expected to throw exception here due to an incompatible Dataset upgrade.");
    } catch (Exception e) {
      // Expected exception due to incompatible Dataset upgrade
    }

    // Revert the upgrade
    applicationManager = deployApplication(CompatibleDatasetUncheckedUpgradeApp.class);
    datasetManager = applicationManager.getDataSet(DatasetUncheckedUpgradeApp.DATASET_NAME);
    CompatibleDatasetUncheckedUpgradeApp.Record revertRecord =
      (CompatibleDatasetUncheckedUpgradeApp.Record) datasetManager.get().getRecord("key");
    Assert.assertEquals(new CompatibleDatasetUncheckedUpgradeApp.Record("0AXB", "john", false), revertRecord);
  }
}
