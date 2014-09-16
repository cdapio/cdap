/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.http.HttpRequest;
import co.cask.cdap.common.http.HttpRequests;
import co.cask.cdap.common.http.HttpResponse;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ProcedureClient;
import co.cask.cdap.test.ProcedureManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.WorkflowManager;
import co.cask.cdap.test.XSlowTests;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.Socket;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
@Category(SlowTests.class)
public class TestFrameworkTest extends TestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestFrameworkTest.class);

  @After
  public void cleanup() throws Exception {
    // Sleep a second before clear. There is a race between removal of RuntimeInfo
    // in the AbstractProgramRuntimeService class and the clear() method, which loops all RuntimeInfo.
    // The reason for the race is because removal is done through callback.
    TimeUnit.SECONDS.sleep(1);
    clear();
  }

  @Test
  public void testFlowRuntimeArguments() throws Exception {
    ApplicationManager applicationManager = deployApplication(FilterApp.class);
    try {
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
      //TODO : Can be removed after fixing JIRA - REACTOR-373
      TimeUnit.SECONDS.sleep(2);

      Assert.assertEquals("1",
                          gson.fromJson(client.query("result", ImmutableMap.of("type", "highpass")), String.class));
    } finally {
      applicationManager.stopAll();
      TimeUnit.SECONDS.sleep(1);
    }
  }

  @Category(XSlowTests.class)
  @Test
  public void testDeployWorkflowApp() throws InterruptedException {
    ApplicationManager applicationManager = deployApplication(AppWithSchedule.class);
    WorkflowManager wfmanager = applicationManager.startWorkflow("SampleWorkflow", null);
    List<String> schedules = wfmanager.getSchedules();
    Assert.assertEquals(1, schedules.size());
    String scheduleId = schedules.get(0);
    Assert.assertNotNull(scheduleId);
    Assert.assertFalse(scheduleId.isEmpty());

    List<RunRecord> history;
    int workflowRuns = 0;
    workFlowHistoryCheck(5, wfmanager, 0);

    String status = wfmanager.getSchedule(scheduleId).status();
    Assert.assertEquals("SCHEDULED", status);

    wfmanager.getSchedule(scheduleId).suspend();
    workFlowStatusCheck(5, scheduleId, wfmanager, "SUSPENDED");

    TimeUnit.SECONDS.sleep(3);
    history = wfmanager.getHistory();
    workflowRuns = history.size();

    //Sleep for some time and verify there are no more scheduled jobs after the suspend.
    TimeUnit.SECONDS.sleep(10);
    int workflowRunsAfterSuspend = wfmanager.getHistory().size();
    Assert.assertEquals(workflowRuns, workflowRunsAfterSuspend);

    wfmanager.getSchedule(scheduleId).resume();

    //Check that after resume it goes to "SCHEDULED" state
    workFlowStatusCheck(5, scheduleId, wfmanager, "SCHEDULED");

    workFlowHistoryCheck(5, wfmanager, workflowRunsAfterSuspend);

    //check scheduled state
    Assert.assertEquals("SCHEDULED", wfmanager.getSchedule(scheduleId).status());

    //check status of non-existent schedule
    Assert.assertEquals("NOT_FOUND", wfmanager.getSchedule("doesnt exist").status());

    //suspend the schedule
    wfmanager.getSchedule(scheduleId).suspend();

    //Check that after suspend it goes to "SUSPENDED" state
    workFlowStatusCheck(5, scheduleId, wfmanager, "SUSPENDED");

    TimeUnit.SECONDS.sleep(10);
    applicationManager.stopAll();
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
      status = wfmanager.getSchedule(scheduleId).status();
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
    try {
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

    } finally {
      applicationManager.stopAll();
    }
  }

  @Category(XSlowTests.class)
  @Test(timeout = 360000)
  public void testApp() throws InterruptedException, IOException, TimeoutException {
    testApp(WordCountApp.class, "text");
  }

  @Category(SlowTests.class)
  @Test
  public void testGetServiceURL() throws Exception {
    ApplicationManager applicationManager = deployApplication(AppUsingGetServiceURL.class);
    ServiceManager centralServiceManager = applicationManager.startService(AppUsingGetServiceURL.CENTRAL_SERVICE);
    serviceStatusCheck(centralServiceManager, true);

    // Test procedure's getServiceURL
    ProcedureManager procedureManager = applicationManager.startProcedure(AppUsingGetServiceURL.PROCEDURE);
    ProcedureClient procedureClient = procedureManager.getClient();
    String result = procedureClient.query("ping", Collections.<String, String>emptyMap());
    String decodedResult = new Gson().fromJson(result, String.class);
    // Verify that the procedure was able to hit the CentralService and retrieve the answer.
    Assert.assertEquals(AppUsingGetServiceURL.ANSWER, decodedResult);


    // Test serviceWorker's getServiceURL
    ServiceManager serviceWithWorker = applicationManager.startService(AppUsingGetServiceURL.SERVICE_WITH_WORKER);
    serviceStatusCheck(serviceWithWorker, true);
    // Since the worker is passive (we can not ping it), allow the service worker 2 seconds to ping the CentralService,
    // get the appropriate response, and write to to a dataset.
    Thread.sleep(2000);
    serviceWithWorker.stop();
    serviceStatusCheck(serviceWithWorker, false);

    result = procedureClient.query("readDataSet", ImmutableMap.of(AppUsingGetServiceURL.DATASET_WHICH_KEY,
                                                           AppUsingGetServiceURL.DATASET_KEY));
    decodedResult = new Gson().fromJson(result, String.class);
    Assert.assertEquals(AppUsingGetServiceURL.ANSWER, decodedResult);
    procedureManager.stop();

    centralServiceManager.stop();
    serviceStatusCheck(centralServiceManager, false);
  }

  /**
   * Checks to ensure that a particular runnable of the {@param serviceManager} has {@param expected} number of
   * instances. If the initial check fails, it performs {@param retries} more attempts, sleeping 1 second before each
   * successive attempt.
   */
  private void runnableInstancesCheck(ServiceManager serviceManager, String runnableName,
                                      int expected, int retries) throws InterruptedException {
    for (int i = 0; i <= retries; i++) {
      int actualInstances = serviceManager.getRunnableInstances(runnableName);
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
    try {
      ServiceManager serviceManager = applicationManager.startService(AppUsingGetServiceURL.SERVICE_WITH_WORKER);
      serviceStatusCheck(serviceManager, true);

      String runnableName = AppUsingGetServiceURL.SERVICE_WITH_WORKER;
      int retries = 5;

      // Should be 1 instance when first started.
      runnableInstancesCheck(serviceManager, runnableName, 1, retries);

      // Test increasing instances.
      serviceManager.setRunnableInstances(runnableName, 5);
      runnableInstancesCheck(serviceManager, runnableName, 5, retries);

      // Test decreasing instances.
      serviceManager.setRunnableInstances(runnableName, 2);
      runnableInstancesCheck(serviceManager, runnableName, 2, retries);

      // Test requesting same number of instances.
      serviceManager.setRunnableInstances(runnableName, 2);
      runnableInstancesCheck(serviceManager, runnableName, 2, retries);

      serviceManager.stop();
      serviceStatusCheck(serviceManager, false);

      // Should be 0 instances when stopped.
      runnableInstancesCheck(serviceManager, runnableName, 0, retries);

    } finally {
      applicationManager.stopAll();
    }
  }

  @Category(SlowTests.class)
  @Test
  public void testAppWithServices() throws Exception {
    ApplicationManager applicationManager = deployApplication(AppWithServices.class);
    LOG.info("Deployed.");
    ServiceManager serviceManager = applicationManager.startService(AppWithServices.SERVICE_NAME);
    serviceStatusCheck(serviceManager, true);

    LOG.info("Service Started");

    // Look for service endpoint
    final ServiceDiscovered serviceDiscovered = serviceManager.discover("AppWithServices",
                                                                        AppWithServices.SERVICE_NAME);
    final BlockingQueue<Discoverable> discoverables = new LinkedBlockingQueue<Discoverable>();
    serviceDiscovered.watchChanges(new ServiceDiscovered.ChangeListener() {
      @Override
      public void onChange(ServiceDiscovered serviceDiscovered) {
        Iterables.addAll(discoverables, serviceDiscovered);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    // There should be one endpoint only
    Discoverable discoverable = discoverables.poll(5, TimeUnit.SECONDS);
    Assert.assertNotNull(discoverable);
    Assert.assertTrue(discoverables.isEmpty());

    URL url = new URL(String.format("http://%s:%d/v2/apps/AppWithServices/services/%s/methods/ping2",
                                    discoverable.getSocketAddress().getHostName(),
                                    discoverable.getSocketAddress().getPort(), AppWithServices.SERVICE_NAME));
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(response.getResponseCode(), 200);

    // Connect and close. This should stop the leader instance.
    Socket socket = new Socket(discoverable.getSocketAddress().getAddress(), discoverable.getSocketAddress().getPort());
    socket.close();

    serviceManager.stop();
    serviceStatusCheck(serviceManager, false);

    LOG.info("Service Stopped");
    // we can verify metrics, by adding getServiceMetrics in RuntimeStats and then disabling the system scope test in
    // TestMetricsCollectionService

    LOG.info("DatasetUpdateService Started");
    serviceManager = applicationManager.startService(AppWithServices.DATASET_WORKER_SERVICE_NAME);
    serviceStatusCheck(serviceManager, true);

    ProcedureManager procedureManager = applicationManager.startProcedure("NoOpProcedure");
    ProcedureClient procedureClient = procedureManager.getClient();

    String result = procedureClient.query("ping", ImmutableMap.of(AppWithServices.PROCEDURE_DATASET_KEY,
                                                                  AppWithServices.DATASET_TEST_KEY));
    String decodedResult = new Gson().fromJson(result, String.class);
    Assert.assertEquals(AppWithServices.DATASET_TEST_VALUE, decodedResult);

    serviceManager.stop();
    serviceStatusCheck(serviceManager, false);

    result = procedureClient.query("ping", ImmutableMap.of(AppWithServices.PROCEDURE_DATASET_KEY,
                                                           AppWithServices.DATASET_TEST_KEY_STOP));
    decodedResult = new Gson().fromJson(result, String.class);
    Assert.assertEquals(AppWithServices.DATASET_TEST_VALUE_STOP, decodedResult);

    procedureManager.stop();
    LOG.info("DatasetUpdateService Stopped");
  }

  @Test
  public void testTransactionHandlerService() throws Exception {
    ApplicationManager applicationManager = deployApplication(AppWithServices.class);
    LOG.info("Deployed.");
    ServiceManager serviceManager = applicationManager.startService(AppWithServices.TRANSACTIONS_SERVICE_NAME);
    serviceStatusCheck(serviceManager, true);

    LOG.info("Service Started");

    // Look for service endpoint
    final ServiceDiscovered serviceDiscovered = serviceManager.discover("AppWithServices",
                                                                        AppWithServices.TRANSACTIONS_SERVICE_NAME);
    final BlockingQueue<Discoverable> discoverables = new LinkedBlockingQueue<Discoverable>();
    serviceDiscovered.watchChanges(new ServiceDiscovered.ChangeListener() {
      @Override
      public void onChange(ServiceDiscovered serviceDiscovered) {
        Iterables.addAll(discoverables, serviceDiscovered);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    final Discoverable discoverable = discoverables.poll(5, TimeUnit.SECONDS);
    Assert.assertNotNull(discoverable);
    Assert.assertTrue(discoverables.isEmpty());

    // Make a request to write in a separate thread and wait for it to return.
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future<Integer> requestFuture = executorService.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        try {
          URL url = new URL(String.format("http://%s:%d/v2/apps/AppWithServices/services/%s/methods/write/%s/%s/%d",
                                          discoverable.getSocketAddress().getHostName(),
                                          discoverable.getSocketAddress().getPort(),
                                          AppWithServices.TRANSACTIONS_SERVICE_NAME,
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
    URL url = new URL(String.format("http://%s:%d/v2/apps/AppWithServices/services/%s/methods/read/%s",
                                    discoverable.getSocketAddress().getHostName(),
                                    discoverable.getSocketAddress().getPort(),
                                    AppWithServices.TRANSACTIONS_SERVICE_NAME,
                                    AppWithServices.DATASET_TEST_KEY));
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
    serviceStatusCheck(serviceManager, false);
  }

  private void serviceStatusCheck(ServiceManager serviceManger, boolean running) throws InterruptedException {
    int trial = 0;
    while (trial++ < 5) {
      if (serviceManger.isRunning() == running) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    throw new IllegalStateException("Service state not executed. Expected " + running);
  }

  // todo: passing stream name as a workaround for not cleaning up streams during reset()
  private void testApp(Class<? extends Application> app, String streamName)
    throws IOException, TimeoutException, InterruptedException {

    ApplicationManager applicationManager = deployApplication(app);

    try {
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

    } finally {
      applicationManager.stopAll();
    }
  }

  @Category(SlowTests.class)
  @Test
  public void testGenerator() throws InterruptedException, IOException, TimeoutException {
    ApplicationManager applicationManager = deployApplication(GenSinkApp2.class);

    try {
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
      sinkMetrics.waitFor("process.events.in", 198, 5, TimeUnit.SECONDS);
      sinkMetrics.waitForProcessed(198, 5, TimeUnit.SECONDS);
      Assert.assertEquals(0L, sinkMetrics.getException());

      // Batch sink only get the 99 batch events
      batchSinkMetrics.waitFor("process.events.in", 99, 5, TimeUnit.SECONDS);
      batchSinkMetrics.waitForProcessed(99, 5, TimeUnit.SECONDS);
      Assert.assertEquals(0L, batchSinkMetrics.getException());

      Assert.assertEquals(1L, genMetrics.getException());

    } finally {
      applicationManager.stopAll();
    }
  }

  @Test
  public void testAppRedeployKeepsData() {
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


  @Test (timeout = 30000L)
  public void testInitDataSetAccess() throws TimeoutException, InterruptedException {
    ApplicationManager appManager = deployApplication(DataSetInitApp.class);
    FlowManager flowManager = appManager.startFlow("DataSetFlow");

    RuntimeMetrics flowletMetrics = RuntimeStats.getFlowletMetrics("DataSetInitApp", "DataSetFlow", "Consumer");

    flowletMetrics.waitForProcessed(1, 5, TimeUnit.SECONDS);

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

    try {
      // Query the result
      ProcedureManager procedureManager = applicationManager.startProcedure(procedureName);
      ProcedureClient procedureClient = procedureManager.getClient();

      procedureClient.query("set", ImmutableMap.of("key", "key1", "value", "value1"));

      String response = procedureClient.query("get", ImmutableMap.of("key", "key1"));
      Assert.assertEquals("value1", new Gson().fromJson(response, String.class));

    } finally {
      TimeUnit.SECONDS.sleep(2);
      applicationManager.stopAll();
    }
  }

  @Test(timeout = 60000L)
  public void testSQLQuery() throws Exception {

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
      // list the tables and make sure the table is there
      ResultSet results = connection.prepareStatement("show tables").executeQuery();
      Assert.assertTrue(results.next());
      Assert.assertTrue("cdap_user_mytable".equalsIgnoreCase(results.getString(1)));

      // run a query over the dataset
      results = connection.prepareStatement("select first from cdap_user_mytable where second = '1'")
          .executeQuery();
      Assert.assertTrue(results.next());
      Assert.assertEquals("a", results.getString(1));
      Assert.assertTrue(results.next());
      Assert.assertEquals("c", results.getString(1));
      Assert.assertFalse(results.next());

    } finally {
      connection.close();
      appManager.stopAll();
    }
  }

}
