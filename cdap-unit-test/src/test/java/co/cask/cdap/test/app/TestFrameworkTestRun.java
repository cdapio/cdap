/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.AppUsingNamespace;
import co.cask.cdap.ConfigTestApp;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.DefaultId;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.WorkflowTokenDetail;
import co.cask.cdap.proto.WorkflowTokenNodeDetail;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkerManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.artifacts.AppWithPlugin;
import co.cask.cdap.test.artifacts.plugins.ToStringPlugin;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 *
 */
@Category(SlowTests.class)
public class TestFrameworkTestRun extends TestFrameworkTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestFrameworkTestRun.class);

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private final Id.Namespace testSpace = Id.Namespace.from("testspace");

  @Before
  public void setUp() throws Exception {
    getNamespaceAdmin().create(new NamespaceMeta.Builder().setName(testSpace).build());
  }

  @Test
  public void testOrgHamcrest() {
    // this tests that org.hamcrest classes can be used in tests without class loading conflicts (see CDAP-7391)
    Assert.assertThat("oh hello yeah", CoreMatchers.containsString("hello"));
  }

  @Test
  public void testInvalidAppWithDuplicateDatasets() throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "invalid-app", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, AppWithDuplicateData.class);

    Id.Artifact pluginArtifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "test-plugin", "1.0.0-SNAPSHOT");
    addPluginArtifact(pluginArtifactId, artifactId, ToStringPlugin.class);

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "InvalidApp");

    for (int choice = 4; choice > 0; choice /= 2) {
      try {
        AppRequest<AppWithDuplicateData.ConfigClass> createRequest = new AppRequest<>(
          new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()),
          new AppWithDuplicateData.ConfigClass((choice == 4), (choice == 2), (choice == 1)));
        deployApplication(appId, createRequest);
        // fail if we succeed with application deployment
        Assert.fail();
      } catch (Exception e) {
        // expected
      }
    }

    AppRequest<AppWithDuplicateData.ConfigClass> createRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()),
      new AppWithDuplicateData.ConfigClass(false, false, false));
    deployApplication(appId, createRequest);
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
  public void testWorkerThrowingException() throws Exception {
    ApplicationManager appManager = deployApplication(AppWithExceptionThrowingWorker.class);
    final WorkerManager workerManager = appManager.getWorkerManager(AppWithExceptionThrowingWorker.WORKER_NAME);

    // Only one instance of the worker and it throws an exception. ProgramRunStatus should go to FAILED state.
    testExceptionWorker(workerManager);
    Tasks.waitFor(3, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return workerManager.getHistory(ProgramRunStatus.FAILED).size();
      }
    }, 5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);

    // Test a case where worker completes without an exception.
    workerManager.start();
    workerManager.waitForFinish(3, TimeUnit.SECONDS);
    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return workerManager.getHistory(ProgramRunStatus.COMPLETED).size();
      }
    }, 5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);

    // Few of the instances of the worker will throw an exception, while others complete normally. Still the
    // ProgramRunStatus should go to FAILED state.
    workerManager.setInstances(9);
    testExceptionWorker(workerManager);
    Tasks.waitFor(6, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return workerManager.getHistory(ProgramRunStatus.FAILED).size();
      }
    }, 5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);
    Tasks.waitFor(7, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return workerManager.getHistory().size();
      }
    }, 5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);

    // Test a case where worker completes without an exception.
    workerManager.start();
    workerManager.waitForFinish(3, TimeUnit.SECONDS);
    Tasks.waitFor(2, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return workerManager.getHistory(ProgramRunStatus.COMPLETED).size();
      }
    }, 5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);
  }

  private void testExceptionWorker(WorkerManager workerManager) throws Exception {
    workerManager.start(ImmutableMap.of(AppWithExceptionThrowingWorker.INITIALIZE, ""));
    workerManager.waitForFinish(3, TimeUnit.SECONDS);
    workerManager.start(ImmutableMap.of(AppWithExceptionThrowingWorker.RUN, ""));
    workerManager.waitForFinish(3, TimeUnit.SECONDS);
    workerManager.start(ImmutableMap.of(AppWithExceptionThrowingWorker.DESTROY, ""));
    workerManager.waitForFinish(3, TimeUnit.SECONDS);
  }

  @Test
  public void testServiceManager() throws Exception {
    ApplicationManager applicationManager = deployApplication(FilterApp.class);
    final ServiceManager countService = applicationManager.getServiceManager("CountService");
    countService.setInstances(2);
    Assert.assertEquals(0, countService.getProvisionedInstances());
    Assert.assertEquals(2, countService.getRequestedInstances());
    Assert.assertFalse(countService.isRunning());

    List<RunRecord> history = countService.getHistory();
    Assert.assertEquals(0, history.size());

    countService.start();
    Assert.assertTrue(countService.isRunning());
    Assert.assertEquals(2, countService.getProvisionedInstances());

    // requesting with ProgramRunStatus.KILLED returns empty list
    history = countService.getHistory(ProgramRunStatus.KILLED);
    Assert.assertEquals(0, history.size());

    // requesting with either RUNNING or ALL will return one record
    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return countService.getHistory(ProgramRunStatus.RUNNING).size();
      }
    }, 5, TimeUnit.SECONDS);

    history = countService.getHistory(ProgramRunStatus.RUNNING);
    Assert.assertEquals(ProgramRunStatus.RUNNING, history.get(0).getStatus());

    history = countService.getHistory(ProgramRunStatus.ALL);
    Assert.assertEquals(1, history.size());
    Assert.assertEquals(ProgramRunStatus.RUNNING, history.get(0).getStatus());
  }

  @Test
  public void testNamespaceAvailableAtRuntime() throws Exception {
    ApplicationManager applicationManager = deployApplication(testSpace, AppUsingNamespace.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(AppUsingNamespace.SERVICE_NAME);
    serviceManager.start();
    serviceManager.waitForStatus(true, 1, 10);

    URL serviceURL = serviceManager.getServiceURL(10, TimeUnit.SECONDS);
    Assert.assertEquals(testSpace.getId(), callServiceGet(serviceURL, "ns"));

    serviceManager.stop();
    serviceManager.waitForStatus(false, 1, 10);
  }

  @Test
  public void testAppConfigWithNull() throws Exception {
    testAppConfig(ConfigTestApp.NAME, deployApplication(ConfigTestApp.class), null);
  }

  @Test
  public void testAppConfig() throws Exception {
    ConfigTestApp.ConfigClass conf = new ConfigTestApp.ConfigClass("testStream", "testDataset");
    testAppConfig(ConfigTestApp.NAME, deployApplication(ConfigTestApp.class, conf), conf);
  }

  @Test
  public void testAppWithPlugin() throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "app-with-plugin", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, AppWithPlugin.class);

    Id.Artifact pluginArtifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "test-plugin", "1.0.0-SNAPSHOT");
    addPluginArtifact(pluginArtifactId, artifactId, ToStringPlugin.class);

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "AppWithPlugin");
    AppRequest createRequest = new AppRequest(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()));

    ApplicationManager appManager = deployApplication(appId, createRequest);

    final WorkerManager workerManager = appManager.getWorkerManager(AppWithPlugin.WORKER);
    workerManager.start();
    workerManager.waitForStatus(false, 5, 1);
    Tasks.waitFor(false, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return workerManager.getHistory(ProgramRunStatus.COMPLETED).isEmpty();
      }
    }, 5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);

    final ServiceManager serviceManager = appManager.getServiceManager(AppWithPlugin.SERVICE);
    serviceManager.start();
    serviceManager.waitForStatus(true, 1, 10);
    URL serviceURL = serviceManager.getServiceURL(5, TimeUnit.SECONDS);
    callServiceGet(serviceURL, "dummy");
    serviceManager.stop();
    serviceManager.waitForStatus(false, 1, 10);
    Tasks.waitFor(false, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return serviceManager.getHistory(ProgramRunStatus.KILLED).isEmpty();
      }
    }, 5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);

    WorkflowManager workflowManager = appManager.getWorkflowManager(AppWithPlugin.WORKFLOW);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);
    List<RunRecord> runRecords = workflowManager.getHistory();
    Assert.assertNotEquals(ProgramRunStatus.FAILED, runRecords.get(0).getStatus());
    DataSetManager<KeyValueTable> workflowTableManager = getDataset(AppWithPlugin.WORKFLOW_TABLE);
    String value = Bytes.toString(workflowTableManager.get().read("val"));
    Assert.assertEquals(AppWithPlugin.TEST, value);

    // Testing Spark Plugins. First send some data to stream for the Spark program to process
    StreamManager streamManager = getStreamManager(AppWithPlugin.SPARK_STREAM);
    for (int i = 0; i < 5; i++) {
      streamManager.send("Message " + i);
    }

    SparkManager sparkManager = appManager.getSparkManager(AppWithPlugin.SPARK).start();
    sparkManager.waitForFinish(2, TimeUnit.MINUTES);

    // Verify the Spark result.
    DataSetManager<Table> dataSetManager = getDataset(AppWithPlugin.SPARK_TABLE);
    Table table = dataSetManager.get();
    try (Scanner scanner = table.scan(null, null)) {
      for (int i = 0; i < 5; i++) {
        Row row = scanner.next();
        Assert.assertNotNull(row);
        String expected = "Message " + i + " " + AppWithPlugin.TEST;
        Assert.assertEquals(expected, Bytes.toString(row.getRow()));
        Assert.assertEquals(expected, Bytes.toString(row.get(expected)));
      }
      // There shouldn't be any more rows in the table.
      Assert.assertNull(scanner.next());
    }
  }

  @Test
  public void testAppFromArtifact() throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "cfg-app", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "AppFromArtifact");
    AppRequest<ConfigTestApp.ConfigClass> createRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()),
      new ConfigTestApp.ConfigClass("testStream", "testDataset")
    );
    ApplicationManager appManager = deployApplication(appId, createRequest);
    testAppConfig(appId.getId(), appManager, createRequest.getConfig());
  }

  @Test
  public void testAppVersionsCreation() throws Exception {
    ArtifactId artifactId = new ArtifactId(NamespaceId.DEFAULT.getNamespace(), "cfg-app", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);

    ApplicationId appId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), "AppV1", "version1");
    AppRequest<ConfigTestApp.ConfigClass> createRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
      new ConfigTestApp.ConfigClass("tS1", "tD1", "tV1"));
    ApplicationManager appManager = deployApplication(appId, createRequest);
    ServiceManager serviceManager = appManager.getServiceManager(ConfigTestApp.SERVICE_NAME);
    serviceManager.start();

    URL serviceURL = serviceManager.getServiceURL();
    Gson gson = new Gson();
    Assert.assertEquals("tV1", gson.fromJson(callServiceGet(serviceURL, "ping"), String.class));
    serviceManager.stop();

    appId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), "AppV1", "version2");
    createRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
      new ConfigTestApp.ConfigClass("tS2", "tD2", "tV2"));
    appManager = deployApplication(appId, createRequest);
    serviceManager = appManager.getServiceManager(ConfigTestApp.SERVICE_NAME);
    serviceManager.start();

    serviceURL = serviceManager.getServiceURL();
    Assert.assertEquals("tV2", gson.fromJson(callServiceGet(serviceURL, "ping"), String.class));
    serviceManager.stop();
  }

  private void testAppConfig(String appName, ApplicationManager appManager,
                             ConfigTestApp.ConfigClass conf) throws Exception {
    String streamName = conf == null ? ConfigTestApp.DEFAULT_STREAM : conf.getStreamName();
    String datasetName = conf == null ? ConfigTestApp.DEFAULT_TABLE : conf.getTableName();

    FlowManager flowManager = appManager.getFlowManager(ConfigTestApp.FLOW_NAME).start();
    StreamManager streamManager = getStreamManager(streamName);
    streamManager.send("abcd");
    streamManager.send("xyz");
    RuntimeMetrics metrics = flowManager.getFlowletMetrics(ConfigTestApp.FLOWLET_NAME);
    metrics.waitForProcessed(2, 1, TimeUnit.MINUTES);
    flowManager.stop();
    DataSetManager<KeyValueTable> dsManager = getDataset(datasetName);
    KeyValueTable table = dsManager.get();
    Assert.assertEquals("abcd", Bytes.toString(table.read(appName + ".abcd")));
    Assert.assertEquals("xyz", Bytes.toString(table.read(appName + ".xyz")));
  }

  @Category(SlowTests.class)
  @Test
  public void testMapperDatasetAccess() throws Exception {
    addDatasetInstance("keyValueTable", "table1");
    addDatasetInstance("keyValueTable", "table2");
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
    verifyMapperJobOutput(DatasetWithMRApp.class, outTableManager);
  }

  @Category(SlowTests.class)
  @Test
  public void testCrossNSMapperDatasetAccess() throws Exception {
    NamespaceMeta inputNS = new NamespaceMeta.Builder().setName("inputNS").build();
    NamespaceMeta outputNS = new NamespaceMeta.Builder().setName("outputNS").build();
    getNamespaceAdmin().create(inputNS);
    getNamespaceAdmin().create(outputNS);

    addDatasetInstance(inputNS.getNamespaceId().toId(), "keyValueTable", "table1");
    addDatasetInstance(outputNS.getNamespaceId().toId(), "keyValueTable", "table2");
    DataSetManager<KeyValueTable> tableManager = getDataset(inputNS.getNamespaceId().toId(), "table1");
    KeyValueTable inputTable = tableManager.get();
    inputTable.write("hello", "world");
    tableManager.flush();

    ApplicationManager appManager = deployApplication(DatasetCrossNSAccessWithMAPApp.class);
    Map<String, String> argsForMR = ImmutableMap.of(
      DatasetCrossNSAccessWithMAPApp.INPUT_DATASET_NS, inputNS.getName(),
      DatasetCrossNSAccessWithMAPApp.INPUT_DATASET_NAME, "table1",
      DatasetCrossNSAccessWithMAPApp.OUTPUT_DATASET_NS, outputNS.getName(),
      DatasetCrossNSAccessWithMAPApp.OUTPUT_DATASET_NAME, "table2");
    MapReduceManager mrManager = appManager.getMapReduceManager(DatasetCrossNSAccessWithMAPApp.MAPREDUCE_PROGRAM)
      .start(argsForMR);
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    appManager.stopAll();

    DataSetManager<KeyValueTable> outTableManager = getDataset(outputNS.getNamespaceId().toId(), "table2");
    verifyMapperJobOutput(DatasetCrossNSAccessWithMAPApp.class, outTableManager);
  }

  private void verifyMapperJobOutput(Class<?> appClass,
                                     DataSetManager<KeyValueTable> outTableManager) throws Exception {
    KeyValueTable outputTable = outTableManager.get();
    Assert.assertEquals("world", Bytes.toString(outputTable.read("hello")));

    // Verify dataset metrics
    String readCountName = "system." + Constants.Metrics.Name.Dataset.READ_COUNT;
    String writeCountName = "system." + Constants.Metrics.Name.Dataset.WRITE_COUNT;
    Collection<MetricTimeSeries> metrics = getMetricsManager().query(
      new MetricDataQuery(
        0, System.currentTimeMillis() / 1000, Integer.MAX_VALUE,
        ImmutableMap.of(
          readCountName, AggregationFunction.SUM,
          writeCountName, AggregationFunction.SUM
        ),
        ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, DefaultId.NAMESPACE.getNamespace(),
                        Constants.Metrics.Tag.APP, appClass.getSimpleName(),
                        Constants.Metrics.Tag.MAPREDUCE, DatasetWithMRApp.MAPREDUCE_PROGRAM),
        ImmutableList.<String>of()
      )
    );

    // Transform the collection of metrics into a map from metrics name to aggregated sum
    Map<String, Long> aggs = Maps.transformEntries(Maps.uniqueIndex(metrics, new Function<MetricTimeSeries, String>() {
      @Override
      public String apply(MetricTimeSeries input) {
        return input.getMetricName();
      }
    }), new Maps.EntryTransformer<String, MetricTimeSeries, Long>() {
      @Override
      public Long transformEntry(String key, MetricTimeSeries value) {
        Preconditions.checkArgument(value.getTimeValues().size() == 1,
                                    "Expected one value for aggregated sum for metrics %s", key);
        return value.getTimeValues().get(0).getValue();
      }
    });

    Assert.assertEquals(Long.valueOf(1), aggs.get(readCountName));
    Assert.assertEquals(Long.valueOf(1), aggs.get(writeCountName));
  }

  @Test
  public void testWorkflowStatus() throws Exception {
    ApplicationManager appManager = deployApplication(WorkflowStatusTestApp.class);

    File workflowSuccess = new File(TMP_FOLDER.newFolder() + "/workflow.success");
    File actionSuccess = new File(TMP_FOLDER.newFolder() + "/action.success");
    File workflowKilled = new File(TMP_FOLDER.newFolder() + "/workflow.killed");
    File firstFile = new File(TMP_FOLDER.newFolder() + "/first");
    File firstFileDone = new File(TMP_FOLDER.newFolder() + "/first.done");

    WorkflowManager workflowManager = appManager.getWorkflowManager(WorkflowStatusTestApp.WORKFLOW_NAME);
    workflowManager.start(ImmutableMap.of("workflow.success.file", workflowSuccess.getAbsolutePath(),
                                          "action.success.file", actionSuccess.getAbsolutePath(),
                                          "throw.exception", "true"));
    workflowManager.waitForFinish(1, TimeUnit.MINUTES);

    // Since action and workflow failed the files should not exist
    Assert.assertFalse(workflowSuccess.exists());
    Assert.assertFalse(actionSuccess.exists());

    workflowManager.start(ImmutableMap.of("workflow.success.file", workflowSuccess.getAbsolutePath(),
                                          "action.success.file", actionSuccess.getAbsolutePath()));
    workflowManager.waitForFinish(1, TimeUnit.MINUTES);
    Assert.assertTrue(workflowSuccess.exists());
    Assert.assertTrue(actionSuccess.exists());

    // Test the killed status
    workflowManager.start(ImmutableMap.of("workflow.killed.file", workflowKilled.getAbsolutePath(),
                                          "first.file", firstFile.getAbsolutePath(),
                                          "first.done.file", firstFileDone.getAbsolutePath(),
                                          "test.killed", "true"));
    verifyFileExists(Lists.newArrayList(firstFile));
    workflowManager.stop();
    workflowManager.waitForStatus(false);
    Assert.assertTrue(workflowKilled.exists());
  }

  @Category(SlowTests.class)
  @Test
  public void testCustomActionDatasetAccess() throws Exception {
    addDatasetInstance("keyValueTable", DatasetWithCustomActionApp.CUSTOM_TABLE);
    addDatasetInstance("fileSet", DatasetWithCustomActionApp.CUSTOM_FILESET);

    ApplicationManager appManager = deployApplication(DatasetWithCustomActionApp.class);
    ServiceManager serviceManager = appManager.getServiceManager(DatasetWithCustomActionApp.CUSTOM_SERVICE).start();
    serviceManager.waitForStatus(true);

    WorkflowManager workflowManager = appManager.getWorkflowManager(DatasetWithCustomActionApp.CUSTOM_WORKFLOW).start();
    workflowManager.waitForFinish(2, TimeUnit.MINUTES);
    appManager.stopAll();

    DataSetManager<KeyValueTable> outTableManager = getDataset(DatasetWithCustomActionApp.CUSTOM_TABLE);
    KeyValueTable outputTable = outTableManager.get();

    Assert.assertEquals("world", Bytes.toString(outputTable.read("hello")));
    Assert.assertEquals("service", Bytes.toString(outputTable.read("hi")));
    Assert.assertEquals("another.world", Bytes.toString(outputTable.read("another.hello")));

    DataSetManager<FileSet> outFileSetManager = getDataset(DatasetWithCustomActionApp.CUSTOM_FILESET);
    FileSet fs = outFileSetManager.get();
    try (InputStream in = fs.getLocation("test").getInputStream()) {
      Assert.assertEquals(42, in.read());
    }
  }

  @Category(XSlowTests.class)
  @Test
  public void testWorkflowLocalDatasets() throws Exception {
    ApplicationManager applicationManager = deployApplication(testSpace, WorkflowAppWithLocalDatasets.class);

    // Execute Workflow without keeping the local datasets after run
    Map<String, String> additionalParams = new HashMap<>();
    String runId = executeWorkflow(applicationManager, additionalParams);
    verifyWorkflowRun(runId, false, false, "COMPLETED");

    additionalParams.put("dataset.wordcount.keep.local", "true");
    runId = executeWorkflow(applicationManager, additionalParams);
    verifyWorkflowRun(runId, true, false, "COMPLETED");

    additionalParams.clear();
    additionalParams.put("dataset.*.keep.local", "true");
    runId = executeWorkflow(applicationManager, additionalParams);
    verifyWorkflowRun(runId, true, true, "COMPLETED");

    additionalParams.clear();
    additionalParams.put("dataset.*.keep.local", "true");
    additionalParams.put("destroy.throw.exception", "true");
    runId = executeWorkflow(applicationManager, additionalParams);
    verifyWorkflowRun(runId, true, true, "STARTED");

    WorkflowManager wfManager = applicationManager.getWorkflowManager(WorkflowAppWithLocalDatasets.WORKFLOW_NAME);
    List<RunRecord> history = wfManager.getHistory();
    Assert.assertEquals(4, history.size());
    for (RunRecord record : history) {
      Assert.assertEquals(ProgramRunStatus.COMPLETED, record.getStatus());
    }
  }

  private void verifyFileExists(final List<File> fileList)
    throws Exception {
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        for (File file : fileList) {
          if (!file.exists()) {
            return false;
          }
        }
        return true;
      }
    }, 100, TimeUnit.SECONDS);
  }

  private void verifyWorkflowRun(String runId, boolean shouldKeepWordCountDataset, boolean shouldKeepCSVFilesetDataset,
                                 String expectedRunStatus)
    throws Exception {

    // Once the Workflow run is complete local datasets should not be available
    DataSetManager<KeyValueTable> localKeyValueDataset = getDataset(testSpace,
                                                                    WorkflowAppWithLocalDatasets.WORDCOUNT_DATASET
                                                                      + "." + runId);

    if (shouldKeepWordCountDataset) {
      Assert.assertNotNull(localKeyValueDataset.get());
    } else {
      Assert.assertNull(localKeyValueDataset.get());
    }

    DataSetManager<FileSet> localFileSetDataset = getDataset(testSpace, WorkflowAppWithLocalDatasets.CSV_FILESET_DATASET
      + "." + runId);

    if (shouldKeepCSVFilesetDataset) {
      Assert.assertNotNull(localFileSetDataset.get());
    } else {
      Assert.assertNull(localFileSetDataset.get());
    }

    // Dataset which is not local should still be available
    DataSetManager<KeyValueTable> nonLocalKeyValueDataset = getDataset(testSpace,
                                                                       WorkflowAppWithLocalDatasets.RESULT_DATASET);
    Assert.assertEquals("6", Bytes.toString(nonLocalKeyValueDataset.get().read("UniqueWordCount")));

    // There should not be any local copy of the non local dataset
    nonLocalKeyValueDataset = getDataset(testSpace, WorkflowAppWithLocalDatasets.RESULT_DATASET + "." + runId);
    Assert.assertNull(nonLocalKeyValueDataset.get());

    DataSetManager<KeyValueTable> workflowRuns
      = getDataset(testSpace, WorkflowAppWithLocalDatasets.WORKFLOW_RUNS_DATASET);

    Assert.assertEquals(expectedRunStatus, Bytes.toString(workflowRuns.get().read(runId)));
  }

  private String executeWorkflow(ApplicationManager applicationManager, Map<String, String> additionalParams)
    throws Exception {
    WorkflowManager wfManager = applicationManager.getWorkflowManager(WorkflowAppWithLocalDatasets.WORKFLOW_NAME);
    Map<String, String> runtimeArgs = new HashMap<>();
    File waitFile = new File(TMP_FOLDER.newFolder(), "/wait.file");
    File doneFile = new File(TMP_FOLDER.newFolder(), "/done.file");

    runtimeArgs.put("input.path", "input");
    runtimeArgs.put("output.path", "output");
    runtimeArgs.put("wait.file", waitFile.getAbsolutePath());
    runtimeArgs.put("done.file", doneFile.getAbsolutePath());
    runtimeArgs.putAll(additionalParams);
    wfManager.start(runtimeArgs);

    // Wait till custom action in the Workflow is triggered.
    while (!waitFile.exists()) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // Now the Workflow should have RUNNING status. Get its runid.
    List<RunRecord> history = wfManager.getHistory(ProgramRunStatus.RUNNING);
    Assert.assertEquals(1, history.size());
    String runId = history.get(0).getPid();

    // Get the local datasets for this Workflow run
    DataSetManager<KeyValueTable> localDataset = getDataset(testSpace, WorkflowAppWithLocalDatasets.WORDCOUNT_DATASET
      + "." + runId);
    Assert.assertEquals("2", Bytes.toString(localDataset.get().read("text")));

    DataSetManager<FileSet> fileSetDataset = getDataset(testSpace, WorkflowAppWithLocalDatasets.CSV_FILESET_DATASET
      + "." + runId);
    Assert.assertNotNull(fileSetDataset.get());

    // Local datasets should not exist at the namespace level
    localDataset = getDataset(testSpace, WorkflowAppWithLocalDatasets.WORDCOUNT_DATASET);
    Assert.assertNull(localDataset.get());

    fileSetDataset = getDataset(testSpace, WorkflowAppWithLocalDatasets.CSV_FILESET_DATASET);
    Assert.assertNull(fileSetDataset.get());

    // Verify that the workflow hasn't completed on its own before we signal it to
    history = wfManager.getHistory(ProgramRunStatus.RUNNING);
    Assert.assertEquals(1, history.size());

    // Signal the Workflow to continue
    doneFile.createNewFile();

    // Wait for workflow to finish
    wfManager.waitForFinish(1, TimeUnit.MINUTES);
    Map<String, WorkflowNodeStateDetail> nodeStateDetailMap = wfManager.getWorkflowNodeStates(runId);
    Map<String, String> workflowMetricsContext = new HashMap<>();
    workflowMetricsContext.put(Constants.Metrics.Tag.NAMESPACE, testSpace.getId());
    workflowMetricsContext.put(Constants.Metrics.Tag.APP, applicationManager.getInfo().getName());
    workflowMetricsContext.put(Constants.Metrics.Tag.WORKFLOW, WorkflowAppWithLocalDatasets.WORKFLOW_NAME);
    workflowMetricsContext.put(Constants.Metrics.Tag.RUN_ID, runId);

    Map<String, String> writerContext = new HashMap<>(workflowMetricsContext);
    writerContext.put(Constants.Metrics.Tag.NODE,
                      WorkflowAppWithLocalDatasets.LocalDatasetWriter.class.getSimpleName());

    Assert.assertEquals(2, getMetricsManager().getTotalMetric(writerContext, "user.num.lines"));

    Map<String, String> wfSparkMetricsContext = new HashMap<>(workflowMetricsContext);
    wfSparkMetricsContext.put(Constants.Metrics.Tag.NODE, "JavaSparkCSVToSpaceConverter");
    Assert.assertEquals(2, getMetricsManager().getTotalMetric(wfSparkMetricsContext, "user.num.lines"));

    // check in spark context
    Map<String, String> sparkMetricsContext = new HashMap<>();
    sparkMetricsContext.put(Constants.Metrics.Tag.NAMESPACE, testSpace.getId());
    sparkMetricsContext.put(Constants.Metrics.Tag.APP, applicationManager.getInfo().getName());
    sparkMetricsContext.put(Constants.Metrics.Tag.SPARK, "JavaSparkCSVToSpaceConverter");
    sparkMetricsContext.put(Constants.Metrics.Tag.RUN_ID,
                            nodeStateDetailMap.get("JavaSparkCSVToSpaceConverter").getRunId());
    Assert.assertEquals(2, getMetricsManager().getTotalMetric(sparkMetricsContext, "user.num.lines"));

    Map<String, String> appMetricsContext = new HashMap<>();
    appMetricsContext.put(Constants.Metrics.Tag.NAMESPACE, testSpace.getId());
    appMetricsContext.put(Constants.Metrics.Tag.APP, applicationManager.getInfo().getName());
    // app metrics context should have sum from custom action and spark metrics.
    Assert.assertEquals(4, getMetricsManager().getTotalMetric(appMetricsContext, "user.num.lines"));

    Map<String, String> wfMRMetricsContext = new HashMap<>(workflowMetricsContext);
    wfMRMetricsContext.put(Constants.Metrics.Tag.NODE, "WordCount");
    Assert.assertEquals(7, getMetricsManager().getTotalMetric(wfMRMetricsContext, "user.num.words"));

    // mr metrics context
    Map<String, String> mrMetricsContext = new HashMap<>();
    mrMetricsContext.put(Constants.Metrics.Tag.NAMESPACE, testSpace.getId());
    mrMetricsContext.put(Constants.Metrics.Tag.APP, applicationManager.getInfo().getName());
    mrMetricsContext.put(Constants.Metrics.Tag.MAPREDUCE, "WordCount");
    mrMetricsContext.put(Constants.Metrics.Tag.RUN_ID,
                            nodeStateDetailMap.get("WordCount").getRunId());
    Assert.assertEquals(7, getMetricsManager().getTotalMetric(mrMetricsContext, "user.num.words"));

    Map<String, String> readerContext = new HashMap<>(workflowMetricsContext);
    readerContext.put(Constants.Metrics.Tag.NODE, "readerAction");
    Assert.assertEquals(6, getMetricsManager().getTotalMetric(readerContext, "user.unique.words"));
    return runId;
  }

  @Category(XSlowTests.class)
  @Test
  public void testDeployWorkflowApp() throws Exception {
    ApplicationManager applicationManager = deployApplication(testSpace, AppWithSchedule.class);
    final WorkflowManager wfmanager = applicationManager.getWorkflowManager("SampleWorkflow");
    List<ScheduleSpecification> schedules = wfmanager.getSchedules();
    Assert.assertEquals(1, schedules.size());
    String scheduleName = schedules.get(0).getSchedule().getName();
    Assert.assertNotNull(scheduleName);
    Assert.assertFalse(scheduleName.isEmpty());
    wfmanager.getSchedule(scheduleName).resume();

    String status = wfmanager.getSchedule(scheduleName).status(200);
    Assert.assertEquals("SCHEDULED", status);

    // Make sure something ran before suspending
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return !wfmanager.getHistory().isEmpty();
      }
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    wfmanager.getSchedule(scheduleName).suspend();
    waitForScheduleState(scheduleName, wfmanager, Scheduler.ScheduleState.SUSPENDED);

    // All runs should be completed
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        for (RunRecord record : wfmanager.getHistory()) {
          if (record.getStatus() != ProgramRunStatus.COMPLETED) {
            return false;
          }
        }
        return true;
      }
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    List<RunRecord> history = wfmanager.getHistory();
    int workflowRuns = history.size();
    Assert.assertTrue(workflowRuns > 0);

    //Sleep for some time and verify there are no more scheduled jobs after the suspend.
    TimeUnit.SECONDS.sleep(5);
    final int workflowRunsAfterSuspend = wfmanager.getHistory().size();
    Assert.assertEquals(workflowRuns, workflowRunsAfterSuspend);

    wfmanager.getSchedule(scheduleName).resume();

    //Check that after resume it goes to "SCHEDULED" state
    waitForScheduleState(scheduleName, wfmanager, Scheduler.ScheduleState.SCHEDULED);

    // Make sure new runs happens after resume
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return wfmanager.getHistory().size() > workflowRunsAfterSuspend;
      }
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    //check scheduled state
    Assert.assertEquals("SCHEDULED", wfmanager.getSchedule(scheduleName).status(200));

    //check status of non-existent schedule
    Assert.assertEquals("NOT_FOUND", wfmanager.getSchedule("doesnt exist").status(404));

    //suspend the schedule
    wfmanager.getSchedule(scheduleName).suspend();

    //Check that after suspend it goes to "SUSPENDED" state
    waitForScheduleState(scheduleName, wfmanager, Scheduler.ScheduleState.SUSPENDED);

    // test workflow token while suspended
    String pid = history.get(0).getPid();
    WorkflowTokenDetail workflowToken = wfmanager.getToken(pid, WorkflowToken.Scope.SYSTEM, null);
    Assert.assertEquals(0, workflowToken.getTokenData().size());
    workflowToken = wfmanager.getToken(pid, null, null);
    Assert.assertEquals(2, workflowToken.getTokenData().size());

    // wait till workflow finishes execution
    waitForWorkflowStatus(wfmanager, ProgramRunStatus.COMPLETED);

    // verify workflow token after workflow completion
    WorkflowTokenNodeDetail workflowTokenAtNode =
      wfmanager.getTokenAtNode(pid, AppWithSchedule.DummyAction.class.getSimpleName(),
                               WorkflowToken.Scope.USER, "finished");
    Assert.assertEquals(true, Boolean.parseBoolean(workflowTokenAtNode.getTokenDataAtNode().get("finished")));
    workflowToken = wfmanager.getToken(pid, null, null);
    Assert.assertEquals(false, Boolean.parseBoolean(workflowToken.getTokenData().get("running").get(0).getValue()));
  }

  @Test
  public void testBatchStreamUpload() throws Exception {
    StreamManager batchStream = getStreamManager("batchStream");
    batchStream.createStream();
    String event1 = "this,is,some";
    String event2 = "test,csv,data";
    String event3 = "that,can,be,used,to,test";
    String event4 = "batch,upload,capability";
    String event5 = "for,streams in testbase";
    File testData = TEMP_FOLDER.newFile("test-stream-data.txt");
    try (FileWriter fileWriter = new FileWriter(testData);
         BufferedWriter out = new BufferedWriter(fileWriter)) {
      out.write(String.format("%s\n", event1));
      out.write(String.format("%s\n", event2));
      out.write(String.format("%s\n", event3));
      out.write(String.format("%s\n", event4));
      out.write(String.format("%s\n", event5));
    }

    // batch upload file containing 10 events
    batchStream.send(testData, "text/csv");
    // verify upload
    List<StreamEvent> uploadedEvents = batchStream.getEvents(0, System.currentTimeMillis(), 100);
    Assert.assertEquals(5, uploadedEvents.size());
    Assert.assertEquals(event1, Bytes.toString(uploadedEvents.get(0).getBody()));
    Assert.assertEquals(event2, Bytes.toString(uploadedEvents.get(1).getBody()));
    Assert.assertEquals(event3, Bytes.toString(uploadedEvents.get(2).getBody()));
    Assert.assertEquals(event4, Bytes.toString(uploadedEvents.get(3).getBody()));
    Assert.assertEquals(event5, Bytes.toString(uploadedEvents.get(4).getBody()));
  }

  private void waitForWorkflowStatus(final WorkflowManager wfmanager, ProgramRunStatus expected) throws Exception {
    Tasks.waitFor(expected, new Callable<ProgramRunStatus>() {
      @Override
      public ProgramRunStatus call() throws Exception {
        List<RunRecord> history = wfmanager.getHistory();
        RunRecord runRecord = history.get(history.size() - 1);
        return runRecord.getStatus();
      }
    }, 5, TimeUnit.SECONDS, 30, TimeUnit.MILLISECONDS);
  }

  private void waitForWorkflowRuns(final WorkflowManager wfmanager, int expected) throws Exception {
    Tasks.waitFor(expected, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return wfmanager.getHistory().size();
      }
    }, 5, TimeUnit.SECONDS, 30, TimeUnit.MILLISECONDS);
  }

  private void waitForScheduleState(final String scheduleId, final WorkflowManager wfmanager,
                                    Scheduler.ScheduleState expected) throws Exception {
    Tasks.waitFor(expected, new Callable<Scheduler.ScheduleState>() {
      @Override
      public Scheduler.ScheduleState call() throws Exception {
        return Scheduler.ScheduleState.valueOf(wfmanager.getSchedule(scheduleId).status(200));
      }
    }, 5, TimeUnit.SECONDS, 30, TimeUnit.MILLISECONDS);
  }


  @Category(XSlowTests.class)
  @Test(timeout = 240000)
  public void testMultiInput() throws Exception {
    ApplicationManager applicationManager = deployApplication(JoinMultiStreamApp.class);
    FlowManager flowManager = applicationManager.getFlowManager("JoinMultiFlow").start();

    StreamManager s1 = getStreamManager("s1");
    StreamManager s2 = getStreamManager("s2");
    StreamManager s3 = getStreamManager("s3");

    s1.send("testing 1");
    s2.send("testing 2");
    s3.send("testing 3");

    RuntimeMetrics terminalMetrics = flowManager.getFlowletMetrics("Terminal");
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

    // Program manager is not notified when worker stops on its own, hence suppressing the exception.
    // JIRA - CDAP-3656
    try {
      pingingWorker.stop();
    } catch (Throwable e) {
      LOG.error("Got exception while stopping pinging worker", e);
    }
    pingingWorker.waitForStatus(false);

    centralServiceManager.stop();
    centralServiceManager.waitForStatus(false);
  }

  /**
   * Checks to ensure that a particular  {@param workerManager} has {@param expected} number of
   * instances while retrying every 50 ms for 15 seconds.
   *
   * @throws Exception if the worker does not have the specified number of instances after 15 seconds.
   */
  private void workerInstancesCheck(final WorkerManager workerManager, int expected) throws Exception {
    Tasks.waitFor(expected, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return workerManager.getInstances();
      }
    }, 15, TimeUnit.SECONDS);
  }

  /**
   * Checks to ensure that a particular key is present in a {@link KeyValueTable}
   * @param namespace {@link Id.Namespace}
   * @param datasetName name of the dataset
   * @param expectedKey expected key byte array
   *
   * @throws Exception if the key is not found even after 15 seconds of timeout
   */
  private void kvTableKeyCheck(final Id.Namespace namespace, final String datasetName, final byte[] expectedKey)
    throws Exception {
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        DataSetManager<KeyValueTable> datasetManager = getDataset(namespace, datasetName);
        KeyValueTable kvTable = datasetManager.get();
        return kvTable.read(expectedKey) != null;
      }
    }, 15, TimeUnit.SECONDS);
  }

  @Category(SlowTests.class)
  @Test
  public void testWorkerInstances() throws Exception {
    ApplicationManager applicationManager = deployApplication(testSpace, AppUsingGetServiceURL.class);
    WorkerManager workerManager = applicationManager.getWorkerManager(AppUsingGetServiceURL.PINGING_WORKER).start();
    workerManager.waitForStatus(true);

    // Should be 5 instances when first started.
    workerInstancesCheck(workerManager, 5);

    // Test increasing instances.
    workerManager.setInstances(10);
    workerInstancesCheck(workerManager, 10);

    // Test decreasing instances.
    workerManager.setInstances(2);
    workerInstancesCheck(workerManager, 2);

    // Test requesting same number of instances.
    workerManager.setInstances(2);
    workerInstancesCheck(workerManager, 2);

    WorkerManager lifecycleWorkerManager = applicationManager.getWorkerManager(AppUsingGetServiceURL.LIFECYCLE_WORKER);
    lifecycleWorkerManager.setInstances(3);
    lifecycleWorkerManager.start().waitForStatus(true);

    workerInstancesCheck(lifecycleWorkerManager, 3);
    for (int i = 0; i < 3; i++) {
      kvTableKeyCheck(testSpace, AppUsingGetServiceURL.WORKER_INSTANCES_DATASET,
                      Bytes.toBytes(String.format("init.%d", i)));
    }

    // Set 5 instances for the LifecycleWorker
    lifecycleWorkerManager.setInstances(5);
    workerInstancesCheck(lifecycleWorkerManager, 5);

    // Make sure all the keys have been written before stopping the worker
    for (int i = 0; i < 5; i++) {
      kvTableKeyCheck(testSpace, AppUsingGetServiceURL.WORKER_INSTANCES_DATASET,
                      Bytes.toBytes(String.format("init.%d", i)));
    }

    lifecycleWorkerManager.stop();
    lifecycleWorkerManager.waitForStatus(false);

    if (workerManager.isRunning()) {
      workerManager.stop();
    }
    workerManager.waitForStatus(false);

    // Should be same instances after being stopped.
    workerInstancesCheck(lifecycleWorkerManager, 5);
    workerInstancesCheck(workerManager, 2);

    // Assert the LifecycleWorker dataset writes
    // 3 workers should have started with 3 total instances. 2 more should later start with 5 total instances.
    assertWorkerDatasetWrites(Bytes.toBytes("init"),
                              Bytes.stopKeyForPrefix(Bytes.toBytes("init.2")), 3, 3);
    assertWorkerDatasetWrites(Bytes.toBytes("init.3"),
                              Bytes.stopKeyForPrefix(Bytes.toBytes("init")), 2, 5);

    // Test that the worker had 5 instances when stopped, and each knew that there were 5 instances
    byte[] startRow = Bytes.toBytes("stop");
    assertWorkerDatasetWrites(startRow, Bytes.stopKeyForPrefix(startRow), 5, 5);
  }

  private void assertWorkerDatasetWrites(byte[] startRow, byte[] endRow,
                                         int expectedCount, int expectedTotalCount) throws Exception {
    DataSetManager<KeyValueTable> datasetManager =
      getDataset(testSpace, AppUsingGetServiceURL.WORKER_INSTANCES_DATASET);
    KeyValueTable instancesTable = datasetManager.get();
    try (CloseableIterator<KeyValue<byte[], byte[]>> instancesIterator = instancesTable.scan(startRow, endRow)) {
      List<KeyValue<byte[], byte[]>> workerInstances = Lists.newArrayList(instancesIterator);
      // Assert that the worker starts with expectedCount instances
      Assert.assertEquals(expectedCount, workerInstances.size());
      // Assert that each instance of the worker knows the total number of instances
      for (KeyValue<byte[], byte[]> keyValue : workerInstances) {
        Assert.assertEquals(expectedTotalCount, Bytes.toInt(keyValue.getValue()));
      }
    }
  }

  // AppFabricClient returns IllegalStateException if the app fails to deploy
  @Category(SlowTests.class)
  @Test(expected = IllegalStateException.class)
  public void testServiceWithInvalidHandler() throws Exception {
    deployApplication(AppWithInvalidHandler.class);
  }

  @Category(SlowTests.class)
  @Test
  public void testAppWithWorker() throws Exception {
    ApplicationManager applicationManager = deployApplication(testSpace, AppWithWorker.class);
    LOG.info("Deployed.");
    WorkerManager manager = applicationManager.getWorkerManager(AppWithWorker.WORKER).start();

    // Wait for initialize and run states
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        DataSetManager<KeyValueTable> dataSetManager = getDataset(testSpace, AppWithWorker.DATASET);
        KeyValueTable table = dataSetManager.get();
        return AppWithWorker.INITIALIZE.equals(Bytes.toString(table.read(AppWithWorker.INITIALIZE))) &&
          AppWithWorker.RUN.equals(Bytes.toString(table.read(AppWithWorker.RUN)));
      }
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    manager.stop();
    applicationManager.stopAll();

    // Wait for stop state
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        DataSetManager<KeyValueTable> dataSetManager = getDataset(testSpace, AppWithWorker.DATASET);
        KeyValueTable table = dataSetManager.get();
        return AppWithWorker.STOP.equals(Bytes.toString(table.read(AppWithWorker.STOP)));
      }
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  @Category(SlowTests.class)
  @Test
  public void testAppWithTxTimeout() throws Exception {
    int txDefaulTimeoutService = 17;
    int txDefaulTimeoutWorker = 18;
    int txDefaulTimeoutWorkflow = 19;
    int txDefaulTimeoutAction = 20;
    int txDefaulTimeoutFlow = 21;
    int txDefaulTimeoutFlowlet = 22;
    int txDefaulTimeoutMapReduce = 23;
    int txDefaulTimeoutSpark = 24;

    ApplicationManager appManager = deployApplication(testSpace, AppWithCustomTx.class);
    try {
      // attempt to start with a tx timeout that exceeds the max tx timeout
      try {
        appManager.getServiceManager(AppWithCustomTx.SERVICE)
          .start(txTimeoutArguments(100000));
      } catch (IllegalArgumentException e) {
        //expected
      }

      // now start all the programs with valid tx timeouts
      getStreamManager(testSpace, AppWithCustomTx.INPUT).send("hello");
      ServiceManager serviceManager = appManager.getServiceManager(AppWithCustomTx.SERVICE)
        .start(txTimeoutArguments(txDefaulTimeoutService));
      WorkerManager notxWorkerManager = appManager.getWorkerManager(AppWithCustomTx.WORKER_NOTX)
        .start(txTimeoutArguments(txDefaulTimeoutWorker));
      WorkerManager txWorkerManager = appManager.getWorkerManager(AppWithCustomTx.WORKER_TX)
        .start(txTimeoutArguments(txDefaulTimeoutWorker));
      WorkflowManager txWFManager = appManager.getWorkflowManager(AppWithCustomTx.WORKFLOW_TX)
        .start(txTimeoutArguments(txDefaulTimeoutWorkflow));
      WorkflowManager notxWFManager = appManager.getWorkflowManager(AppWithCustomTx.WORKFLOW_NOTX)
        .start(txTimeoutArguments(txDefaulTimeoutWorkflow,
                                  txDefaulTimeoutAction, "action", AppWithCustomTx.ACTION_NOTX));
      FlowManager flowManager = appManager.getFlowManager(AppWithCustomTx.FLOW)
        .start(txTimeoutArguments(txDefaulTimeoutFlow,
                                  txDefaulTimeoutFlowlet, "flowlet", AppWithCustomTx.FLOWLET_NOTX));
      MapReduceManager txMRManager = appManager.getMapReduceManager(AppWithCustomTx.MAPREDUCE_TX)
        .start(txTimeoutArguments(txDefaulTimeoutMapReduce));
      MapReduceManager notxMRManager = appManager.getMapReduceManager(AppWithCustomTx.MAPREDUCE_NOTX)
        .start(txTimeoutArguments(txDefaulTimeoutMapReduce));
      SparkManager txSparkManager = appManager.getSparkManager(AppWithCustomTx.SPARK_TX)
        .start(txTimeoutArguments(txDefaulTimeoutSpark));
      SparkManager notxSparkManager = appManager.getSparkManager(AppWithCustomTx.SPARK_NOTX)
        .start(txTimeoutArguments(txDefaulTimeoutSpark));

      flowManager.getFlowletMetrics(AppWithCustomTx.FLOWLET_NOTX).waitForProcessed(1, 10, TimeUnit.SECONDS);
      flowManager.stop();
      flowManager.waitForStatus(false);

      serviceManager.waitForStatus(true);
      callServicePut(serviceManager.getServiceURL(), "tx", "hello");
      callServicePut(serviceManager.getServiceURL(), "tx", AppWithCustomTx.FAIL_PRODUCER, 200);
      callServicePut(serviceManager.getServiceURL(), "tx", AppWithCustomTx.FAIL_CONSUMER, 500);
      callServicePut(serviceManager.getServiceURL(), "notx", "hello");
      callServicePut(serviceManager.getServiceURL(), "notx", AppWithCustomTx.FAIL_PRODUCER, 200);
      callServicePut(serviceManager.getServiceURL(), "notx", AppWithCustomTx.FAIL_CONSUMER, 500);
      serviceManager.stop();
      serviceManager.waitForStatus(false);

      txMRManager.waitForFinish(10L, TimeUnit.SECONDS);
      notxMRManager.waitForFinish(10L, TimeUnit.SECONDS);
      txSparkManager.waitForFinish(10L, TimeUnit.SECONDS);
      notxSparkManager.waitForFinish(10L, TimeUnit.SECONDS);
      notxWorkerManager.waitForFinish(10L, TimeUnit.SECONDS);
      txWorkerManager.waitForFinish(10L, TimeUnit.SECONDS);
      txWFManager.waitForFinish(10L, TimeUnit.SECONDS);
      notxWFManager.waitForFinish(10L, TimeUnit.SECONDS);

      DataSetManager<TransactionCapturingTable> dataset = getDataset(testSpace, AppWithCustomTx.CAPTURE);
      Table t = dataset.get().getTable();

      // all programs attempt to write to the table in different transactional contexts
      // - if it is outside a transaction, then the expected value is null
      // - if it is in an attempt of a nested transaction, then the expected value is null
      // - if it is in an implicit transaction, then the expected value is "default"
      // - if it is in an explicit transaction, then the expected value is the transaction timeout
      Object[][] writesToValidate = new Object[][] {

        // transactions attempted by the workers
        { AppWithCustomTx.WORKER_TX, AppWithCustomTx.INITIALIZE, txDefaulTimeoutWorker },
        { AppWithCustomTx.WORKER_TX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.WORKER_TX, AppWithCustomTx.DESTROY, txDefaulTimeoutWorker },
        { AppWithCustomTx.WORKER_TX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.WORKER_TX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.WORKER_TX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutWorker},
        { AppWithCustomTx.WORKER_TX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_WORKER_RUNTIME },
        { AppWithCustomTx.WORKER_TX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.INITIALIZE, null },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.INITIALIZE_TX, AppWithCustomTx.TIMEOUT_WORKER_INITIALIZE },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.INITIALIZE_TX_D, txDefaulTimeoutWorker },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.DESTROY, null },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_WORKER_DESTROY },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.DESTROY_TX_D, txDefaulTimeoutWorker },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_WORKER_RUNTIME },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutWorker },
        { AppWithCustomTx.WORKER_NOTX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },

        // transactions attempted by the service
        { AppWithCustomTx.HANDLER_TX, AppWithCustomTx.INITIALIZE, txDefaulTimeoutService },
        { AppWithCustomTx.HANDLER_TX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.HANDLER_TX, AppWithCustomTx.DESTROY, txDefaulTimeoutService },
        { AppWithCustomTx.HANDLER_TX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.HANDLER_TX, AppWithCustomTx.RUNTIME, txDefaulTimeoutService },
        { AppWithCustomTx.HANDLER_TX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_CONSUMER_RUNTIME },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.RUNTIME_TX_T, AppWithCustomTx.TIMEOUT_CONSUMER_RUNTIME },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.RUNTIME_NEST_T, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.RUNTIME_NEST_CT, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.RUNTIME_NEST_TC, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.DESTROY, txDefaulTimeoutService },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.ONERROR, txDefaulTimeoutService },
        { AppWithCustomTx.CONSUMER_TX, AppWithCustomTx.ONERROR_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_PRODUCER_RUNTIME },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.RUNTIME_TX_T, AppWithCustomTx.TIMEOUT_PRODUCER_RUNTIME },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.RUNTIME_NEST_T, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.RUNTIME_NEST_CT, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.RUNTIME_NEST_TC, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.DESTROY, txDefaulTimeoutService },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.ONERROR, txDefaulTimeoutService },
        { AppWithCustomTx.PRODUCER_TX, AppWithCustomTx.ONERROR_NEST, AppWithCustomTx.FAILED },

        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.INITIALIZE, null },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.INITIALIZE_TX, AppWithCustomTx.TIMEOUT_HANDLER_INITIALIZE },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.INITIALIZE_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.DESTROY, null },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_HANDLER_DESTROY },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.DESTROY_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_HANDLER_RUNTIME },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.HANDLER_NOTX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_CONSUMER_RUNTIME },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.RUNTIME_TX_T, AppWithCustomTx.TIMEOUT_CONSUMER_RUNTIME },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.RUNTIME_NEST_T, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.RUNTIME_NEST_CT, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.RUNTIME_NEST_TC, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.DESTROY, null },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_CONSUMER_DESTROY },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.DESTROY_TX_D, txDefaulTimeoutService},
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.ONERROR, null },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.ONERROR_TX, AppWithCustomTx.TIMEOUT_CONSUMER_ERROR },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.ONERROR_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.CONSUMER_NOTX, AppWithCustomTx.ONERROR_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_PRODUCER_RUNTIME },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.RUNTIME_TX_T, AppWithCustomTx.TIMEOUT_PRODUCER_RUNTIME },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.RUNTIME_NEST_T, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.RUNTIME_NEST_CT, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.RUNTIME_NEST_TC, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.DESTROY, null },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_PRODUCER_DESTROY },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.DESTROY_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.ONERROR, null },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.ONERROR_TX, AppWithCustomTx.TIMEOUT_PRODUCER_ERROR },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.ONERROR_TX_D, txDefaulTimeoutService },
        { AppWithCustomTx.PRODUCER_NOTX, AppWithCustomTx.ONERROR_NEST, AppWithCustomTx.FAILED },

        // transactions attempted by the workflows
        { AppWithCustomTx.WORKFLOW_TX, AppWithCustomTx.INITIALIZE, txDefaulTimeoutWorkflow },
        { AppWithCustomTx.WORKFLOW_TX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.WORKFLOW_TX, AppWithCustomTx.DESTROY, txDefaulTimeoutWorkflow },
        { AppWithCustomTx.WORKFLOW_TX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.ACTION_TX, AppWithCustomTx.INITIALIZE, txDefaulTimeoutWorkflow },
        { AppWithCustomTx.ACTION_TX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.ACTION_TX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.ACTION_TX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_ACTION_RUNTIME },
        { AppWithCustomTx.ACTION_TX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutWorkflow },
        { AppWithCustomTx.ACTION_TX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.ACTION_TX, AppWithCustomTx.DESTROY, txDefaulTimeoutWorkflow },
        { AppWithCustomTx.ACTION_TX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },

        { AppWithCustomTx.WORKFLOW_NOTX, AppWithCustomTx.INITIALIZE, null },
        { AppWithCustomTx.WORKFLOW_NOTX, AppWithCustomTx.INITIALIZE_TX, AppWithCustomTx.TIMEOUT_WORKFLOW_INITIALIZE },
        { AppWithCustomTx.WORKFLOW_NOTX, AppWithCustomTx.INITIALIZE_TX_D, txDefaulTimeoutWorkflow },
        { AppWithCustomTx.WORKFLOW_NOTX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.WORKFLOW_NOTX, AppWithCustomTx.DESTROY, null },
        { AppWithCustomTx.WORKFLOW_NOTX, AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_WORKFLOW_DESTROY },
        { AppWithCustomTx.WORKFLOW_NOTX, AppWithCustomTx.DESTROY_TX_D, txDefaulTimeoutWorkflow },
        { AppWithCustomTx.WORKFLOW_NOTX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.INITIALIZE, null },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.INITIALIZE_TX, AppWithCustomTx.TIMEOUT_ACTION_INITIALIZE },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.INITIALIZE_TX_D, txDefaulTimeoutAction },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.RUNTIME, null },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.RUNTIME_TX, AppWithCustomTx.TIMEOUT_ACTION_RUNTIME },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.RUNTIME_TX_D, txDefaulTimeoutAction },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.DESTROY, null },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_ACTION_DESTROY },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.DESTROY_TX_D, txDefaulTimeoutAction },
        { AppWithCustomTx.ACTION_NOTX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },

        // transactions attempted by the flow
        { AppWithCustomTx.FLOWLET_TX, AppWithCustomTx.INITIALIZE, txDefaulTimeoutFlow },
        { AppWithCustomTx.FLOWLET_TX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.FLOWLET_TX, AppWithCustomTx.RUNTIME, txDefaulTimeoutFlow },
        { AppWithCustomTx.FLOWLET_TX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.FLOWLET_TX, AppWithCustomTx.DESTROY, txDefaulTimeoutFlow },
        { AppWithCustomTx.FLOWLET_TX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.FLOWLET_NOTX, AppWithCustomTx.INITIALIZE, null },
        { AppWithCustomTx.FLOWLET_NOTX, AppWithCustomTx.INITIALIZE_TX, AppWithCustomTx.TIMEOUT_FLOWLET_INITIALIZE },
        { AppWithCustomTx.FLOWLET_NOTX, AppWithCustomTx.INITIALIZE_TX_D, txDefaulTimeoutFlowlet },
        { AppWithCustomTx.FLOWLET_NOTX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.FLOWLET_NOTX, AppWithCustomTx.RUNTIME, txDefaulTimeoutFlowlet },
        { AppWithCustomTx.FLOWLET_NOTX, AppWithCustomTx.RUNTIME_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.FLOWLET_NOTX, AppWithCustomTx.DESTROY, null },
        { AppWithCustomTx.FLOWLET_NOTX, AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_FLOWLET_DESTROY },
        { AppWithCustomTx.FLOWLET_NOTX, AppWithCustomTx.DESTROY_TX_D, txDefaulTimeoutFlowlet },
        { AppWithCustomTx.FLOWLET_NOTX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },

        // transactions attempted by the mapreduce's
        { AppWithCustomTx.MAPREDUCE_TX, AppWithCustomTx.INITIALIZE, txDefaulTimeoutMapReduce },
        { AppWithCustomTx.MAPREDUCE_TX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.MAPREDUCE_TX, AppWithCustomTx.DESTROY, txDefaulTimeoutMapReduce },
        { AppWithCustomTx.MAPREDUCE_TX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.MAPREDUCE_NOTX, AppWithCustomTx.INITIALIZE, null },
        { AppWithCustomTx.MAPREDUCE_NOTX, AppWithCustomTx.INITIALIZE_TX, AppWithCustomTx.TIMEOUT_MAPREDUCE_INITIALIZE },
        { AppWithCustomTx.MAPREDUCE_NOTX, AppWithCustomTx.INITIALIZE_TX_D, txDefaulTimeoutMapReduce },
        { AppWithCustomTx.MAPREDUCE_NOTX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.MAPREDUCE_NOTX, AppWithCustomTx.DESTROY, null },
        { AppWithCustomTx.MAPREDUCE_NOTX, AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_MAPREDUCE_DESTROY },
        { AppWithCustomTx.MAPREDUCE_NOTX, AppWithCustomTx.DESTROY_TX_D, txDefaulTimeoutMapReduce },
        { AppWithCustomTx.MAPREDUCE_NOTX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },

        // transactions attempted by the spark's
        { AppWithCustomTx.SPARK_TX, AppWithCustomTx.INITIALIZE, txDefaulTimeoutSpark },
        { AppWithCustomTx.SPARK_TX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.SPARK_TX, AppWithCustomTx.DESTROY, txDefaulTimeoutSpark },
        { AppWithCustomTx.SPARK_TX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.INITIALIZE, null },
        { AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.INITIALIZE_TX, AppWithCustomTx.TIMEOUT_SPARK_INITIALIZE },
        { AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.INITIALIZE_TX_D, txDefaulTimeoutSpark },
        { AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.INITIALIZE_NEST, AppWithCustomTx.FAILED },
        { AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.DESTROY, null },
        { AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.DESTROY_TX, AppWithCustomTx.TIMEOUT_SPARK_DESTROY },
        { AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.DESTROY_TX_D, txDefaulTimeoutSpark },
        { AppWithCustomTx.SPARK_NOTX, AppWithCustomTx.DESTROY_NEST, AppWithCustomTx.FAILED },
      };

      for (Object[] writeToValidate : writesToValidate) {
        String row = (String) writeToValidate[0];
        String column = (String) writeToValidate[1];
        String expectedValue = writeToValidate[2] == null ? null : String.valueOf(writeToValidate[2]);
        Assert.assertEquals("Error for " + row + "." + column,
                            expectedValue, t.get(new Get(row, column)).getString(column));
      }

    } finally {
      appManager.stopAll();
    }
  }

  private static Map<String, String> txTimeoutArguments(int timeout) {
    return ImmutableMap.of(SystemArguments.TRANSACTION_TIMEOUT, String.valueOf(timeout));
  }
  private static Map<String, String> txTimeoutArguments(int timeout, int timeoutForScope, String scope, String name) {
    return ImmutableMap.of(SystemArguments.TRANSACTION_TIMEOUT, String.valueOf(timeout),
                           String.format("%s.%s.%s", scope, name, SystemArguments.TRANSACTION_TIMEOUT),
                           String.valueOf(timeoutForScope));
  }

  @Test
  public void testWorkerStop() throws Exception {
    // Test to make sure the worker program's status goes to stopped after the run method finishes
    ApplicationManager manager = deployApplication(NoOpWorkerApp.class);
    WorkerManager workerManager = manager.getWorkerManager("NoOpWorker");
    workerManager.start();
    workerManager.waitForStatus(false, 30, 1);
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
    Assert.assertTrue(response.getResponseBodyAsString().contains("Exception"));

    // Call the verify ClassLoader endpoint
    url = new URL(serviceURL, "verifyClassLoader");
    request = HttpRequest.get(url).build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());

    RuntimeMetrics serviceMetrics = serviceManager.getMetrics();
    serviceMetrics.waitForinput(3, 5, TimeUnit.SECONDS);
    Assert.assertEquals(3, serviceMetrics.getInput());
    Assert.assertEquals(2, serviceMetrics.getProcessed());
    Assert.assertEquals(1, serviceMetrics.getException());

    // in the AppWithServices the handlerName is same as the serviceName - "ServerService" handler
    RuntimeMetrics handlerMetrics = getMetricsManager().getServiceHandlerMetrics(Id.Namespace.DEFAULT.getId(),
                                                                                 AppWithServices.APP_NAME,
                                                                                 AppWithServices.SERVICE_NAME,
                                                                                 AppWithServices.SERVICE_NAME);
    handlerMetrics.waitForinput(3, 5, TimeUnit.SECONDS);
    Assert.assertEquals(3, handlerMetrics.getInput());
    Assert.assertEquals(2, handlerMetrics.getProcessed());
    Assert.assertEquals(1, handlerMetrics.getException());

    // we can verify metrics, by adding getServiceMetrics in MetricsManager and then disabling the system scope test in
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

    handlerMetrics = getMetricsManager().getServiceHandlerMetrics(Id.Namespace.DEFAULT.getId(),
                                                                  AppWithServices.APP_NAME,
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
    FlowManager flowManager = applicationManager.getFlowManager("WordCountFlow").start();

    // Send some inputs to streams
    StreamManager streamManager = getStreamManager(streamName);
    for (int i = 0; i < 100; i++) {
      streamManager.send(ImmutableMap.of("title", "title " + i), "testing message " + i);
    }

    // Check the flowlet metrics
    RuntimeMetrics flowletMetrics = flowManager.getFlowletMetrics("CountByField");

    flowletMetrics.waitForProcessed(500, 10, TimeUnit.SECONDS);
    Assert.assertEquals(0L, flowletMetrics.getException());

    // Query the result
    ServiceManager serviceManager = applicationManager.getServiceManager("WordFrequency").start();
    serviceManager.waitForStatus(true, 2, 1);

    // Verify the query result
    Type resultType = new TypeToken<Map<String, Long>>() { }.getType();
    Map<String, Long> result = new Gson().fromJson(
      callServiceGet(serviceManager.getServiceURL(), "wordfreq/" + streamName + ":testing"), resultType);
    Assert.assertNotNull(result);
    Assert.assertEquals(100L, result.get(streamName + ":testing").longValue());

    // check the metrics
    RuntimeMetrics serviceMetrics = serviceManager.getMetrics();
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

    // also test the deprecated version of getDataset(). This can be removed when we remove the method
    mydatasetManager = getDataset("mydataset");
    Assert.assertEquals(100L, Long.valueOf(mydatasetManager.get().get("title:title")).longValue());
  }

  @Category(SlowTests.class)
  @Test
  public void testGenerator() throws InterruptedException, IOException, TimeoutException {
    ApplicationManager applicationManager = deployApplication(testSpace, GenSinkApp2.class);
    FlowManager flowManager = applicationManager.getFlowManager("GenSinkFlow").start();

    // Check the flowlet metrics
    RuntimeMetrics genMetrics = flowManager.getFlowletMetrics("GenFlowlet");

    RuntimeMetrics sinkMetrics = flowManager.getFlowletMetrics("SinkFlowlet");

    RuntimeMetrics batchSinkMetrics = flowManager.getFlowletMetrics("BatchSinkFlowlet");

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

  @Category(SlowTests.class)
  @Test
  public void testDynamicBatchSize() throws Exception {
    ApplicationManager applicationManager = deployApplication(testSpace, GenSinkApp2.class);

    DataSetManager<KeyValueTable> table = getDataset(testSpace, "table");

    // Start the flow with runtime argument. It should set the batch size to 1.
    FlowManager flowManager = applicationManager.getFlowManager("GenSinkFlow").start(Collections.singletonMap(
      "flowlet.BatchSinkFlowlet.batch.size", "1"
    ));

    RuntimeMetrics batchSinkMetrics = flowManager.getFlowletMetrics("BatchSinkFlowlet");

    // Batch sink only get the 99 batch events
    batchSinkMetrics.waitForProcessed(99, 5, TimeUnit.SECONDS);
    flowManager.stop();
    flowManager.waitForFinish(10, TimeUnit.SECONDS);

    try (CloseableIterator<KeyValue<byte[], byte[]>> itor = table.get().scan(null, null)) {
      // Should only see batch size of 1.
      while (itor.hasNext()) {
        Assert.assertEquals(1, Bytes.toInt(itor.next().getKey()));
      }
    }
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
    RuntimeMetrics flowletMetrics = flowManager.getFlowletMetrics("Consumer");
    flowletMetrics.waitForProcessed(1, 5, TimeUnit.SECONDS);
    flowManager.stop();
    Assert.assertEquals(1, flowletMetrics.getProcessed());
    getMetricsManager().resetAll();
    // check the metrics were deleted after reset
    Assert.assertEquals(0, flowletMetrics.getProcessed());
  }

  @Test(timeout = 60000L)
  public void testFlowletInitAndSetInstances() throws Exception {
    ApplicationManager appManager = deployApplication(testSpace, DataSetInitApp.class);
    FlowManager flowManager = appManager.getFlowManager("DataSetFlow").start();

    RuntimeMetrics flowletMetrics = flowManager.getFlowletMetrics("Consumer");

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
    addDatasetInstance("myKeyValueTable", "myTable", DatasetProperties.EMPTY);
    testAppWithDataset(AppsWithDataset.AppWithExisting.class, "MyService");
  }

  @Test(timeout = 60000L)
  public void testAppWithExistingDatasetInjectedByAnnotation() throws Exception {
    deployDatasetModule("my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    addDatasetInstance("myKeyValueTable", "myTable", DatasetProperties.EMPTY);
    testAppWithDataset(AppsWithDataset.AppUsesAnnotation.class, "MyServiceWithUseDataSetAnnotation");
  }

  @Test(timeout = 60000L)
  public void testDatasetWithoutApp() throws Exception {
    // TODO: Although this has nothing to do with this testcase, deploying a dummy app to create the default namespace
    deployApplication(testSpace, DummyApp.class);
    deployDatasetModule(testSpace, "my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    addDatasetInstance(testSpace, "myKeyValueTable", "myTable", DatasetProperties.EMPTY);
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

  @Category(XSlowTests.class)
  @Test
  public void testByteCodeClassLoader() throws Exception {
    // This test verify bytecode generated classes ClassLoading

    ApplicationManager appManager = deployApplication(testSpace, ClassLoaderTestApp.class);
    FlowManager flowManager = appManager.getFlowManager("BasicFlow").start();

    // Wait for at least 10 records being generated
    RuntimeMetrics flowMetrics = flowManager.getFlowletMetrics("Sink");
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
    return callServicePut(serviceURL, path, body, null);
  }

  @Nullable
  private String callServicePut(URL serviceURL, String path, String body, Integer expectedStatus) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL(serviceURL.toString() + path).openConnection();
    connection.setDoOutput(true);
    connection.setRequestMethod("PUT");
    try (OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream())) {
      out.write(body);
    }
    int expected = expectedStatus == null ? 200 : expectedStatus;
    Assert.assertEquals(expected, connection.getResponseCode());
    if (expectedStatus != null) {
      return null;
    }
    try (
      BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), Charsets.UTF_8))
    ) {
      return reader.readLine();
    }
  }
}
