/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers.log;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.common.guice.NonCustomLocationUnitTestModule;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.NoOpMetadataServiceClient;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.logging.gateway.handlers.FormattedTextLogEvent;
import io.cdap.cdap.logging.gateway.handlers.LogData;
import io.cdap.cdap.logging.gateway.handlers.LogHttpHandler;
import io.cdap.cdap.logging.guice.LogQueryRuntimeModule;
import io.cdap.cdap.logging.read.LogOffset;
import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.logging.service.LogQueryService;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.impersonation.NoOpOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.impersonation.UnsupportedUGIProvider;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.tephra.TransactionManager;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test {@link LogHttpHandler}.
 */
public class LogHttpHandlerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Type LIST_LOGLINE_TYPE = new TypeToken<List<LogLine>>() { }.getType();
  private static final Type LIST_LOGDATA_OFFSET_TYPE = new TypeToken<List<LogDataOffset>>() { }.getType();
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(LogOffset.class, new LogOffsetAdapter()).create();
  private static final String[] FORMATS = new String[] { "text", "json" };

  private static TransactionManager transactionManager;
  private static DatasetOpExecutorService dsOpService;
  private static DatasetService datasetService;

  private static MockLogReader mockLogReader;
  private static LogQueryService logQueryService;
  private static DiscoveryServiceClient discoveryServiceClient;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.LogQuery.ADDRESS, InetAddress.getLoopbackAddress().getHostAddress());

    Injector injector = Guice.createInjector(Modules.override(
      new ConfigModule(cConf),
      new NonCustomLocationUnitTestModule(),
      new InMemoryDiscoveryModule(),
      new LogQueryRuntimeModule().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetsModules().getStandaloneModules(),
      new DataSetServiceModules().getInMemoryModules(),
      new ExploreClientModule(),
      new NamespaceAdminTestModule(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule()
    ).with(new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
        bind(LogReader.class).to(MockLogReader.class).in(Scopes.SINGLETON);
        bind(Store.class).to(DefaultStore.class);
        bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
        bind(OwnerAdmin.class).to(NoOpOwnerAdmin.class);
        // TODO (CDAP-14677): find a better way to inject metadata publisher
        bind(MetadataServiceClient.class).to(NoOpMetadataServiceClient.class);
      }
    }));

    transactionManager = injector.getInstance(TransactionManager.class);
    transactionManager.startAndWait();
    StructuredTableRegistry structuredTableRegistry = injector.getInstance(StructuredTableRegistry.class);
    structuredTableRegistry.initialize();
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class), structuredTableRegistry);
    dsOpService = injector.getInstance(DatasetOpExecutorService.class);
    dsOpService.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    logQueryService = injector.getInstance(LogQueryService.class);
    logQueryService.startAndWait();

    mockLogReader = (MockLogReader) injector.getInstance(LogReader.class);
    mockLogReader.generateLogs();

    discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
  }

  @AfterClass
  public static void tearDown() {
    logQueryService.stopAndWait();

    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    transactionManager.stopAndWait();
  }

  @Test
  public void testWorkerNext() throws Exception {
    testNext("testApp1", "workers", "testWorker1", true, MockLogReader.TEST_NAMESPACE);
    testNextNoMax("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE);
    testNextFilter("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE);
    testNextNoFrom("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE);
    testNext("testApp1", "workers", "testWorker1", false, MockLogReader.TEST_NAMESPACE);
    testNextRunId("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE, "text", ImmutableList.of());
    testNextRunId("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of());
    testNextRunId("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of("logLevel", "lineNumber"));
  }

  @Test
  public void testServiceNext() throws Exception {
    testNext("testApp4", "services", "testService1", true, MockLogReader.TEST_NAMESPACE);
    testNextNoMax("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE);
    testNextFilter("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE);
    testNextNoFrom("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE);
    testNext("testApp4", "services", "testService1", false, MockLogReader.TEST_NAMESPACE);
    testNextRunId("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, "text",
                  ImmutableList.of());
    testNextRunId("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of());
    testNextRunId("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of("logLevel", "lineNumber"));
  }

  @Test
  public void testMapReduceNext() throws Exception {
    testNext("testApp3", "mapreduce", "testMapReduce1", true, NamespaceId.DEFAULT.getEntityName());
    try {
      testNext("testApp3", "mapreduce", "testMapReduce1", true, MockLogReader.TEST_NAMESPACE);
      Assert.fail();
    } catch (AssertionError e) {
      // this must fail
    }
    testNextNoMax("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName());
    testNextFilter("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName());
    testNextNoFrom("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName());
    testNextRunId("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName(), "text",
                  ImmutableList.of());
    testNextRunId("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName(), "json",
                  ImmutableList.of());
    testNextRunId("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName(), "json",
                  ImmutableList.of("logLevel", "lineNumber"));
  }

  @Test
  public void testWorkerPrev() throws Exception {
    testPrev("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE);
    testPrevNoMax("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE);
    testPrevFilter("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE);
    testPrevNoFrom("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE);
    testPrevRunId("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE, "text", ImmutableList.of());
    testPrevRunId("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of());
    testPrevRunId("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of("logLevel", "lineNumber"));
  }

  @Test
  public void testServicePrev() throws Exception {
    testPrev("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE);
    testPrevNoMax("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE);
    testPrevFilter("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE);
    testPrevNoFrom("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE);
    testPrevRunId("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, "text",
                  ImmutableList.of());
    testPrevRunId("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of());
    testPrevRunId("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of("logLevel", "lineNumber"));
  }

  @Test
  public void testMapReducePrev() throws Exception {
    testPrev("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName());
    testPrevNoMax("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName());
    testPrevFilter("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName());
    testPrevNoFrom("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName());
    testPrevRunId("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName(), "text",
                  ImmutableList.of());
    testPrevRunId("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName(), "json",
                  ImmutableList.of());
    testPrevRunId("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName(), "json",
                  ImmutableList.of("logLevel", "lineNumber"));
    try {
      testPrevNoMax("testApp3", "mapreduce", "testMapReduce1", MockLogReader.TEST_NAMESPACE);
    } catch (AssertionError e) {
      // this should fail.
      return;
    }
    Assert.fail();
  }

  @Test
  public void testWorkerLogs() throws Exception {
    testLogs("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE);
    testLogsFilter("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE);
    testLogsRunId("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE, "text",
                  ImmutableList.of());
    testLogsRunId("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of());
    testLogsRunId("testApp1", "workers", "testWorker1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of("logLevel", "lineNumber"));
    try {
      testLogs("testApp1", "workers", "testWorker1", NamespaceId.DEFAULT.getEntityName());
    } catch (AssertionError e) {
      // should fail
      return;
    }
    Assert.fail();
  }

  @Test
  public void testServiceLogs() throws Exception {
    testLogs("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE);
    testLogsFilter("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE);
    testLogsRunId("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, "text",
                  ImmutableList.of());
    testLogsRunId("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of());
    testLogsRunId("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of("logLevel", "lineNumber"));
  }

  @Test
  public void testMapReduceLogs() throws Exception {
    testLogs("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName());
    testLogsFilter("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName());
    testLogsRunId("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName(), "text",
                  ImmutableList.of());
    testLogsRunId("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName(), "json",
                  ImmutableList.of());
    testLogsRunId("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName(), "json",
                  ImmutableList.of("logLevel", "lineNumber"));
    try {
      testLogsFilter("testApp3", "mapreduce", "testMapReduce1", MockLogReader.TEST_NAMESPACE);
    } catch (AssertionError e) {
      // should fail
      return;
    }
    Assert.fail();
  }

  @Test
  public void testWorkflowLogs() throws Exception {
    testLogs("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE);
    testLogsFilter("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE);
  }

  @Test
  public void testWorkflowLogsNext() throws Exception {
    testNext("testTemplate1", "workflows", "testWorkflow1", true, MockLogReader.TEST_NAMESPACE);
    testNextNoMax("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE);
    testNextFilter("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE);
    testNextNoFrom("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE);
  }

  @Test
  public void testWorkflowLogsPrev() throws Exception {
    testPrev("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE);
    testPrevNoMax("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE);
    testPrevFilter("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE);
    testPrevNoFrom("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE);
    testPrevRunId("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE, "text",
                  ImmutableList.of());
    testPrevRunId("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of("timestamp", "logLevel", "threadName", "className", "simpleClassName",
                                   "lineNumber", "message", "stackTrace"));
    testPrevRunId("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of());
    testPrevRunId("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of("logLevel", "threadName"));
  }

  private List<LogLine> getLogs(String namespaceId, String appId, String programType, String programName, String runId,
                                String endPoint, int expectedStatusCode) throws IOException {
    String path = String.format("apps/%s/%s/%s/runs/%s/logs/%s?max=1000", appId, programType, programName, runId,
                                endPoint);
    HttpResponse response = doGet(getVersionedAPIPath(path, namespaceId));
    Assert.assertEquals(expectedStatusCode, response.getResponseCode());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      return ImmutableList.of();
    }
    return GSON.fromJson(response.getResponseBodyAsString(), LIST_LOGLINE_TYPE);
  }

  @Test
  public void testWorkflowRunLogs() throws Exception {
    ProgramId workflowId = MockLogReader.SOME_WORKFLOW_APP
      .workflow(MockLogReader.SOME_WORKFLOW);
    RunRecord runRecord = mockLogReader.getRunRecord(workflowId);
    List<LogLine> logLines = getLogs(MockLogReader.TEST_NAMESPACE, MockLogReader.SOME_WORKFLOW_APP.getApplication(),
                                     "workflows", MockLogReader.SOME_WORKFLOW, runRecord.getPid(), "next");
    Assert.assertEquals(320, logLines.size());
    // First 80 lines correspond to the Workflow
    String log = logLines.get(5).getLog();
    Assert.assertTrue(log.contains(MockLogReader.SOME_WORKFLOW));
    Assert.assertFalse(log.contains(MockLogReader.SOME_MAPREDUCE));
    Assert.assertFalse(log.contains(MockLogReader.SOME_SPARK));
    // Lines 81-160 corresponds to MapReduce
    log = logLines.get(85).getLog();
    Assert.assertFalse(log.contains(MockLogReader.SOME_WORKFLOW));
    Assert.assertTrue(log.contains(MockLogReader.SOME_MAPREDUCE));
    Assert.assertFalse(log.contains(MockLogReader.SOME_SPARK));
    // Lines 161-240 corresponds to Spark
    log = logLines.get(165).getLog();
    Assert.assertFalse(log.contains(MockLogReader.SOME_WORKFLOW));
    Assert.assertFalse(log.contains(MockLogReader.SOME_MAPREDUCE));
    Assert.assertTrue(log.contains(MockLogReader.SOME_SPARK));

    ProgramId mapReduceId = MockLogReader.SOME_WORKFLOW_APP.mr(MockLogReader.SOME_MAPREDUCE);
    runRecord = mockLogReader.getRunRecord(mapReduceId);
    logLines = getLogs(MockLogReader.TEST_NAMESPACE, MockLogReader.SOME_WORKFLOW_APP.getApplication(), "mapreduce",
                       MockLogReader.SOME_MAPREDUCE, runRecord.getPid(), "next");
    // Only 80 lines should correspond to MapReduce
    Assert.assertEquals(80, logLines.size());
    log = logLines.get(10).getLog();
    Assert.assertFalse(log.contains(MockLogReader.SOME_WORKFLOW));
    Assert.assertTrue(log.contains(MockLogReader.SOME_MAPREDUCE));
    Assert.assertFalse(log.contains(MockLogReader.SOME_SPARK));

    ProgramId sparkId = MockLogReader.SOME_WORKFLOW_APP.spark(MockLogReader.SOME_SPARK);
    runRecord = mockLogReader.getRunRecord(sparkId);
    logLines = getLogs(MockLogReader.TEST_NAMESPACE, MockLogReader.SOME_WORKFLOW_APP.getApplication(), "spark",
                       MockLogReader.SOME_SPARK, runRecord.getPid(), "next");
    // Only 80 lines should correspond to Spark
    Assert.assertEquals(80, logLines.size());
    log = logLines.get(15).getLog();
    Assert.assertFalse(log.contains(MockLogReader.SOME_WORKFLOW));
    Assert.assertFalse(log.contains(MockLogReader.SOME_MAPREDUCE));
    Assert.assertTrue(log.contains(MockLogReader.SOME_SPARK));
  }

  @Test
  public void testSystemLogs() throws Exception {
    testPrevSystemLogs(Constants.Service.APP_FABRIC_HTTP);
    testPrevSystemLogs(Constants.Service.MASTER_SERVICES);
    testNextSystemLogs(Constants.Service.APP_FABRIC_HTTP);
    testNextSystemLogs(Constants.Service.MASTER_SERVICES);
  }

  // Verify the Json returned for logs has isNativeMethod set correctly
  @Test
  public void testNativeMethodField() throws Exception {
    ProgramId programId =
      new NamespaceId(MockLogReader.TEST_NAMESPACE).app("testTemplate1").program(ProgramType.
        valueOfCategoryName("workflows"), "testWorkflow1");
    RunRecord runRecord = mockLogReader.getRunRecord(programId);
    String logsUrl = String.format("apps/%s/%s/%s/runs/%s/logs/next?format=json",
                            "testTemplate1", "workflows", "testWorkflow1", runRecord.getPid());
    HttpResponse response = doGet(getVersionedAPIPath(logsUrl, MockLogReader.TEST_NAMESPACE));
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    List<LogDataOffset> logDataOffsetList = GSON.fromJson(response.getResponseBodyAsString(), LIST_LOGDATA_OFFSET_TYPE);
    Assert.assertEquals(logDataOffsetList.size(), 15);
    Assert.assertTrue(logDataOffsetList.get(0).getLog().getNativeMethod());
    Assert.assertFalse(logDataOffsetList.get(1).getLog().getNativeMethod());
    Assert.assertFalse(logDataOffsetList.get(2).getLog().getNativeMethod());
  }


  private List<LogLine> getLogs(String namespaceId, String appId, String programType, String programName, String runId,
                                String endPoint) throws IOException {
    return getLogs(namespaceId, appId, programType, programName, runId, endPoint, HttpURLConnection.HTTP_OK);
  }

  @Test
  public void testNonExistenceRunLogs() throws IOException {
    getLogs(MockLogReader.TEST_NAMESPACE, MockLogReader.SOME_WORKFLOW_APP.getApplication(), "workflows",
            MockLogReader.SOME_WORKFLOW, RunIds.generate().getId(), "next", HttpURLConnection.HTTP_NOT_FOUND);

    getLogs(MockLogReader.TEST_NAMESPACE, MockLogReader.SOME_WORKFLOW_APP.getApplication(), "workflows",
            MockLogReader.SOME_WORKFLOW, RunIds.generate().getId(), "prev", HttpURLConnection.HTTP_NOT_FOUND);
  }

  @Test
  public void testFilterWithEmptyResult() throws Exception {
    String appId = "testTemplate1";
    String entityType = "workflows";
    String entityId = "testWorkflow1";
    String namespace = NamespaceId.DEFAULT.getEntityName();

    ProgramId programId = new NamespaceId(namespace).app(appId).program(ProgramType.valueOfCategoryName(entityType),
                                                                        entityId);
    RunRecord runRecord = mockLogReader.getRunRecord(programId);

    String logsUrl = String.format("apps/%s/%s/%s/runs/%s/logs?format=json&filter=MDC:asdf=nothing",
                                   appId, entityType, entityId, runRecord.getPid());

    HttpResponse response = doGet(getVersionedAPIPath(logsUrl, namespace));
    verifyLogs(response, entityId, "json", true, true, true, 0, 0);
  }

  private void testNext(String appId, String entityType, String entityId, boolean escape, String namespace)
    throws Exception {
    for (String format : FORMATS) {
      String nextUrl = String.format("apps/%s/%s/%s/logs/next?fromOffset=%s&max=10&escape=%s&format=%s",
                                     appId, entityType, entityId, getFromOffset(5), escape, format);
      HttpResponse response = doGet(getVersionedAPIPath(nextUrl, namespace));
      verifyLogs(response, entityId, format, false, false, escape, 10, 5);
    }
  }

  private void testNextNoMax(String appId, String entityType, String entityId, String namespace) throws Exception {
    for (String format : FORMATS) {
      String nextNoMaxUrl = String.format("apps/%s/%s/%s/logs/next?fromOffset=%s&format=%s",
                                          appId, entityType, entityId, getFromOffset(10), format);
      HttpResponse response = doGet(getVersionedAPIPath(nextNoMaxUrl, namespace));
      verifyLogs(response, entityId, format, false, false, true, 50, 10);
    }
  }

  private void testNextFilter(String appId, String entityType, String entityId, String namespace) throws Exception {
    for (String format : FORMATS) {
      String nextFilterUrl =
        String.format("apps/%s/%s/%s/logs/next?fromOffset=%s&max=16&filter=loglevel=ERROR&format=%s",
                      appId, entityType, entityId, getFromOffset(12), format);
      HttpResponse response = doGet(getVersionedAPIPath(nextFilterUrl, namespace));
      verifyLogs(response, entityId, format, true, false, true, 8, 12);
    }
  }

  private void testNextNoFrom(String appId, String entityType, String entityId, String namespace) throws Exception {
    for (String format : FORMATS) {
      String nextNoFromUrl = String.format("apps/%s/%s/%s/logs/next?format=%s", appId, entityType, entityId, format);
      HttpResponse response = doGet(getVersionedAPIPath(nextNoFromUrl, namespace));
      verifyLogs(response, entityId, format, false, false, true, 50, 30);
    }
  }

  private void testNextRunId(String appId, String entityType, String entityId, String namespace, String format,
                             List<String> suppress)
    throws Exception {
    ProgramId programId =
      new NamespaceId(namespace).app(appId).program(ProgramType.valueOfCategoryName(entityType), entityId);
    RunRecord runRecord = mockLogReader.getRunRecord(programId);
    int expectedEvents = 20;
    if (runRecord.getStatus() == ProgramRunStatus.RUNNING || runRecord.getStatus() == ProgramRunStatus.SUSPENDED) {
      expectedEvents = 30;
    }
    String nextNoFromUrl;
    if (suppress.isEmpty()) {
      nextNoFromUrl = String.format("apps/%s/%s/%s/runs/%s/logs/next?format=%s&max=100",
                                    appId, entityType, entityId, runRecord.getPid(), format);
    } else {
      String fieldsToSuppress = getSuppressStr(suppress);
      nextNoFromUrl = String.format("apps/%s/%s/%s/runs/%s/logs/next?format=%s&max=100&suppress=%s",
                                    appId, entityType, entityId, runRecord.getPid(), format, fieldsToSuppress);
    }
    HttpResponse response = doGet(getVersionedAPIPath(nextNoFromUrl, namespace));
    verifyLogs(response, entityId, format, true, false, true, expectedEvents, 20, suppress);
  }

  private void testPrev(String appId, String entityType, String entityId, String namespace) throws Exception {
    for (String format : FORMATS) {
      String prevUrl = String.format("apps/%s/%s/%s/logs/prev?fromOffset=%s&max=10&format=%s",
                                     appId, entityType, entityId, getToOffset(25), format);
      HttpResponse response = doGet(getVersionedAPIPath(prevUrl, namespace));
      verifyLogs(response, entityId, format, false, false, true, 10, 15);
    }
  }

  private void testPrevRunId(String appId, String entityType, String entityId, String namespace, String format,
                             List<String> suppress)
    throws Exception {
    ProgramId programId =
      new NamespaceId(namespace).app(appId).program(ProgramType.valueOfCategoryName(entityType), entityId);
    RunRecord runRecord = mockLogReader.getRunRecord(programId);
    int expectedEvents = 20;
    if (runRecord.getStatus() == ProgramRunStatus.RUNNING || runRecord.getStatus() == ProgramRunStatus.SUSPENDED) {
      expectedEvents = 30;
    }
    String prevRunIdUrl;
    if (suppress.isEmpty()) {
      prevRunIdUrl = String.format("apps/%s/%s/%s/runs/%s/logs/prev?format=%s&max=100",
                                   appId, entityType, entityId, runRecord.getPid(), format);
    } else {
      String fieldsToSuppress = getSuppressStr(suppress);
      prevRunIdUrl = String.format("apps/%s/%s/%s/runs/%s/logs/prev?format=%s&max=100&suppress=%s",
                      appId, entityType, entityId, runRecord.getPid(), format, fieldsToSuppress);
    }

    HttpResponse response = doGet(getVersionedAPIPath(prevRunIdUrl, namespace));
    verifyLogs(response, entityId, format, true, false, true, expectedEvents, 20, suppress);
  }

  private void testNextSystemLogs(String serviceName) throws Exception {
    for (String format : FORMATS) {
      String prevUrl = String.format("/%s/system/services/%s/logs/next?max=10&format=%s",
                                     Constants.Gateway.API_VERSION_3_TOKEN, serviceName, format);
      HttpResponse response = doGet(prevUrl);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    }
  }

  private void testPrevSystemLogs(String serviceName) throws Exception {
    for (String format : FORMATS) {
      String prevUrl = String.format("/%s/system/services/%s/logs/prev?max=10&format=%s",
                                     Constants.Gateway.API_VERSION_3_TOKEN, serviceName, format);
      HttpResponse response = doGet(prevUrl);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    }
  }

  private void testPrevNoMax(String appId, String entityType, String entityId, String namespace) throws Exception {
    for (String format : FORMATS) {
      String prevNoMaxUrl = String.format("apps/%s/%s/%s/logs/prev?fromOffset=%s&format=%s",
                                          appId, entityType, entityId, getToOffset(70), format);
      HttpResponse response = doGet(getVersionedAPIPath(prevNoMaxUrl, namespace));
      verifyLogs(response, entityId, format, false, false, true, 50, 20);
    }
  }

  private void testPrevFilter(String appId, String entityType, String entityId, String namespace) throws Exception {
    for (String format : FORMATS) {
      String prevFilterUrl =
        String.format("apps/%s/%s/%s/logs/prev?fromOffset=%s&max=16&format=%s&filter=loglevel=ERROR",
                      appId, entityType, entityId, getToOffset(41), format);
      HttpResponse response = doGet(getVersionedAPIPath(prevFilterUrl, namespace));
      verifyLogs(response, entityId, format, true, false, true, 8, 26);
    }
  }

  private void testPrevNoFrom(String appId, String entityType, String entityId, String namespace) throws Exception {
    String prevNoFrom = String.format("apps/%s/%s/%s/logs/prev", appId, entityType, entityId);
    HttpResponse response = doGet(getVersionedAPIPath(prevNoFrom, namespace));
    verifyLogs(response, entityId, "text", false, false, true, 50, 30);
  }

  private void testLogsRunId(String appId, String entityType, String entityId, String namespace, String format,
                             List<String> suppress) throws Exception {
    ProgramId programId =
      new NamespaceId(namespace).app(appId).program(ProgramType.valueOfCategoryName(entityType), entityId);
    RunRecord runRecord = mockLogReader.getRunRecord(programId);
    int expectedEvents = 20;
    if (runRecord.getStatus() == ProgramRunStatus.RUNNING || runRecord.getStatus() == ProgramRunStatus.SUSPENDED) {
      expectedEvents = 30;
    }
    long startTime = MockLogReader.getMockTimeSecs(0);
    long stopTime = MockLogReader.getMockTimeSecs(100);
    String nextNoFromUrl;
    if (suppress.isEmpty()) {
      nextNoFromUrl = String.format("apps/%s/%s/%s/runs/%s/logs?format=%s&start=%s&stop=%s",
                                    appId, entityType, entityId, runRecord.getPid(), format, startTime, stopTime);
    } else {
      String fieldsToSuppress = getSuppressStr(suppress);
      nextNoFromUrl = String.format("apps/%s/%s/%s/runs/%s/logs?format=%s&start=%s&stop=%s&suppress=%s",
                                    appId, entityType, entityId, runRecord.getPid(), format, startTime,
                                    stopTime, fieldsToSuppress);
    }
    HttpResponse response = doGet(getVersionedAPIPath(nextNoFromUrl, namespace));
    verifyLogs(response, entityId, format, true, true, true, expectedEvents, 20, suppress);
  }

  private void testLogs(String appId, String entityType, String entityId, String namespace) throws Exception {
    long startTime = MockLogReader.getMockTimeSecs(20);
    long stopTime = MockLogReader.getMockTimeSecs(35);
    for (String format : FORMATS) {
      String logsUrl = String.format("apps/%s/%s/%s/logs?start=%s&stop=%s&format=%s",
                                     appId, entityType, entityId, startTime, stopTime, format);
      HttpResponse response = doGet(getVersionedAPIPath(logsUrl, namespace));
      verifyLogs(response, entityId, format, false, true, true, 15, 20);

      // Try with invalid time range -> start > stop
      logsUrl = String.format("apps/%s/%s/%s/logs?start=%s&stop=%s&format=%s",
                              appId, entityType, entityId, 350, 300, format);
      response = doGet(getVersionedAPIPath(logsUrl, namespace));
      Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.getResponseCode());
    }
  }

  private void testLogsFilter(String appId, String entityType, String entityId, String namespace) throws Exception {
    long startTime = MockLogReader.getMockTimeSecs(20);
    long stopTime = MockLogReader.getMockTimeSecs(35);
    for (String format : FORMATS) {
      String logsFilterUrl = String.format("apps/%s/%s/%s/logs?start=%s&stop=%s&format=%s&filter=loglevel=ERROR", appId,
                                           entityType, entityId, startTime, stopTime, format);
      HttpResponse response = doGet(getVersionedAPIPath(logsFilterUrl, namespace));
      verifyLogs(response, entityId, format, true, true, true, 8, 20);

      // Test origin filter
      String originFilterUrl = String.format("apps/%s/%s/%s/logs?start=%s&stop=%s&format=%s&filter=.origin=plugin",
                                             appId, entityType, entityId, startTime, stopTime, format);
      // There are 2 logs with .origin=plugin and loglevel=ERROR starting from 24
      response = doGet(getVersionedAPIPath(originFilterUrl + "%20AND%20loglevel=ERROR", namespace));
      verifyLogs(response, entityId, format, 6, true, true, 2, 24, ImmutableList.of());

      // There are 3 logs with .origin=program and MDC:eventType=lifeCycle starting from 22
      originFilterUrl = String.format("apps/%s/%s/%s/logs?start=%s&stop=%s&format=%s&filter=.origin=program", appId,
                                      entityType, entityId, startTime, stopTime, format);
      response = doGet(getVersionedAPIPath(originFilterUrl + "%20AND%20MDC:eventType=lifecycle", namespace));
      verifyLogs(response, entityId, format, 6, true, true, 3, 22, ImmutableList.of());

      originFilterUrl = String.format("apps/%s/%s/%s/logs?start=%s&stop=%s&format=%s&filter=loglevel=ERROR", appId,
                                      entityType, entityId, startTime, stopTime, format);
      // Test complex filters with combining AndFilter and OrFilter. Filters are combined from right to left.
      // Therefore, ".origin=plugin OR .origin=program OR .origin=system" is first combined to a single OrFilter,
      // which all logs can pass. Then loglevel=ERROR is combined with this OrFilter to form an AndFilter.
      // The whole filter therefore filters out logs with loglevel=ERROR.
      response = doGet(getVersionedAPIPath(
        originFilterUrl + "%20AND%20.origin=plugin%20OR%20.origin=program%20OR%20.origin=system", namespace));
      verifyLogs(response, entityId, format, 2, true, true, 8, 20, ImmutableList.of());
    }
  }

  /**
   * Verify the logs returned in the {@link HttpResponse}.
   *
   * @param response {@link HttpResponse}
   * @param entityId Entity for which the logs were fetched
   * @param format {@link LogHttpHandler.LogFormatType}
   * @param runIdOrFilter true if the log is fetched for a runId or a filter was used
   * @param fullLogs true if /logs endpoint was used (this is because the response format is different
   *                 for /logs vs /next or /prev)
   * @param escapeChoice true if the response was chosen to be escaped
   * @param expectedEvents number of expected logs events
   * @param expectedStartValue expected value in the log message
   * @throws IOException
   */
  private void verifyLogs(HttpResponse response, String entityId, String format, boolean runIdOrFilter,
                          boolean fullLogs, boolean escapeChoice, int expectedEvents, int expectedStartValue)
    throws IOException {
    verifyLogs(response, entityId, format, runIdOrFilter, fullLogs, escapeChoice, expectedEvents, expectedStartValue,
    ImmutableList.of());
  }

  /**
   * Verify the logs returned in the {@link HttpResponse}.
   *
   * @param response {@link HttpResponse}
   * @param entityId Entity for which the logs were fetched
   * @param format {@link LogHttpHandler.LogFormatType}
   * @param runIdOrFilter true if the log is fetched for a runId or a filter was used
   * @param fullLogs true if /logs endpoint was used (this is because the response format is different
   *                 for /logs vs /next or /prev)
   * @param escapeChoice true if the response was chosen to be escaped
   * @param expectedEvents number of expected logs events
   * @param expectedStartValue expected value in the log message
   * @param suppress log fields to suppress
   * @throws IOException
   */
  private void verifyLogs(HttpResponse response, String entityId, String format, boolean runIdOrFilter,
                          boolean fullLogs, boolean escapeChoice, int expectedEvents, int expectedStartValue,
                          List<String> suppress) throws IOException {
    int stepSize = runIdOrFilter ? 2 : 1;
    verifyLogs(response, entityId, format, stepSize, fullLogs, escapeChoice, expectedEvents, expectedStartValue,
               suppress);
  }

  /**
   * Verify the logs returned in the {@link HttpResponse}.
   *
   * @param response {@link HttpResponse}
   * @param entityId Entity for which the logs were fetched
   * @param format {@link LogHttpHandler.LogFormatType}
   * @param stepSize the number used to increment expected integer value in the log message every time
   * @param fullLogs true if /logs endpoint was used (this is because the response format is different
   *                 for /logs vs /next or /prev)
   * @param escapeChoice true if the response was chosen to be escaped
   * @param expectedEvents number of expected logs events
   * @param expectedStartValue expected value in the log message
   * @param suppress log fields to suppress
   */
  private void verifyLogs(HttpResponse response, String entityId, String format, int stepSize,
                          boolean fullLogs, boolean escapeChoice, int expectedEvents, int expectedStartValue,
                          List<String> suppress) {
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    String out = response.getResponseBodyAsString();
    List<String> logMessages = new ArrayList<>();
    boolean escape;
    // based on the format choose the appropriate GSON deserialization type
    switch (format) {
      case "json":
        // escape choice is always false in the json format
        escape = false;
        List<LogDataOffset> logDataOffsetList = GSON.fromJson(out, LIST_LOGDATA_OFFSET_TYPE);
        for (LogDataOffset logDataOffset : logDataOffsetList) {
          verifyFieldsToSuppress(logDataOffset.getLog(), suppress);
          logMessages.add(logDataOffset.getLog().getMessage());
        }
        break;
      default:
        escape = escapeChoice;
        // if full logs were requested for text format, the format is different between /logs and /next (or /prev)
        if (fullLogs) {
          logMessages = Lists.newArrayList(Splitter.on("\n").omitEmptyStrings().split(out));
        } else {
          List<LogLine> logLines = GSON.fromJson(out, LIST_LOGLINE_TYPE);
          for (LogLine logLine : logLines) {
            logMessages.add(logLine.getLog().trim());
          }
        }
    }

    Assert.assertEquals(expectedEvents, logMessages.size());
    int expected = expectedStartValue;
    for (String log : logMessages) {
      String expectedStr = entityId;
      if (!escape) {
        expectedStr += "<img>-" + expected;
      } else {
        expectedStr += "&lt;img&gt;-" + expected;
      }
      if (!Strings.isNullOrEmpty(log)) {
        Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      }
      // Figure out what is the next expected integer value in the log message
      expected = expected + stepSize;
    }
  }

  private void verifyFieldsToSuppress(LogData logData, List<String> suppress) {
    for (String field : suppress) {
      try {
        Field declaredField = logData.getClass().getDeclaredField(field);
        declaredField.setAccessible(true);
        if (declaredField.get(logData) != null) {
          Assert.fail(String.format("The field %s should not be present in LogData", field));
        }
      } catch (IllegalAccessException e) {
        Assert.fail(String.format("The field %s is not accessible in LogData", field));
      } catch (NoSuchFieldException e) {
        Assert.fail(String.format("The field %s is not present in LogData", field));
      }
    }
  }

  private String getFromOffset(long offset) {
    return FormattedTextLogEvent.formatLogOffset(new LogOffset(offset, -1));
  }

  private String getToOffset(long offset) {
    return FormattedTextLogEvent.formatLogOffset(new LogOffset(offset, Long.MAX_VALUE));
  }

  private String getSuppressStr(List<String> suppress) {
    return Joiner.on("&suppress=").join(suppress);
  }

  /**
   * Given a non-versioned API path, returns its corresponding versioned API path with v3 and default namespace.
   *
   * @param nonVersionedApiPath API path without version
   * @param namespace the namespace
   */
  private static String getVersionedAPIPath(String nonVersionedApiPath, String namespace) {
    String version = Constants.Gateway.API_VERSION_3_TOKEN;
    return String.format("/%s/namespaces/%s/%s", version, namespace, nonVersionedApiPath);
  }

  /**
   * Performs a get call on the given path from the log query server.
   */
  private HttpResponse doGet(String path) throws IOException {
    Discoverable discoverable = new RandomEndpointStrategy(
      () -> discoveryServiceClient.discover(Constants.Service.LOG_QUERY)).pick(10, TimeUnit.SECONDS);
    Assert.assertNotNull(discoverable);

    // Path is literal, hence replacing the "%" with "%%" for formatter
    URL url = URIScheme.createURI(discoverable, path.replace("%", "%%")).toURL();
    return HttpRequests.execute(HttpRequest.get(url).build(), new DefaultHttpRequestConfig(false));
  }
}
