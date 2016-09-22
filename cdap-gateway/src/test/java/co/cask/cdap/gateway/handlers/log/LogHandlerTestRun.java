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

package co.cask.cdap.gateway.handlers.log;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.metrics.MetricsSuiteTestBase;
import co.cask.cdap.logging.gateway.handlers.FormattedTextLogEvent;
import co.cask.cdap.logging.gateway.handlers.LogData;
import co.cask.cdap.logging.gateway.handlers.LogHandler;
import co.cask.cdap.logging.read.LogOffset;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Test LogHandler.
 */
public class LogHandlerTestRun extends MetricsSuiteTestBase {
  private static final Type LIST_LOGLINE_TYPE = new TypeToken<List<LogLine>>() { }.getType();
  private static final Type LIST_LOGDATA_OFFSET_TYPE = new TypeToken<List<LogDataOffset>>() { }.getType();
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(LogOffset.class, new LogOffsetAdapter()).create();

  private static MockLogReader mockLogReader;

  @BeforeClass
  public static void setup() throws Exception {
    mockLogReader = (MockLogReader) logReader;
    mockLogReader.generateLogs();
  }

  @Test
  public void testFlowNext() throws Exception {
    testNext("testApp1", "flows", "testFlow1", true, MockLogReader.TEST_NAMESPACE);
    testNextNoMax("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE);
    testNextFilter("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE);
    testNextNoFrom("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE);
    testNext("testApp1", "flows", "testFlow1", false, MockLogReader.TEST_NAMESPACE);
    testNextRunId("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, "text", ImmutableList.<String>of());
    testNextRunId("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.<String>of());
    testNextRunId("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, "json",
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
                  ImmutableList.<String>of());
    testNextRunId("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.<String>of());
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
                  ImmutableList.<String>of());
    testNextRunId("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName(), "json",
                  ImmutableList.<String>of());
    testNextRunId("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName(), "json",
                  ImmutableList.of("logLevel", "lineNumber"));
  }

  @Test
  public void testFlowPrev() throws Exception {
    testPrev("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE);
    testPrevNoMax("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE);
    testPrevFilter("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE);
    testPrevNoFrom("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE);
    testPrevRunId("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, "text", ImmutableList.<String>of());
    testPrevRunId("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.<String>of());
    testPrevRunId("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of("logLevel", "lineNumber"));
  }

  @Test
  public void testServicePrev() throws Exception {
    testPrev("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE);
    testPrevNoMax("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE);
    testPrevFilter("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE);
    testPrevNoFrom("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE);
    testPrevRunId("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, "text",
                  ImmutableList.<String>of());
    testPrevRunId("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.<String>of());
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
                  ImmutableList.<String>of());
    testPrevRunId("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName(), "json",
                  ImmutableList.<String>of());
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
  public void testFlowLogs() throws Exception {
    testLogs("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE);
    testLogsFilter("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE);
    testLogsRunId("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, "text",
                  ImmutableList.<String>of());
    testLogsRunId("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.<String>of());
    testLogsRunId("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of("logLevel", "lineNumber"));
    try {
      testLogs("testApp1", "flows", "testFlow1", NamespaceId.DEFAULT.getEntityName());
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
                  ImmutableList.<String>of());
    testLogsRunId("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.<String>of());
    testLogsRunId("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of("logLevel", "lineNumber"));
  }

  @Test
  public void testMapReduceLogs() throws Exception {
    testLogs("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName());
    testLogsFilter("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName());
    testLogsRunId("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName(), "text",
                  ImmutableList.<String>of());
    testLogsRunId("testApp3", "mapreduce", "testMapReduce1", NamespaceId.DEFAULT.getEntityName(), "json",
                  ImmutableList.<String>of());
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
                  ImmutableList.<String>of());
    testPrevRunId("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of("timestamp", "logLevel", "threadName", "className", "simpleClassName",
                                   "lineNumber", "message", "stackTrace"));
    testPrevRunId("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.<String>of());
    testPrevRunId("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE, "json",
                  ImmutableList.of("logLevel", "threadName"));
  }

  private List<LogLine> getLogs(String namespaceId, String appId, String programType, String programName, String runId,
                                String endPoint) throws Exception {
    String path = String.format("apps/%s/%s/%s/runs/%s/logs/%s?max=1000", appId, programType, programName, runId,
                                endPoint);
    HttpResponse response = doGet(getVersionedAPIPath(path, namespaceId));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    return GSON.fromJson(out, LIST_LOGLINE_TYPE);
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

  private void testNext(String appId, String entityType, String entityId, boolean escape, String namespace)
    throws Exception {
    String nextUrl = String.format("apps/%s/%s/%s/logs/next?fromOffset=%s&max=10&escape=%s",
                                   appId, entityType, entityId, getFromOffset(5), escape);
    HttpResponse response = doGet(getVersionedAPIPath(nextUrl, namespace));
    verifyLogs(response, entityId, "text", false, false, escape, 10, 5);
  }

  private void testNextNoMax(String appId, String entityType, String entityId, String namespace) throws Exception {
    String nextNoMaxUrl = String.format("apps/%s/%s/%s/logs/next?fromOffset=%s",
                                        appId, entityType, entityId, getFromOffset(10));
    HttpResponse response = doGet(getVersionedAPIPath(nextNoMaxUrl, namespace));
    verifyLogs(response, entityId, "text", false, false, true, 50, 10);
  }

  private void testNextFilter(String appId, String entityType, String entityId, String namespace) throws Exception {
    String nextFilterUrl = String.format("apps/%s/%s/%s/logs/next?fromOffset=%s&max=16&filter=loglevel=ERROR", appId,
                                         entityType, entityId, getFromOffset(12));
    HttpResponse response = doGet(getVersionedAPIPath(nextFilterUrl, namespace));
    verifyLogs(response, entityId, "text", true, false, true, 8, 12);
  }

  private void testNextNoFrom(String appId, String entityType, String entityId, String namespace) throws Exception {
    String nextNoFromUrl = String.format("apps/%s/%s/%s/logs/next", appId, entityType, entityId);
    HttpResponse response = doGet(getVersionedAPIPath(nextNoFromUrl, namespace));
    verifyLogs(response, entityId, "text", false, false, true, 50, 30);
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
    String prevUrl = String.format("apps/%s/%s/%s/logs/prev?fromOffset=%s&max=10",
                                   appId, entityType, entityId, getToOffset(25));
    HttpResponse response = doGet(getVersionedAPIPath(prevUrl, namespace));
    verifyLogs(response, entityId, "text", false, false, true, 10, 15);
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
    String prevUrl = String.format("/%s/system/services/%s/logs/next?max=10",
                                   Constants.Gateway.API_VERSION_3_TOKEN, serviceName);
    HttpResponse response = doGet(prevUrl);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
  }

  private void testPrevSystemLogs(String serviceName) throws Exception {
    String prevUrl = String.format("/%s/system/services/%s/logs/prev?max=10",
                                   Constants.Gateway.API_VERSION_3_TOKEN, serviceName);
    HttpResponse response = doGet(prevUrl);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
  }

  private void testPrevNoMax(String appId, String entityType, String entityId, String namespace) throws Exception {
    String prevNoMaxUrl = String.format("apps/%s/%s/%s/logs/prev?fromOffset=%s",
                                        appId, entityType, entityId, getToOffset(70));
    HttpResponse response = doGet(getVersionedAPIPath(prevNoMaxUrl, namespace));
    verifyLogs(response, entityId, "text", false, false, true, 50, 20);
  }

  private void testPrevFilter(String appId, String entityType, String entityId, String namespace) throws Exception {
    String prevFilterUrl = String.format("apps/%s/%s/%s/logs/prev?fromOffset=%s&max=16&filter=loglevel=ERROR",
                                         appId, entityType, entityId, getToOffset(41));
    HttpResponse response = doGet(getVersionedAPIPath(prevFilterUrl, namespace));
    verifyLogs(response, entityId, "text", true, false, true, 8, 26);
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
    String logsUrl = String.format("apps/%s/%s/%s/logs?start=%s&stop=%s",
                                   appId, entityType, entityId, startTime, stopTime);
    HttpResponse response = doGet(getVersionedAPIPath(logsUrl, namespace));
    verifyLogs(response, entityId, "text", false, true, true, 15, 20);

    // Try with invalid time range -> start > stop
    logsUrl = String.format("apps/%s/%s/%s/logs?start=%s&stop=%s",
                            appId, entityType, entityId, 350, 300);
    response = doGet(getVersionedAPIPath(logsUrl, namespace));
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(), response.getStatusLine().getStatusCode());
  }

  private void testLogsFilter(String appId, String entityType, String entityId, String namespace) throws Exception {
    long startTime = MockLogReader.getMockTimeSecs(20);
    long stopTime = MockLogReader.getMockTimeSecs(35);
    String logsFilterUrl = String.format("apps/%s/%s/%s/logs?start=%s&stop=%s&filter=loglevel=ERROR", appId,
                                         entityType, entityId, startTime, stopTime);
    HttpResponse response = doGet(getVersionedAPIPath(logsFilterUrl, namespace));
    verifyLogs(response, entityId, "text", true, true, true, 8, 20);
  }

  /**
   * Verify the logs returned in the {@link HttpResponse}.
   *
   * @param response {@link HttpResponse}
   * @param entityId Entity for which the logs were fetched
   * @param format {@link LogHandler.LogFormatType}
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
    ImmutableList.<String>of());
  }

  /**
   * Verify the logs returned in the {@link HttpResponse}.
   *
   * @param response {@link HttpResponse}
   * @param entityId Entity for which the logs were fetched
   * @param format {@link LogHandler.LogFormatType}
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
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
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
      expected = expected + (runIdOrFilter ? 2 : 1);
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

  private String getFromOffset(long offset) throws UnsupportedEncodingException {
    return FormattedTextLogEvent.formatLogOffset(new LogOffset(offset, -1));
  }

  private String getToOffset(long offset) throws UnsupportedEncodingException {
    return FormattedTextLogEvent.formatLogOffset(new LogOffset(offset, Long.MAX_VALUE));
  }

  private String getSuppressStr(List<String> suppress) {
    return Joiner.on("&suppress=").join(suppress);
  }
}
