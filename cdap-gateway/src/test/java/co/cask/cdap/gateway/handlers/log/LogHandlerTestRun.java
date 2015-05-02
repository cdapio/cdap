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

package co.cask.cdap.gateway.handlers.log;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.metrics.MetricsSuiteTestBase;
import co.cask.cdap.logging.gateway.handlers.FormattedLogEvent;
import co.cask.cdap.logging.read.LogOffset;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import com.google.common.base.Splitter;
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

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Test LogHandler.
 */
public class LogHandlerTestRun extends MetricsSuiteTestBase {
  private static final Type LIST_LOGLINE_TYPE = new TypeToken<List<LogLine>>() { }.getType();
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
    testNext("testApp1", "flows", "testFlow1", true, MockLogReader.TEST_NAMESPACE, null);
    testNextNoMax("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, null);
    testNextFilter("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, null);
    testNextNoFrom("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, null);
    testNext("testApp1", "flows", "testFlow1", false, MockLogReader.TEST_NAMESPACE, null);
    testNextRunId("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, null);
  }

  @Test
  public void testServiceNext() throws Exception {
    testNext("testApp4", "services", "testService1", true, MockLogReader.TEST_NAMESPACE, null);
    testNextNoMax("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, null);
    testNextFilter("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, null);
    testNextNoFrom("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, null);
    testNext("testApp4", "services", "testService1", false, MockLogReader.TEST_NAMESPACE, null);
    testNextRunId("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, null);
  }

  @Test
  public void testMapReduceNext() throws Exception {
    testNext("testApp3", "mapreduce", "testMapReduce1", true, Constants.DEFAULT_NAMESPACE, null);
    try {
      testNext("testApp3", "mapreduce", "testMapReduce1", true, MockLogReader.TEST_NAMESPACE, null);
      Assert.fail();
    } catch (AssertionError e) {
      // this must fail
    }
    testNextNoMax("testApp3", "mapreduce", "testMapReduce1", Constants.DEFAULT_NAMESPACE, null);
    testNextFilter("testApp3", "mapreduce", "testMapReduce1", Constants.DEFAULT_NAMESPACE, null);
    testNextNoFrom("testApp3", "mapreduce", "testMapReduce1", Constants.DEFAULT_NAMESPACE, null);
    testNextRunId("testApp3", "mapreduce", "testMapReduce1", Constants.DEFAULT_NAMESPACE, null);
  }

  @Test
  public void testFlowPrev() throws Exception {
    testPrev("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, null);
    testPrevNoMax("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, null);
    testPrevFilter("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, null);
    testPrevNoFrom("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, null);
    testPrevRunId("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, null);
  }

  @Test
  public void testServicePrev() throws Exception {
    testPrev("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, null);
    testPrevNoMax("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, null);
    testPrevFilter("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, null);
    testPrevNoFrom("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, null);
    testPrevRunId("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, null);
  }

  @Test
  public void testMapReducePrev() throws Exception {
    testPrev("testApp3", "mapreduce", "testMapReduce1", Constants.DEFAULT_NAMESPACE, null);
    testPrevNoMax("testApp3", "mapreduce", "testMapReduce1", Constants.DEFAULT_NAMESPACE, null);
    testPrevFilter("testApp3", "mapreduce", "testMapReduce1", Constants.DEFAULT_NAMESPACE, null);
    testPrevNoFrom("testApp3", "mapreduce", "testMapReduce1", Constants.DEFAULT_NAMESPACE, null);
    testPrevRunId("testApp3", "mapreduce", "testMapReduce1", Constants.DEFAULT_NAMESPACE, null);
    try {
      testPrevNoMax("testApp3", "mapreduce", "testMapReduce1", MockLogReader.TEST_NAMESPACE, null);
    } catch (AssertionError e) {
      // this should fail.
      return;
    }
    Assert.fail();
  }

  @Test
  public void testFlowLogs() throws Exception {
    testLogs("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, null);
    testLogsFilter("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, null);
    testLogsRunId("testApp1", "flows", "testFlow1", MockLogReader.TEST_NAMESPACE, null);
    try {
      testLogs("testApp1", "flows", "testFlow1", Constants.DEFAULT_NAMESPACE, null);
    } catch (AssertionError e) {
      // should fail
      return;
    }
    Assert.fail();
  }

  @Test
  public void testServiceLogs() throws Exception {
    testLogs("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, null);
    testLogsFilter("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, null);
    testLogsRunId("testApp4", "services", "testService1", MockLogReader.TEST_NAMESPACE, null);
  }

  @Test
  public void testMapReduceLogs() throws Exception {
    testLogs("testApp3", "mapreduce", "testMapReduce1", Constants.DEFAULT_NAMESPACE, null);
    testLogsFilter("testApp3", "mapreduce", "testMapReduce1", Constants.DEFAULT_NAMESPACE, null);
    testLogsRunId("testApp3", "mapreduce", "testMapReduce1", Constants.DEFAULT_NAMESPACE, null);
    try {
      testLogsFilter("testApp3", "mapreduce", "testMapReduce1", MockLogReader.TEST_NAMESPACE, null);
    } catch (AssertionError e) {
      // should fail
      return;
    }
    Assert.fail();
  }

  @Test
  public void testAdapterLogs() throws Exception {
    testLogs("testTemplate1", "mapreduce", "testMapReduce1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    // also test that the same logs appear as mapreduce logs as well
    testLogs("testTemplate1", "mapreduce", "testMapReduce1", MockLogReader.TEST_NAMESPACE, null);
    testLogsFilter("testTemplate1", "mapreduce", "testMapReduce1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    // tests adapter logs with run id
    testLogsRunId("testTemplate1", "mapreduce", "testMapReduce1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    try {
      // in default namespace, mapreduce was run outside of adapter, so shouldn't find adapter logs
      testLogs("testTemplate1", "mapreduce", "testMapReduce1", Constants.DEFAULT_NAMESPACE, "testAdapter1");
    } catch (AssertionError e) {
      // Expected exception
      return;
    }
    Assert.fail();
  }

  @Test
  public void testAdapterLogsNext() throws Exception {
    testNext("testTemplate1", "mapreduce", "testMapReduce1", true, MockLogReader.TEST_NAMESPACE, "testAdapter1");
    testNextNoMax("testTemplate1", "mapreduce", "testMapReduce1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    testNextFilter("testTemplate1", "mapreduce", "testMapReduce1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    testNextNoFrom("testTemplate1", "mapreduce", "testMapReduce1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    try {
      testNext("testTemplate1", "mapreduce", "testMapReduce1", true, Constants.DEFAULT_NAMESPACE, "testAdapter1");
    } catch (AssertionError e) {
      // this must fail
      return;
    }
    Assert.fail();
  }

  @Test
  public void testAdapterLogsPrev() throws Exception {
    testPrev("testTemplate1", "mapreduce", "testMapReduce1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    testPrevNoMax("testTemplate1", "mapreduce", "testMapReduce1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    testPrevFilter("testTemplate1", "mapreduce", "testMapReduce1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    testPrevNoFrom("testTemplate1", "mapreduce", "testMapReduce1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    try {
      testPrevNoMax("testTemplate1", "mapreduce", "testMapReduce1", Constants.DEFAULT_NAMESPACE, "testAdapter1");
    } catch (AssertionError e) {
      // this must fail.
      return;
    }
    Assert.fail();
  }

  @Test
  public void testWorkflowLogs() throws Exception {
    testLogs("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    testLogsFilter("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    try {
      // in default namespace, we did not produce logs through adapters
      testNext("testTemplate1", "workflows", "testWorkflow1", true, Constants.DEFAULT_NAMESPACE, "testAdapter1");
    } catch (AssertionError e) {
      // this must fail
      return;
    }
    Assert.fail();
  }

  @Test
  public void testWorkflowLogsNext() throws Exception {
    testNext("testTemplate1", "workflows", "testWorkflow1", true, MockLogReader.TEST_NAMESPACE, "testAdapter1");
    testNextNoMax("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    testNextFilter("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    testNextNoFrom("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    try {
      // in default namespace, we did not produce logs through adapters
      testNext("testTemplate1", "workflows", "testWorkflow1", true, Constants.DEFAULT_NAMESPACE, "testAdapter1");
    } catch (AssertionError e) {
      // this must fail
      return;
    }
    Assert.fail();
  }

  @Test
  public void testWorkflowLogsPrev() throws Exception {
    testPrev("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    testPrevNoMax("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    testPrevFilter("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    testPrevNoFrom("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    testPrevRunId("testTemplate1", "workflows", "testWorkflow1", MockLogReader.TEST_NAMESPACE, "testAdapter1");
    try {
      // in default namespace, we did not produce logs through adapters
      testPrevNoMax("testTemplate1", "workflows", "testWorkflow1", Constants.DEFAULT_NAMESPACE, "testAdapter1");
    } catch (AssertionError e) {
      // this should fail.
      return;
    }
    Assert.fail();
  }

  @Test
  public void testSystemLogs() throws Exception {
    testPrevSystemLogs(Constants.Service.APP_FABRIC_HTTP);
    testPrevSystemLogs(Constants.Service.MASTER_SERVICES);
    testNextSystemLogs(Constants.Service.APP_FABRIC_HTTP);
    testNextSystemLogs(Constants.Service.MASTER_SERVICES);
  }

  private void testNext(String appId, String entityType, String entityId, boolean escape, String namespace,
                        @Nullable String adapterId)
    throws Exception {
    String img = escape ? "&lt;img&gt;" : "<img>";
    String nextUrl = String.format("apps/%s/%s/%s/logs/next?fromOffset=%s&max=10&escape=%s",
                                   appId, entityType, entityId, getFromOffset(5), escape);
    nextUrl = getUrlWithAdapterId(nextUrl, adapterId, "&");
    HttpResponse response = doGet(getVersionedAPIPath(nextUrl, namespace));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = GSON.fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(10, logLines.size());
    int expected = 5;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset().getKafkaOffset());
      Assert.assertEquals(expected, logLine.getOffset().getTime());
      String expectedStr = entityId + img + "-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected++;
    }
  }

  private void testNextNoMax(String appId, String entityType, String entityId, String namespace,
                             @Nullable String adapterId) throws Exception {
    String nextNoMaxUrl = String.format("apps/%s/%s/%s/logs/next?fromOffset=%s",
                                        appId, entityType, entityId, getFromOffset(10));
    nextNoMaxUrl = getUrlWithAdapterId(nextNoMaxUrl, adapterId, "&");
    HttpResponse response = doGet(getVersionedAPIPath(nextNoMaxUrl, namespace));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = GSON.fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(50, logLines.size());
    int expected = 10;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset().getKafkaOffset());
      Assert.assertEquals(expected, logLine.getOffset().getTime());
      String expectedStr = entityId + "&lt;img&gt;-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected++;
    }
  }

  private void testNextFilter(String appId, String entityType, String entityId, String namespace,
                              @Nullable String adapterId) throws Exception {
    String nextFilterUrl = String.format("apps/%s/%s/%s/logs/next?fromOffset=%s&max=16&filter=loglevel=ERROR", appId,
                                         entityType, entityId, getFromOffset(12));
    nextFilterUrl = getUrlWithAdapterId(nextFilterUrl, adapterId, "&");
    HttpResponse response = doGet(getVersionedAPIPath(nextFilterUrl, namespace));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = GSON.fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(8, logLines.size());
    int expected = 12;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset().getKafkaOffset());
      Assert.assertEquals(expected, logLine.getOffset().getTime());
      String expectedStr = entityId + "&lt;img&gt;-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected += 2;
    }
  }

  private void testNextNoFrom(String appId, String entityType, String entityId, String namespace,
                              @Nullable String adapterId) throws Exception {
    String nextNoFromUrl = String.format("apps/%s/%s/%s/logs/next", appId, entityType, entityId);
    nextNoFromUrl = getUrlWithAdapterId(nextNoFromUrl, adapterId, "?");
    HttpResponse response = doGet(getVersionedAPIPath(nextNoFromUrl, namespace));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = GSON.fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(50, logLines.size());
    int expected = 30;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset().getKafkaOffset());
      Assert.assertEquals(expected, logLine.getOffset().getTime());
      String expectedStr = entityId + "&lt;img&gt;-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected++;
    }
  }

  private void testNextRunId(String appId, String entityType, String entityId, String namespace,
                             @Nullable String adapterId) throws Exception {
    Id.Program id = Id.Program.from(namespace, appId, ProgramType.valueOfCategoryName(entityType), entityId);
    RunRecord runRecord = mockLogReader.getRunRecord(id);
    int expectedEvents = 20;
    if (runRecord.getStatus() == ProgramRunStatus.RUNNING || runRecord.getStatus() == ProgramRunStatus.SUSPENDED) {
      expectedEvents = 30;
    }
    String nextNoFromUrl = String.format("apps/%s/%s/%s/runs/%s/logs/next?max=100",
                                         appId, entityType, entityId, runRecord.getPid());
    nextNoFromUrl = getUrlWithAdapterId(nextNoFromUrl, adapterId, "&");
    HttpResponse response = doGet(getVersionedAPIPath(nextNoFromUrl, namespace));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = GSON.fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(expectedEvents, logLines.size());
    int expected = 20;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset().getKafkaOffset());
      Assert.assertEquals(expected, logLine.getOffset().getTime());
      String expectedStr = entityId + "&lt;img&gt;-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected += 2;
    }
  }

  private void testPrev(String appId, String entityType, String entityId, String namespace,
                        @Nullable String adapterId) throws Exception {
    String prevUrl = String.format("apps/%s/%s/%s/logs/prev?fromOffset=%s&max=10",
                                   appId, entityType, entityId, getToOffset(25));
    prevUrl = getUrlWithAdapterId(prevUrl, adapterId, "&");
    HttpResponse response = doGet(getVersionedAPIPath(prevUrl, namespace));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = GSON.fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(10, logLines.size());
    int expected = 15;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset().getKafkaOffset());
      Assert.assertEquals(expected, logLine.getOffset().getTime());
      String expectedStr = entityId + "&lt;img&gt;-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected++;
    }
  }

  private String getUrlWithAdapterId(String prevUrl, @Nullable String adapterId, String joiner) {
    if (adapterId != null) {
       return String.format("%s%sadapterid=%s", prevUrl, joiner, adapterId);
    }
    return prevUrl;
  }

  private void testPrevRunId(String appId, String entityType, String entityId, String namespace,
                             @Nullable String adapterId) throws Exception {
    Id.Program id = Id.Program.from(namespace, appId, ProgramType.valueOfCategoryName(entityType), entityId);
    RunRecord runRecord = mockLogReader.getRunRecord(id);
    int expectedEvents = 20;
    if (runRecord.getStatus() == ProgramRunStatus.RUNNING || runRecord.getStatus() == ProgramRunStatus.SUSPENDED) {
      expectedEvents = 30;
    }
    String prevRunIdUrl = String.format("apps/%s/%s/%s/runs/%s/logs/prev?max=100",
                                        appId, entityType, entityId, runRecord.getPid());
    prevRunIdUrl = getUrlWithAdapterId(prevRunIdUrl, adapterId, "&");
    HttpResponse response = doGet(getVersionedAPIPath(prevRunIdUrl, namespace));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = GSON.fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(expectedEvents, logLines.size());
    int expected = 20;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset().getKafkaOffset());
      Assert.assertEquals(expected, logLine.getOffset().getTime());
      String expectedStr = entityId + "&lt;img&gt;-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected += 2;
    }
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

  private void testPrevNoMax(String appId, String entityType, String entityId, String namespace,
                             @Nullable String adapterId) throws Exception {
    String prevNoMaxUrl = String.format("apps/%s/%s/%s/logs/prev?fromOffset=%s",
                                        appId, entityType, entityId, getToOffset(70));
    prevNoMaxUrl = getUrlWithAdapterId(prevNoMaxUrl, adapterId, "&");
    HttpResponse response = doGet(getVersionedAPIPath(prevNoMaxUrl, namespace));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = GSON.fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(50, logLines.size());
    int expected = 20;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset().getKafkaOffset());
      Assert.assertEquals(expected, logLine.getOffset().getTime());
      String expectedStr = entityId + "&lt;img&gt;-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected++;
    }
  }

  private void testPrevFilter(String appId, String entityType, String entityId, String namespace,
                              @Nullable String adapterId) throws Exception {
    String prevFilterUrl = String.format("apps/%s/%s/%s/logs/prev?fromOffset=%s&max=16&filter=loglevel=ERROR",
                                         appId, entityType, entityId, getToOffset(41));
    prevFilterUrl = getUrlWithAdapterId(prevFilterUrl, adapterId, "&");
    HttpResponse response = doGet(getVersionedAPIPath(prevFilterUrl, namespace));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = GSON.fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(8, logLines.size());
    int expected = 26;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset().getKafkaOffset());
      Assert.assertEquals(expected, logLine.getOffset().getTime());
      String expectedStr = entityId + "&lt;img&gt;-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected += 2;
    }
  }

  private void testPrevNoFrom(String appId, String entityType, String entityId, String namespace,
                              @Nullable String adapterId) throws Exception {
    String prevNoFrom = String.format("apps/%s/%s/%s/logs/prev", appId, entityType, entityId);
    prevNoFrom = getUrlWithAdapterId(prevNoFrom, adapterId, "?");
    HttpResponse response = doGet(getVersionedAPIPath(prevNoFrom, namespace));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = GSON.fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(50, logLines.size());
    int expected = 30;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset().getKafkaOffset());
      Assert.assertEquals(expected, logLine.getOffset().getTime());
      String expectedStr = entityId + "&lt;img&gt;-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected++;
    }
  }

  private void testLogs(String appId, String entityType, String entityId, String namespace,
                        @Nullable String adapterId) throws Exception {
    long startTime = MockLogReader.getMockTimeSecs(20);
    long stopTime = MockLogReader.getMockTimeSecs(35);
    String logsUrl = String.format("apps/%s/%s/%s/logs?start=%s&stop=%s",
                                   appId, entityType, entityId, startTime, stopTime);
    logsUrl = getUrlWithAdapterId(logsUrl, adapterId, "&");
    HttpResponse response = doGet(getVersionedAPIPath(logsUrl, namespace));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<String> logLines = Lists.newArrayList(Splitter.on("\n").omitEmptyStrings().split(out));
    Assert.assertEquals(15, logLines.size());
    int expected = 20;
    for (String log : logLines) {
      String expectedStr = entityId + "&lt;img&gt;-" + expected;
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected++;
    }

    // Try with invalid time range -> start > stop
    logsUrl = String.format("apps/%s/%s/%s/logs?start=%s&stop=%s",
                                   appId, entityType, entityId, 350, 300);
    logsUrl = getUrlWithAdapterId(logsUrl, adapterId, "&");
    response = doGet(getVersionedAPIPath(logsUrl, namespace));
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(), response.getStatusLine().getStatusCode());
  }

  private void testLogsRunId(String appId, String entityType, String entityId, String namespace,
                             @Nullable String adapterId) throws Exception {
    Id.Program id = Id.Program.from(namespace, appId, ProgramType.valueOfCategoryName(entityType), entityId);
    RunRecord runRecord = mockLogReader.getRunRecord(id);
    int expectedEvents = 20;
    if (runRecord.getStatus() == ProgramRunStatus.RUNNING || runRecord.getStatus() == ProgramRunStatus.SUSPENDED) {
      expectedEvents = 30;
    }
    long startTime = MockLogReader.getMockTimeSecs(0);
    long stopTime = MockLogReader.getMockTimeSecs(100);
    String nextNoFromUrl = String.format("apps/%s/%s/%s/runs/%s/logs?start=%s&stop=%s",
                                         appId, entityType, entityId, runRecord.getPid(), startTime, stopTime);
    nextNoFromUrl = getUrlWithAdapterId(nextNoFromUrl, adapterId, "&");
    HttpResponse response = doGet(getVersionedAPIPath(nextNoFromUrl, namespace));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<String> logLines = Lists.newArrayList(Splitter.on("\n").omitEmptyStrings().split(out));
    Assert.assertEquals(expectedEvents, logLines.size());
    int expected = 20;
    for (String log : logLines) {
      String expectedStr = entityId + "&lt;img&gt;-" + expected;
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected += 2;
    }
  }

  private void testLogsFilter(String appId, String entityType, String entityId, String namespace,
                              @Nullable String adapterId) throws Exception {
    long startTime = MockLogReader.getMockTimeSecs(20);
    long stopTime = MockLogReader.getMockTimeSecs(35);
    String logsFilterUrl = String.format("apps/%s/%s/%s/logs?start=%s&stop=%s&filter=loglevel=ERROR", appId,
                                         entityType, entityId, startTime, stopTime);
    logsFilterUrl = getUrlWithAdapterId(logsFilterUrl, adapterId, "&");
    HttpResponse response = doGet(getVersionedAPIPath(logsFilterUrl, namespace));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<String> logLines = Lists.newArrayList(Splitter.on("\n").omitEmptyStrings().split(out));
    Assert.assertEquals(8, logLines.size());
    int expected = 20;
    for (String log : logLines) {
      String expectedStr = entityId + "&lt;img&gt;-" + expected;
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected += 2;
    }
  }

  private String getFromOffset(long offset) throws UnsupportedEncodingException {
    return FormattedLogEvent.formatLogOffset(new LogOffset(offset, -1));
  }

  private String getToOffset(long offset) throws UnsupportedEncodingException {
    return FormattedLogEvent.formatLogOffset(new LogOffset(offset, Long.MAX_VALUE));
  }
}
