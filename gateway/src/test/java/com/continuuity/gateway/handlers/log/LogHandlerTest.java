package com.continuuity.gateway.handlers.log;

import com.continuuity.gateway.handlers.metrics.MetricsSuiteTestBase;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Test LogHandler.
 */
public class LogHandlerTest extends MetricsSuiteTestBase {
  private static final Type LIST_LOGLINE_TYPE = new TypeToken<List<LogLine>>() { }.getType();
  public static String account = "developer";

  @Test
  public void testFlowNext() throws Exception {
    testNext("testApp1", "flows", "testFlow1", true);
    testNextNoMax("testApp1", "flows", "testFlow1");
    testNextFilter("testApp1", "flows", "testFlow1");
    testNextNoFrom("testApp1", "flows", "testFlow1");
    testNext("testApp1", "flows", "testFlow1", false);
  }

  @Test
  public void testServiceNext() throws Exception {
    testNext("testApp4", "services", "testService1", true);
    testNextNoMax("testApp4", "services", "testService1");
    testNextFilter("testApp4", "services", "testService1");
    testNextNoFrom("testApp4", "services", "testService1");
    testNext("testApp4", "services", "testService1", false);
  }

  @Test
  public void testProcedureNext() throws Exception {
    testNext("testApp2", "procedures", "testProcedure1", true);
    testNextNoMax("testApp2", "procedures", "testProcedure1");
    testNextFilter("testApp2", "procedures", "testProcedure1");
    testNextNoFrom("testApp2", "procedures", "testProcedure1");
  }

  @Test
  public void testMapReduceNext() throws Exception {
    testNext("testApp3", "mapreduce", "testMapReduce1", true);
    testNextNoMax("testApp3", "mapreduce", "testMapReduce1");
    testNextFilter("testApp3", "mapreduce", "testMapReduce1");
    testNextNoFrom("testApp3", "mapreduce", "testMapReduce1");
  }

  @Test
  public void testFlowPrev() throws Exception {
    testPrev("testApp1", "flows", "testFlow1");
    testPrevNoMax("testApp1", "flows", "testFlow1");
    testPrevFilter("testApp1", "flows", "testFlow1");
    testPrevNoFrom("testApp1", "flows", "testFlow1");
  }

  @Test
  public void testServicePrev() throws Exception {
    testPrev("testApp4", "services", "testService1");
    testPrevNoMax("testApp4", "services", "testService1");
    testPrevFilter("testApp4", "services", "testService1");
    testPrevNoFrom("testApp4", "services", "testService1");
  }

  @Test
  public void testProcedurePrev() throws Exception {
    testPrev("testApp2", "procedures", "testProcedure1");
    testPrevNoMax("testApp2", "procedures", "testProcedure1");
    testPrevFilter("testApp2", "procedures", "testProcedure1");
    testPrevNoFrom("testApp2", "procedures", "testProcedure1");
  }

  @Test
  public void testMapReducePrev() throws Exception {
    testPrev("testApp3", "mapreduce", "testMapReduce1");
    testPrevNoMax("testApp3", "mapreduce", "testMapReduce1");
    testPrevFilter("testApp3", "mapreduce", "testMapReduce1");
    testPrevNoFrom("testApp3", "mapreduce", "testMapReduce1");
  }

  @Test
  public void testFlowLogs() throws Exception {
    testLogs("testApp1", "flows", "testFlow1");
    testLogsFilter("testApp1", "flows", "testFlow1");
  }

  @Test
  public void testServiceLogs() throws Exception {
    testLogs("testApp4", "services", "testService1");
    testLogsFilter("testApp4", "services", "testService1");
  }

  @Test
  public void testProcedureLogs() throws Exception {
    testLogs("testApp2", "procedures", "testProcedure1");
    testLogsFilter("testApp2", "procedures", "testProcedure1");
  }

  @Test
  public void testMapReduceLogs() throws Exception {
    testLogs("testApp3", "mapreduce", "testMapReduce1");
    testLogsFilter("testApp3", "mapreduce", "testMapReduce1");
  }

  private void testNext(String appId, String entityType, String entityId, boolean escape) throws Exception {
    String img = escape ? "&lt;img&gt;" : "<img>";
    HttpResponse response = doGet(String.format("/v2/apps/%s/%s/%s/logs/next?fromOffset=5&max=10&escape=%s",
                                                appId, entityType, entityId, escape)
    );
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = new Gson().fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(10, logLines.size());
    int expected = 5;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset());
      String expectedStr = entityId + img + "-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected++;
    }
  }

  private void testNextNoMax(String appId, String entityType, String entityId) throws Exception {
    HttpResponse response = doGet(String.format("/v2/apps/%s/%s/%s/logs/next?fromOffset=10",
                                                appId, entityType, entityId));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = new Gson().fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(50, logLines.size());
    int expected = 10;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset());
      String expectedStr = entityId + "&lt;img&gt;-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected++;
    }
  }

  private void testNextFilter(String appId, String entityType, String entityId) throws Exception {
    HttpResponse response = doGet(String.format("/v2/apps/%s/%s/%s/logs/next?fromOffset=12&max=16&filter=loglevel=EVEN",
                                                appId, entityType, entityId));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = new Gson().fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(8, logLines.size());
    int expected = 12;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset());
      String expectedStr = entityId + "&lt;img&gt;-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected += 2;
    }
  }

  private void testNextNoFrom(String appId, String entityType, String entityId) throws Exception {
    HttpResponse response = doGet(String.format("/v2/apps/%s/%s/%s/logs/next", appId, entityType, entityId));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = new Gson().fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(50, logLines.size());
    int expected = 30;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset());
      String expectedStr = entityId + "&lt;img&gt;-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected++;
    }
  }

  private void testPrev(String appId, String entityType, String entityId) throws Exception {
    HttpResponse response = doGet(String.format("/v2/apps/%s/%s/%s/logs/prev?fromOffset=25&max=10",
                                                appId, entityType, entityId));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = new Gson().fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(10, logLines.size());
    int expected = 15;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset());
      String expectedStr = entityId + "&lt;img&gt;-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected++;
    }
  }

  private void testPrevNoMax(String appId, String entityType, String entityId) throws Exception {
    HttpResponse response = doGet(String.format("/v2/apps/%s/%s/%s/logs/prev?fromOffset=70",
                                                appId, entityType, entityId));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = new Gson().fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(50, logLines.size());
    int expected = 20;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset());
      String expectedStr = entityId + "&lt;img&gt;-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected++;
    }
  }

  private void testPrevFilter(String appId, String entityType, String entityId) throws Exception {
    HttpResponse response = doGet(String.format("/v2/apps/%s/%s/%s/logs/prev?fromOffset=41&max=16&filter=loglevel=EVEN",
                                                appId, entityType, entityId));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = new Gson().fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(8, logLines.size());
    int expected = 26;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset());
      String expectedStr = entityId + "&lt;img&gt;-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected += 2;
    }
  }

  private void testPrevNoFrom(String appId, String entityType, String entityId) throws Exception {
    HttpResponse response = doGet(String.format("/v2/apps/%s/%s/%s/logs/prev", appId, entityType, entityId));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<LogLine> logLines = new Gson().fromJson(out, LIST_LOGLINE_TYPE);
    Assert.assertEquals(50, logLines.size());
    int expected = 30;
    for (LogLine logLine : logLines) {
      Assert.assertEquals(expected, logLine.getOffset());
      String expectedStr = entityId + "&lt;img&gt;-" + expected + "\n";
      String log = logLine.getLog();
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected++;
    }
  }

  private void testLogs(String appId, String entityType, String entityId) throws Exception {
    HttpResponse response = doGet(String.format("/v2/apps/%s/%s/%s/logs?start=20&stop=35",
                                                appId, entityType, entityId));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<String> logLines = Lists.newArrayList(Splitter.on("\n").split(out));
    logLines.remove(logLines.size() - 1);  // Remove last element that is empty
    Assert.assertEquals(15, logLines.size());
    int expected = 20;
    for (String log : logLines) {
      String expectedStr = entityId + "&lt;img&gt;-" + expected;
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected++;
    }
  }

  private void testLogsFilter(String appId, String entityType, String entityId) throws Exception {
    HttpResponse response = doGet(String.format("/v2/apps/%s/%s/%s/logs?start=20&stop=35&filter=loglevel=EVEN",
                                                appId, entityType, entityId));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    String out = EntityUtils.toString(response.getEntity());
    List<String> logLines = Lists.newArrayList(Splitter.on("\n").split(out));
    logLines.remove(logLines.size() - 1);  // Remove last element that is empty
    Assert.assertEquals(8, logLines.size());
    int expected = 20;
    for (String log : logLines) {
      String expectedStr = entityId + "&lt;img&gt;-" + expected;
      Assert.assertEquals(expectedStr, log.substring(log.length() - expectedStr.length()));
      expected += 2;
    }
  }
}
