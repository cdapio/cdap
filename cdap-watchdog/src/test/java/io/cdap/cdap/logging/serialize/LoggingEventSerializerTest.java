/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.logging.serialize;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import ch.qos.logback.classic.spi.ThrowableProxy;
import co.cask.cdap.common.logging.ComponentLoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.logging.logback.TestLoggingContext;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.context.LoggingContextHelper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Test cases for LoggingEventSerializer.
 */
public class LoggingEventSerializerTest {
  @BeforeClass
  public static void setUpContext() {
    LoggingContextAccessor.setLoggingContext(new TestLoggingContext("TEST_ACCT_ID1", "TEST_APP_ID1", "RUN1",
                                                                    "INSTANCE1"));
  }

  @Test
  public void testEmptySerialization() throws Exception {
    Logger logger = LoggerFactory.getLogger(LoggingEventSerializerTest.class);
    LoggingEventSerializer serializer = new LoggingEventSerializer();
    ch.qos.logback.classic.spi.LoggingEvent iLoggingEvent = new ch.qos.logback.classic.spi.LoggingEvent(
      getClass().getName(), (ch.qos.logback.classic.Logger) logger, Level.ERROR, "message", null, null);
    iLoggingEvent.setThreadName("thread-1");
    iLoggingEvent.setTimeStamp(10000000L);

    // Serialize
    ILoggingEvent event = new LogMessage(iLoggingEvent, LoggingContextAccessor.getLoggingContext());
    byte [] serializedBytes = serializer.toBytes(event);

    // De-serialize
    ILoggingEvent actualEvent = serializer.fromBytes(ByteBuffer.wrap(serializedBytes));
    assertLoggingEventEquals(iLoggingEvent, actualEvent);
  }

  @Test
  public void testSerialization() throws Exception {
    Map<String, String> mdcMap = Maps.newHashMap();
    mdcMap.put("mdc1", "mdc-val1");
    mdcMap.put("mdc2", null);
    mdcMap.put(null, null);

    Map<String, String> contextMap = Maps.newHashMap();
    contextMap.put("p1", "ctx-val1");
    contextMap.put("p2", null);
    contextMap.put(null, null);

    LoggingEventSerializer serializer = new LoggingEventSerializer();
    ch.qos.logback.classic.spi.LoggingEvent iLoggingEvent = new ch.qos.logback.classic.spi.LoggingEvent();
    iLoggingEvent.setThreadName("threadName1");
    iLoggingEvent.setLevel(Level.INFO);
    iLoggingEvent.setMessage("Log message1");
    iLoggingEvent.setArgumentArray(new Object[]{null, "arg2", "100", null});
    iLoggingEvent.setLoggerName("loggerName1");

    iLoggingEvent.setLoggerContextRemoteView(new LoggerContextVO("logger_context1", contextMap, 12345634234L));

    Exception e1 = new Exception(null, null);
    Exception e2 = new Exception("Test Exception2", e1);
    iLoggingEvent.setThrowableProxy(new ThrowableProxy(e2));
    iLoggingEvent.prepareForDeferredProcessing();
    ((ThrowableProxy) iLoggingEvent.getThrowableProxy()).calculatePackagingData();

    iLoggingEvent.setCallerData(new StackTraceElement[]{
      new StackTraceElement("com.Class1", "methodName1", "fileName1", 10),
      null,
      new StackTraceElement("com.Class2", "methodName2", "fileName2", 20),
      new StackTraceElement("com.Class3",  "methodName3", null, 30),
      null
    });

    iLoggingEvent.setMarker(null);
    iLoggingEvent.getMDCPropertyMap().putAll(mdcMap);
    iLoggingEvent.setTimeStamp(1234567890L);

    // Serialize
    ILoggingEvent event = new LogMessage(iLoggingEvent, LoggingContextAccessor.getLoggingContext());
    byte [] serializedBytes = serializer.toBytes(event);

    // De-serialize
    ILoggingEvent actualEvent = serializer.fromBytes(ByteBuffer.wrap(serializedBytes));
    System.out.println(actualEvent);
    assertLoggingEventEquals(iLoggingEvent, actualEvent);
  }


  @Test
  public void testOldSystemLoggingContext() throws Exception {
    // see: CDAP-7482
    Map<String, String> mdcMap = new HashMap<>();
    mdcMap.put(ServiceLoggingContext.TAG_SYSTEM_ID, "ns1");
    mdcMap.put(ComponentLoggingContext.TAG_COMPONENT_ID, "comp1");
    mdcMap.put(ServiceLoggingContext.TAG_SERVICE_ID, "ser1");

    ch.qos.logback.classic.spi.LoggingEvent iLoggingEvent = new ch.qos.logback.classic.spi.LoggingEvent();
    iLoggingEvent.setCallerData(new StackTraceElement[] { null });
    iLoggingEvent.setMDCPropertyMap(mdcMap);

    Assert.assertTrue(LoggingContextHelper.getLoggingContext(iLoggingEvent.getMDCPropertyMap())
                        instanceof ServiceLoggingContext);
  }

  @Test
  public void testNullSerialization() throws Exception {
    Logger logger = LoggerFactory.getLogger(LoggingEventSerializerTest.class);
    LoggingEventSerializer serializer = new LoggingEventSerializer();
    ch.qos.logback.classic.spi.LoggingEvent iLoggingEvent = new ch.qos.logback.classic.spi.LoggingEvent(
      null, (ch.qos.logback.classic.Logger) logger, null, null, null, null);
    iLoggingEvent.setThreadName(null);
    iLoggingEvent.setLevel(null);
    iLoggingEvent.setMessage(null);
    iLoggingEvent.setArgumentArray(null);
    iLoggingEvent.setLoggerName(null);
    iLoggingEvent.setLoggerContextRemoteView(null);
    iLoggingEvent.setThrowableProxy(null);
    iLoggingEvent.setCallerData(null);
    iLoggingEvent.setMarker(null);
    iLoggingEvent.setMDCPropertyMap(null);
    iLoggingEvent.setTimeStamp(10000000L);

    // Serialize
    ILoggingEvent event = new LogMessage(iLoggingEvent, LoggingContextAccessor.getLoggingContext());
    byte [] serializedBytes = serializer.toBytes(event);

    // De-serialize
    ILoggingEvent actualEvent = serializer.fromBytes(ByteBuffer.wrap(serializedBytes));

    iLoggingEvent.setLevel(Level.ERROR);
    assertLoggingEventEquals(iLoggingEvent, actualEvent);
  }

  @Test
  public void testDecodeTimestamp() throws IOException {
    long timestamp = System.currentTimeMillis();

    ch.qos.logback.classic.spi.LoggingEvent event = new ch.qos.logback.classic.spi.LoggingEvent();
    event.setLevel(Level.INFO);
    event.setLoggerName("test.logger");
    event.setMessage("Some test");
    event.setTimeStamp(timestamp);

    // Serialize it
    LoggingEventSerializer serializer = new LoggingEventSerializer();
    byte[] bytes = serializer.toBytes(event);

    // Decode timestamp
    Assert.assertEquals(timestamp, serializer.decodeEventTimestamp(ByteBuffer.wrap(bytes)));
  }

  static void assertLoggingEventEquals(ILoggingEvent expected, ILoggingEvent actual) {
    expected.getMDCPropertyMap().putAll(
      ImmutableMap.of(".namespaceId", "TEST_ACCT_ID1", ".applicationId", "TEST_APP_ID1", ".runId", "RUN1",
                      ".instanceId", "INSTANCE1"));

    Assert.assertEquals(expected.getThreadName(), actual.getThreadName());
    Assert.assertEquals(expected.getLevel(), actual.getLevel());
    Assert.assertEquals(expected.getMessage(), actual.getMessage());
    Assert.assertArrayEquals(expected.getArgumentArray(), actual.getArgumentArray());
    Assert.assertEquals(expected.getFormattedMessage(), actual.getFormattedMessage());

    Assert.assertEquals(expected.getLoggerName(), actual.getLoggerName());
    Assert.assertEquals(expected.getLoggerContextVO(), actual.getLoggerContextVO());

    Assert.assertEquals(expected.hasCallerData(), actual.hasCallerData());
    if (expected.hasCallerData()) {
      Assert.assertArrayEquals(expected.getCallerData(), actual.getCallerData());
    }

    Assert.assertEquals(expected.getMarker(), actual.getMarker());
    Assert.assertEquals(expected.getTimeStamp(), actual.getTimeStamp());
    Assert.assertEquals(expected.getMDCPropertyMap(), actual.getMDCPropertyMap());

    assertThrowableProxyEquals(expected.getThrowableProxy(), actual.getThrowableProxy());
  }

  private static void assertThrowableProxyEquals(IThrowableProxy expected, IThrowableProxy actual) {
    if (expected == actual) {
      return;
    }

    assertThrowableProxyEquals(expected.getCause(), actual.getCause());
    Assert.assertEquals(expected.getClassName(), actual.getClassName());
    Assert.assertEquals(expected.getCommonFrames(), actual.getCommonFrames());
    Assert.assertEquals(expected.getMessage(), actual.getMessage());

    Assert.assertArrayEquals(expected.getStackTraceElementProxyArray(), actual.getStackTraceElementProxyArray());

    if (expected.getSuppressed() == actual.getSuppressed()) {
      return;
    }
    Assert.assertEquals(expected.getSuppressed().length, actual.getSuppressed().length);
    for (int i = 0; i < expected.getSuppressed().length; ++i) {
      assertThrowableProxyEquals(expected.getSuppressed()[i], actual.getSuppressed()[i]);
    }
  }
}
