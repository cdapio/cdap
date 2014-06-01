/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.serialize;

import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.logging.logback.TestLoggingContext;
import com.continuuity.logging.appender.kafka.LoggingEventSerializer;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import ch.qos.logback.classic.spi.ThrowableProxy;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import kafka.utils.VerifiableProperties;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Test cases for LoggingEventSerializer.
 */
public class LoggingEventSerializerTest {
  @BeforeClass
  public static void setUpContext() {
    LoggingContextAccessor.setLoggingContext(new TestLoggingContext("TEST_ACCT_ID1", "TEST_APP_ID1"));
  }

  @Test
  public void testEmptySerialization() throws Exception {
    LoggingEventSerializer serializer = new LoggingEventSerializer(new VerifiableProperties());
    ch.qos.logback.classic.spi.LoggingEvent iLoggingEvent = new ch.qos.logback.classic.spi.LoggingEvent();
    iLoggingEvent.setLevel(Level.ERROR);
    iLoggingEvent.setThreadName("thread-1");
    iLoggingEvent.setLoggerName(getClass().getName());
    iLoggingEvent.setMessage("message");
    iLoggingEvent.setTimeStamp(10000000L);

    // Serialize
    LoggingEvent event = new LoggingEvent(iLoggingEvent);
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

    LoggingEventSerializer serializer = new LoggingEventSerializer(new VerifiableProperties());
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
    LoggingEvent event = new LoggingEvent(iLoggingEvent);
    byte [] serializedBytes = serializer.toBytes(event);

    // De-serialize
    ILoggingEvent actualEvent = serializer.fromBytes(ByteBuffer.wrap(serializedBytes));
    System.out.println(actualEvent);
    assertLoggingEventEquals(iLoggingEvent, actualEvent);
  }

  @Test
  public void testNullSerialization() throws Exception {
    LoggingEventSerializer serializer = new LoggingEventSerializer(new VerifiableProperties());
    ch.qos.logback.classic.spi.LoggingEvent iLoggingEvent = new ch.qos.logback.classic.spi.LoggingEvent();
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
    LoggingEvent event = new LoggingEvent(iLoggingEvent);
    byte [] serializedBytes = serializer.toBytes(event);

    // De-serialize
    ILoggingEvent actualEvent = serializer.fromBytes(ByteBuffer.wrap(serializedBytes));

    iLoggingEvent.setLevel(Level.ERROR);
    assertLoggingEventEquals(iLoggingEvent, actualEvent);
  }

  public static void assertLoggingEventEquals(ILoggingEvent expected, ILoggingEvent actual) {
    expected.getMDCPropertyMap().putAll(
      ImmutableMap.of(".accountId", "TEST_ACCT_ID1", ".applicationId", "TEST_APP_ID1"));

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

  public static void assertThrowableProxyEquals(IThrowableProxy expected, IThrowableProxy actual) {
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
