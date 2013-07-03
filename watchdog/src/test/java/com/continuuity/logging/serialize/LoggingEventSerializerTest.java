/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.serialize;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.logging.logback.TestLoggingContext;
import com.continuuity.logging.appender.kafka.LoggingEventSerializer;
import kafka.utils.VerifiableProperties;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;

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
    LoggingEventSerializer serializer = new LoggingEventSerializer(new VerifiableProperties());
    ch.qos.logback.classic.spi.LoggingEvent iLoggingEvent = new ch.qos.logback.classic.spi.LoggingEvent();
    iLoggingEvent.setLevel(Level.INFO);
    iLoggingEvent.setLoggerName("loggerName1");
    iLoggingEvent.setMessage("Log message1");
    iLoggingEvent.setArgumentArray(new Object[]{"arg1", "arg2", "100"});
    iLoggingEvent.setThreadName("threadName1");
    iLoggingEvent.setTimeStamp(1234567890L);
    iLoggingEvent.setCallerData(new StackTraceElement[]{
      new StackTraceElement("com.Class1", "methodName1", "fileName1", 10),
      new StackTraceElement("com.Class2", "methodName2", "fileName2", 20),
    });
    Exception e1 = new Exception("Test Exception1");
    Exception e2 = new Exception("Test Exception2", e1);
    iLoggingEvent.setThrowableProxy(new ThrowableProxy(e2));
    iLoggingEvent.prepareForDeferredProcessing();
    ((ThrowableProxy) iLoggingEvent.getThrowableProxy()).calculatePackagingData();

    // Serialize
    LoggingEvent event = new LoggingEvent(iLoggingEvent);
    byte [] serializedBytes = serializer.toBytes(event);

    // De-serialize
    ILoggingEvent actualEvent = serializer.fromBytes(ByteBuffer.wrap(serializedBytes));
    System.out.println(actualEvent);
    assertLoggingEventEquals(iLoggingEvent, actualEvent);
  }

  public static boolean assertLoggingEventEquals(ILoggingEvent expected, ILoggingEvent actual) {
    if (!expected.getLevel().equals(actual.getLevel())) {
      return false;
    }
    if (notBothNull(expected.getLoggerName(), actual.getLoggerName()) &&
      !expected.getLoggerName().equals(actual.getLoggerName())) {
      return false;
    }
    if (notBothNull(expected.getMessage(), actual.getMessage()) &&
      !expected.getMessage().equals(actual.getMessage())) {
      return false;
    }
    Assert.assertArrayEquals(expected.getArgumentArray(), actual.getArgumentArray());
    if (!expected.getThreadName().equals(actual.getThreadName())) {
      return false;
    }
    if (expected.getTimeStamp() != actual.getTimeStamp()) {
      return false;
    }
    if (expected.hasCallerData() != actual.hasCallerData()) {
      return false;
    }
    if (expected.hasCallerData()) {
      Assert.assertArrayEquals(expected.getCallerData(), actual.getCallerData());
    }
    return !(expected.getThrowableProxy() != actual.getThrowableProxy() &&
      !expected.getThrowableProxy().equals(actual.getThrowableProxy()));
  }

  private static boolean notBothNull(Object obj1, Object obj2) {
    return !(obj1 == null && obj2 == null);
  }
}
