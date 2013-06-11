package com.continuuity.common.logging.logback.serialize;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.logging.logback.TestLoggingContext;
import com.continuuity.common.logging.logback.kafka.LoggingEventSerializer;
import kafka.utils.VerifiableProperties;
import org.junit.BeforeClass;
import org.junit.Test;

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
    ILoggingEvent actualEvent = serializer.fromBytes(serializedBytes);
    System.out.println(actualEvent);
  }

  @Test
  public void testSerialization() throws Exception {
    LoggingEventSerializer serializer = new LoggingEventSerializer(new VerifiableProperties());
    ch.qos.logback.classic.spi.LoggingEvent iLoggingEvent = new ch.qos.logback.classic.spi.LoggingEvent();
    iLoggingEvent.setLevel(Level.INFO);
    iLoggingEvent.setLoggerName("loggerName1");
    iLoggingEvent.setMessage("Log message1");
    iLoggingEvent.setArgumentArray(new Object[]{"arg1", "arg2", 100});
    iLoggingEvent.setThreadName("threadName1");
    iLoggingEvent.setTimeStamp(1234567890L);

    // Serialize
    LoggingEvent event = new LoggingEvent(iLoggingEvent);
    byte [] serializedBytes = serializer.toBytes(event);

    // De-serialize
    ILoggingEvent actualEvent = serializer.fromBytes(serializedBytes);
    System.out.println(actualEvent);
  }
}
