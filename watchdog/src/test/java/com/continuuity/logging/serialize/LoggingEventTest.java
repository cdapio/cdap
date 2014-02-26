package com.continuuity.logging.serialize;

import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.logging.logback.TestLoggingContext;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggerContextVO;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

/**
 * Test cases for LoggingEvent.
 */
public class LoggingEventTest {

  @BeforeClass
  public static void setUpContext() {
    LoggingContextAccessor.setLoggingContext(new TestLoggingContext("TEST_ACCT_ID1", "TEST_APP_ID1"));
  }

  @Test
  public void testSerialize() throws Exception {
    ch.qos.logback.classic.spi.LoggingEvent iLoggingEvent = new ch.qos.logback.classic.spi.LoggingEvent();
    iLoggingEvent.setLevel(Level.ERROR);
    iLoggingEvent.setLoggerName("loggerName1");
    iLoggingEvent.setMessage("Log message1");
    iLoggingEvent.setArgumentArray(new Object[] {"arg1", "arg2", "100"});
    iLoggingEvent.setThreadName("threadName1");
    iLoggingEvent.setTimeStamp(1234567890L);
    iLoggingEvent.setLoggerContextRemoteView(new LoggerContextVO("loggerContextRemoteView",
                                                                 ImmutableMap.of("key1", "value1", "key2", "value2"),
                                                                 100000L));
    Map<String, String> mdcMap = Maps.newHashMap(ImmutableMap.of("mdck1", "mdcv1", "mdck2", "mdck2"));
    iLoggingEvent.setMDCPropertyMap(mdcMap);

    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/logging/schema/LoggingEvent.avsc"));
    GenericRecord datum = LoggingEvent.encode(schema, iLoggingEvent);

    LoggingEvent actualEvent = new LoggingEvent(LoggingEvent.decode(datum));
    LoggingEventSerializerTest.assertLoggingEventEquals(iLoggingEvent, actualEvent);
  }

  @Test
  public void testEmptySerialize() throws Exception {
    ch.qos.logback.classic.spi.LoggingEvent iLoggingEvent = new ch.qos.logback.classic.spi.LoggingEvent();
    iLoggingEvent.setLevel(Level.ERROR);

    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/logging/schema/LoggingEvent.avsc"));
    GenericRecord datum = LoggingEvent.encode(schema, iLoggingEvent);

    LoggingEvent actualEvent = new LoggingEvent(LoggingEvent.decode(datum));
    LoggingEventSerializerTest.assertLoggingEventEquals(iLoggingEvent, actualEvent);
  }
}
