package com.continuuity.common.logging.logback.serialize;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggerContextVO;
import ch.qos.logback.core.util.StatusPrinter;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.ApplicationLoggingContext;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.logging.logback.LogAppenderInitializer;
import com.continuuity.common.logging.logback.kafka.KafkaLogAppender;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Map;

public class LoggingEventTest {

  @Test @Ignore
  public void createLoggingEventSchema() {
    ReflectData reflectData = ReflectData.get();
    Schema schema = reflectData.getSchema(LoggingEvent.class);
    System.out.println(schema.toString(true));

    schema = reflectData.getSchema(StackTraceElement.class);
    System.out.println(schema.toString(true));
  }

  @Test
  public void testSerialize() throws Exception {
    ch.qos.logback.classic.spi.LoggingEvent iLoggingEvent = new ch.qos.logback.classic.spi.LoggingEvent();
    iLoggingEvent.setLevel(Level.ERROR);
    iLoggingEvent.setLoggerName("loggerName1");
    iLoggingEvent.setMessage("Log message1");
    iLoggingEvent.setArgumentArray(new Object[] {"arg1", "arg2", 100});
    iLoggingEvent.setThreadName("threadName1");
    iLoggingEvent.setTimeStamp(1234567890L);
    iLoggingEvent.setLoggerContextRemoteView(new LoggerContextVO("loggerContextRemoteView",
                                                                 ImmutableMap.of("key1", "value1", "key2", "value2"),
                                                                 100000L));
    iLoggingEvent.setMDCPropertyMap(ImmutableMap.of("mdck1", "mdcv1", "mdck2", "mdck2"));

    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/logging/schema/LoggingEvent.avsc"));
    GenericRecord datum = LoggingEvent.encode(schema, iLoggingEvent);

    System.out.println("GenericRecord = " + datum.toString());

    LoggingEvent expectedEvent = new LoggingEvent(LoggingEvent.decode(datum));
    System.out.println(expectedEvent);
    for (Map.Entry<String, String> entry : expectedEvent.getMDCPropertyMap().entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      System.out.println(key + "=" + value);
    }
  }

  @Test
  public void testEmptySerialize() throws Exception {
    ch.qos.logback.classic.spi.LoggingEvent iLoggingEvent = new ch.qos.logback.classic.spi.LoggingEvent();
    iLoggingEvent.setLevel(Level.ERROR);

    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/logging/schema/LoggingEvent.avsc"));
    GenericRecord datum = LoggingEvent.encode(schema, iLoggingEvent);

    LoggingEvent expectedEvent = new LoggingEvent(LoggingEvent.decode(datum));
    System.out.println(expectedEvent);
  }

  @Test @Ignore
  public void testPublish() throws Exception {
    CConfiguration conf = CConfiguration.create();
    new LogAppenderInitializer(conf, new KafkaLogAppender(conf));

    LoggingContextAccessor.setLoggingContext(new TestLoggingContext("ACCNT_ID_1", "APPL_ID_1"));

    Logger logger = LoggerFactory.getLogger(LoggingEventTest.class);
    logger.error("Test log message 1");
    logger.warn("Test log message 2 {} {}", "arg1", "arg2");
    logger.info("Test log message 3", "arg1");

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    StatusPrinter.setPrintStream(new PrintStream(bos));
    StatusPrinter.print((LoggerContext) LoggerFactory.getILoggerFactory());
    System.out.println(bos.toString());
  }

  static class TestLoggingContext extends ApplicationLoggingContext {
    public TestLoggingContext(String accountId, String applicationId) {
      super(accountId, applicationId);
    }

    @Override
    public String getLogPartition() {
      return super.getLogPartition();
    }
  }
}
