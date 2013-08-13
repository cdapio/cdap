package com.continuuity.logging.appender.kafka;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.continuuity.logging.context.FlowletLoggingContext;
import com.continuuity.logging.context.GenericLoggingContext;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.read.DistributedLogReader;
import com.continuuity.logging.read.LogEvent;
import com.continuuity.metrics.collect.KafkaMetricsCollectionServiceTest;
import com.continuuity.weave.internal.kafka.EmbeddedKafkaServer;
import com.continuuity.weave.internal.utils.Networks;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.google.common.base.Preconditions;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Properties;

import static com.continuuity.logging.appender.file.TestFileLogging.LogCallback;

public class TestKafkaLogging {
  private static InMemoryZKServer zkServer;
  private static EmbeddedKafkaServer kafkaServer;
  private static int kafkaPort;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    Properties kafkaConfig = generateKafkaConfig();
    kafkaServer = new EmbeddedKafkaServer(KafkaMetricsCollectionServiceTest.class.getClassLoader(), kafkaConfig);
    kafkaServer.startAndWait();
    kafkaPort = Integer.valueOf(kafkaConfig.getProperty("port"));

    LoggingContextAccessor.setLoggingContext(new FlowletLoggingContext("ACCT_1", "APP_1", "FLOW_1", "FLOWLET_1"));


    CConfiguration conf = CConfiguration.create();
    conf.set(LoggingConfiguration.KAFKA_SEED_BROKERS, "localhost:" + kafkaPort);
    conf.set(LoggingConfiguration.NUM_PARTITIONS, "1");
    conf.set(LoggingConfiguration.KAFKA_PRODUCER_TYPE, "async");
    KafkaLogAppender appender = new KafkaLogAppender(conf);
    new LogAppenderInitializer(appender).initialize();

    Logger logger = LoggerFactory.getLogger(TestKafkaLogging.class);
    Exception e1 = new Exception("Test Exception1");
    Exception e2 = new Exception("Test Exception2", e1);
    for (int i = 0; i < 60; ++i) {
      logger.warn("Test log message " + i + " {} {}", "arg1", "arg2", e2);
    }

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    StatusPrinter.setPrintStream(new PrintStream(bos));
    StatusPrinter.print((LoggerContext) LoggerFactory.getILoggerFactory());
    System.out.println(bos.toString());

    appender.stop();
  }

  @AfterClass
  public static void finish() {
    kafkaServer.stopAndWait();
    zkServer.stopAndWait();
  }

  private static Properties generateKafkaConfig() throws IOException {
    int port = Networks.getRandomPort();
    Preconditions.checkState(port > 0, "Failed to get random port.");

    Properties prop = new Properties();
    prop.setProperty("broker.id", "1");
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("num.network.threads", "2");
    prop.setProperty("num.io.threads", "2");
    prop.setProperty("socket.send.buffer.bytes", "1048576");
    prop.setProperty("socket.receive.buffer.bytes", "1048576");
    prop.setProperty("socket.request.max.bytes", "104857600");
    prop.setProperty("log.dir", tmpFolder.newFolder().getAbsolutePath());
    prop.setProperty("num.partitions", "1");
    prop.setProperty("log.flush.interval.messages", "10000");
    prop.setProperty("log.flush.interval.ms", "1000");
    prop.setProperty("log.retention.hours", "1");
    prop.setProperty("log.segment.bytes", "536870912");
    prop.setProperty("log.cleanup.interval.mins", "1");
    prop.setProperty("zookeeper.connect", zkServer.getConnectionStr());
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");

    return prop;
  }

  @Test
  public void testGetNext() throws Exception {
    CConfiguration conf = new CConfiguration();
    conf.set(LoggingConfiguration.KAFKA_SEED_BROKERS, "localhost:" + kafkaPort);
    conf.set(LoggingConfiguration.NUM_PARTITIONS, "1");
    conf.set(LoggingConfiguration.LOG_RUN_ACCOUNT, "ACCT_1");
    DistributedLogReader distributedLogReader = new DistributedLogReader(null, conf, new Configuration());

    LoggingContext loggingContext = new FlowletLoggingContext("ACCT_1", "APP_1", "FLOW_1", "");

    LogCallback logCallback1 = new LogCallback();
    distributedLogReader.getLogNext(loggingContext, -1, 10, Filter.EMPTY_FILTER, logCallback1);
    List<LogEvent> events = logCallback1.getEvents();
    Assert.assertEquals(10, events.size());
    Assert.assertEquals("Test log message 50 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(9).getLoggingEvent().getFormattedMessage());

    loggingContext = new GenericLoggingContext("ACCT_1", "APP_1", "FLOW_1");
    LogCallback logCallback2 = new LogCallback();
    distributedLogReader.getLogPrev(loggingContext, logCallback1.getFirstOffset(), 20, Filter.EMPTY_FILTER,
                                    logCallback2);
    events = logCallback2.getEvents();
    Assert.assertEquals(20, events.size());
    Assert.assertEquals("Test log message 30 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 49 arg1 arg2", events.get(19).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback3 = new LogCallback();
    distributedLogReader.getLogNext(loggingContext, logCallback2.getLastOffset(), 20, Filter.EMPTY_FILTER,
                                    logCallback3);
    events = logCallback3.getEvents();
    Assert.assertEquals(10, events.size());
    Assert.assertEquals("Test log message 50 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(9).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback4 = new LogCallback();
    distributedLogReader.getLogNext(loggingContext, logCallback2.getFirstOffset(), 20, Filter.EMPTY_FILTER,
                                    logCallback4);
    events = logCallback4.getEvents();
    Assert.assertEquals(20, events.size());
    Assert.assertEquals("Test log message 31 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 50 arg1 arg2", events.get(19).getLoggingEvent().getFormattedMessage());

    distributedLogReader.close();
  }
}
