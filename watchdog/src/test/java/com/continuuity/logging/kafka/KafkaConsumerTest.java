package com.continuuity.logging.kafka;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.logging.KafkaTestBase;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.continuuity.logging.appender.kafka.KafkaLogAppender;
import com.continuuity.logging.context.FlowletLoggingContext;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import static com.continuuity.logging.LoggingConfiguration.KafkaHost;

/**
 * Test Kafka Consumer.
 */
public class KafkaConsumerTest extends KafkaTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerTest.class);

  @Test
  public void testNoTopic() throws Exception {
    List<LoggingConfiguration.KafkaHost> seedBrokers =
      Lists.newArrayList(new KafkaHost("localhost", getKafkaPort()));

    String topic = Long.toString(System.nanoTime());
    KafkaConsumer consumer = new KafkaConsumer(seedBrokers, topic, 0, 150);
    try {
      consumer.fetchOffset(KafkaConsumer.Offset.LATEST);
      Assert.fail();
    } catch (Exception e) {
      // Test passed
      LOG.info("Expected exception", e);
    }

    publishLogs();
    long offset = consumer.fetchOffset(KafkaConsumer.Offset.EARLIEST);
    Assert.assertEquals(0, offset);
  }

  @Test
  public void testNoTopicGraceful() throws Exception {
    List<LoggingConfiguration.KafkaHost> seedBrokers =
      Lists.newArrayList(new KafkaHost("localhost", getKafkaPort()));

    String topic = Long.toString(System.nanoTime());
    KafkaConsumer consumer = new KafkaConsumer(seedBrokers, topic, 0, 150);

    Assert.assertFalse(consumer.isLeaderPresent());

    publishLogs();

    Assert.assertTrue(consumer.isLeaderPresent());
    long offset = consumer.fetchOffset(KafkaConsumer.Offset.EARLIEST);
    Assert.assertEquals(0, offset);
  }

  private void publishLogs() {
    // Publish some logs
    LoggingContextAccessor.setLoggingContext(new FlowletLoggingContext("ACCT_1", "APP_1", "FLOW_1", "FLOWLET_1"));

    CConfiguration conf = CConfiguration.create();
    conf.set(LoggingConfiguration.KAFKA_SEED_BROKERS, "localhost:" + KafkaTestBase.getKafkaPort());
    conf.set(LoggingConfiguration.NUM_PARTITIONS, "1");
    conf.set(LoggingConfiguration.KAFKA_PRODUCER_TYPE, "async");
    KafkaLogAppender appender = new KafkaLogAppender(conf);
    new LogAppenderInitializer(appender).initialize("test_logger");

    Logger logger = LoggerFactory.getLogger("test_logger");
    logger.info("Test log");

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    StatusPrinter.setPrintStream(new PrintStream(bos));
    StatusPrinter.print((LoggerContext) LoggerFactory.getILoggerFactory());
    System.out.println(bos.toString());

    appender.stop();
  }
}
