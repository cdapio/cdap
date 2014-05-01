package com.continuuity.logging;

import com.google.common.base.Preconditions;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Base test class that start up Kafka during at the beginning of the test, and stop Kafka when test is done.
 */
public abstract class KafkaTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaTestBase.class);

  private static InMemoryZKServer zkServer;
  private static EmbeddedKafkaServer kafkaServer;
  private static int kafkaPort;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void startKafka() throws IOException {
    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    Properties kafkaConfig = generateKafkaConfig();
    kafkaServer = new EmbeddedKafkaServer(kafkaConfig);
    kafkaServer.startAndWait();
    kafkaPort = Integer.valueOf(kafkaConfig.getProperty("port"));

    LOG.info("Started kafka server on port {}", kafkaPort);
  }

  public static int getKafkaPort() {
    return kafkaPort;
  }

  public static String getZkConnectString() {
    return zkServer.getConnectionStr();
  }

  @AfterClass
  public static void stopKafka() {
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
    prop.setProperty("num.partitions", "2");
    prop.setProperty("log.flush.interval.messages", "10000");
    prop.setProperty("log.flush.interval.ms", "1000");
    prop.setProperty("log.retention.hours", "1");
    prop.setProperty("log.segment.bytes", "536870912");
    prop.setProperty("log.cleanup.interval.mins", "1");
    prop.setProperty("zookeeper.connect", zkServer.getConnectionStr());
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");

    return prop;
  }
}
