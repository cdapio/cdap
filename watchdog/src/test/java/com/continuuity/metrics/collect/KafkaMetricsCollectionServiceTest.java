/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

import com.continuuity.internal.io.ASMDatumWriterFactory;
import com.continuuity.internal.io.ASMFieldAccessorFactory;
import com.continuuity.internal.io.DatumWriter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.continuuity.metrics.transport.MetricRecord;
import com.continuuity.weave.internal.kafka.EmbeddedKafkaServer;
import com.continuuity.weave.internal.utils.Networks;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class KafkaMetricsCollectionServiceTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static InMemoryZKServer zkServer;
  private static EmbeddedKafkaServer kafkaServer;
  private static String kafkaBrokers;

  @Test
  public void testKafkaPublish() throws UnsupportedTypeException, InterruptedException {
    TypeToken<MetricRecord> metricRecordType = TypeToken.of(MetricRecord.class);
    DatumWriter<MetricRecord> metricRecordDatumWriter = new ASMDatumWriterFactory(new ASMFieldAccessorFactory())
      .create(metricRecordType, new ReflectionSchemaGenerator().generate(metricRecordType.getType()));

    MetricsCollectionService collectionService = new KafkaMetricsCollectionService(kafkaBrokers,
                                                                                   "metrics", metricRecordDatumWriter);

    collectionService.startAndWait();

    for (int i = 0; i < 10; i++) {
      collectionService.getCollector("test.context." + i, "processed").gauge(i);
      collectionService.getCollector("test.context." + i + 1, "processed").gauge(i + 1);
      collectionService.getCollector("test.context." + i + 2, "processed").gauge(i + 2);
      TimeUnit.SECONDS.sleep(1);
    }

    collectionService.stopAndWait();
  }

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    Properties kafkaConfig = generateKafkaConfig();
    kafkaServer = new EmbeddedKafkaServer(KafkaMetricsCollectionServiceTest.class.getClassLoader(), kafkaConfig);
    kafkaServer.startAndWait();
    kafkaBrokers = "localhost:" + kafkaConfig.getProperty("port");
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
    prop.setProperty("num.partitions", "10");
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
