/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.internal.io.ASMDatumWriterFactory;
import com.continuuity.internal.io.ASMFieldAccessorFactory;
import com.continuuity.internal.io.ByteBufferInputStream;
import com.continuuity.internal.io.DatumWriter;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.continuuity.internal.kafka.client.ZKKafkaClientService;
import com.continuuity.kafka.client.FetchedMessage;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.kafka.client.KafkaConsumer;
import com.continuuity.metrics.transport.MetricsRecord;
import com.continuuity.weave.internal.kafka.EmbeddedKafkaServer;
import com.continuuity.weave.internal.utils.Networks;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class KafkaMetricsCollectionServiceTest {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsCollectionServiceTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static InMemoryZKServer zkServer;
  private static EmbeddedKafkaServer kafkaServer;

  @Test
  public void testKafkaPublish() throws UnsupportedTypeException, InterruptedException {
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    KafkaClientService kafkaClient = new ZKKafkaClientService(zkClient);
    kafkaClient.startAndWait();

    final TypeToken<MetricsRecord> metricRecordType = TypeToken.of(MetricsRecord.class);
    final Schema schema = new ReflectionSchemaGenerator().generate(metricRecordType.getType());
    DatumWriter<MetricsRecord> metricRecordDatumWriter = new ASMDatumWriterFactory(new ASMFieldAccessorFactory())
      .create(metricRecordType, schema);

    MetricsCollectionService collectionService = new KafkaMetricsCollectionService(kafkaClient, "metrics",
                                                                                   metricRecordDatumWriter);
    collectionService.startAndWait();

    // publish metrics for different context
    for (int i = 1; i <= 3; i++) {
      collectionService.getCollector(MetricsScope.USER, "test.context." + i, "runId").gauge("processed", i);
    }

    // Sleep to make sure metrics get published
    TimeUnit.SECONDS.sleep(2);

    collectionService.stopAndWait();

    // Consumer from kafka
    final Map<String, MetricsRecord> metrics = Maps.newHashMap();
    final Semaphore semaphore = new Semaphore(0);
    kafkaClient.getConsumer().prepare().addFromBeginning("metrics." + MetricsScope.USER.name().toLowerCase(), 0)
                                       .consume(new KafkaConsumer.MessageCallback() {

      ReflectionDatumReader<MetricsRecord> reader = new ReflectionDatumReader<MetricsRecord>(schema, metricRecordType);

      @Override
      public void onReceived(Iterator<FetchedMessage> messages) {
        try {
          while (messages.hasNext()) {
            ByteBuffer payload = messages.next().getPayload();
            MetricsRecord metricsRecord = reader.read(new BinaryDecoder(new ByteBufferInputStream(payload)), schema);
            metrics.put(metricsRecord.getContext(), metricsRecord);
            semaphore.release();
          }
        } catch (Exception e) {
          LOG.error("Error in consume", e);
        }
      }

      @Override
      public void finished() {
        semaphore.release();
        LOG.info("Finished");
      }
    });

    Assert.assertTrue(semaphore.tryAcquire(3, 5, TimeUnit.SECONDS));

    for (int i = 1; i <= 3; i++) {
      Assert.assertEquals(i, metrics.get("test.context." + i).getValue());
    }

    kafkaClient.stopAndWait();

    // Finished on the callback should get called.
    Assert.assertTrue(semaphore.tryAcquire(1, 5, TimeUnit.SECONDS));
  }

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    Properties kafkaConfig = generateKafkaConfig();
    kafkaServer = new EmbeddedKafkaServer(KafkaMetricsCollectionServiceTest.class.getClassLoader(), kafkaConfig);
    kafkaServer.startAndWait();
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
}
