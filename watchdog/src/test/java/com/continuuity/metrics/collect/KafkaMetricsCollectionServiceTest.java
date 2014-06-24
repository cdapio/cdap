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
import com.continuuity.metrics.transport.MetricsRecord;
import com.continuuity.test.SlowTests;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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
@Category(SlowTests.class)
public class KafkaMetricsCollectionServiceTest {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsCollectionServiceTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder1 = new TemporaryFolder();
  @ClassRule
  public static TemporaryFolder tmpFolder2 = new TemporaryFolder();

  private InMemoryZKServer zkServer;
  private EmbeddedKafkaServer kafkaServer;

  @Test
  public void testKafkaPublish() throws UnsupportedTypeException, InterruptedException, IOException {

    Properties kafkaConfig = generateKafkaConfig(tmpFolder1);
    kafkaServer = new EmbeddedKafkaServer(kafkaConfig);
    kafkaServer.startAndWait();

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

    assertMetricsFromKafka(kafkaClient, schema, metricRecordType,
                           ImmutableMap.of(
                             "test.context.1", 1,
                             "test.context.2", 2,
                             "test.context.3", 3));
  }

  @Test
  public void testRecoverFromStoppedKafkaServerAtStartUp() throws InterruptedException, UnsupportedTypeException,
    IOException {
    // start the metrics collection service
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

    // start the kafka server
    Properties kafkaConfig = generateKafkaConfig(tmpFolder2);
    kafkaServer = new EmbeddedKafkaServer(kafkaConfig);
    kafkaServer.startAndWait();

    // Sleep to make sure brokers get populated
    TimeUnit.SECONDS.sleep(5);

    // public a metric
    collectionService.getCollector(MetricsScope.USER, "test.context", "runId").gauge("metric", 5);

    // Sleep to make sure metrics get published
    TimeUnit.SECONDS.sleep(2);

    collectionService.stopAndWait();

    assertMetricsFromKafka(kafkaClient, schema, metricRecordType, ImmutableMap.of("test.context", 5));
  }

  private void assertMetricsFromKafka(KafkaClientService kafkaClient, final Schema schema,
                                      final TypeToken<MetricsRecord> metricRecordType,
                                      Map<String, Integer> expected) throws InterruptedException {

    // Consume from kafka
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

    Assert.assertTrue(semaphore.tryAcquire(expected.size(), 5, TimeUnit.SECONDS));

    Assert.assertEquals(expected.size(), metrics.size());
    for (Map.Entry<String, Integer> expectedEntry : expected.entrySet()) {
      Assert.assertEquals(expectedEntry.getValue().intValue(), metrics.get(expectedEntry.getKey()).getValue());
    }

    kafkaClient.stopAndWait();

    // Finished on the callback should get called.
    Assert.assertTrue(semaphore.tryAcquire(1, 5, TimeUnit.SECONDS));
  }

  @Before
  public void init() throws IOException {
    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();
  }

  @After
  public void finish() {
    kafkaServer.stopAndWait();
    zkServer.stopAndWait();
  }

  private Properties generateKafkaConfig(TemporaryFolder tmpFolder) throws IOException {
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
