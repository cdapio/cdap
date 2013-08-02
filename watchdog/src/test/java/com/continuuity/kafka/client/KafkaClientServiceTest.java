/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.kafka.client;

import com.continuuity.internal.kafka.client.ZKKafkaClientService;
import com.continuuity.metrics.collect.KafkaMetricsCollectionServiceTest;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.internal.kafka.EmbeddedKafkaServer;
import com.continuuity.weave.internal.utils.Networks;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class KafkaClientServiceTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static InMemoryZKServer zkServer;
  private static EmbeddedKafkaServer kafkaServer;

  @Test
  public void testClientService() throws InterruptedException, ExecutionException {
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    try {
      KafkaClientService kafkaClient = new ZKKafkaClientService(zkClient);
      kafkaClient.startAndWait();

      // Create a consumer
      final CountDownLatch finishLatch = new CountDownLatch(1);
      final BlockingQueue<String> queue = new LinkedBlockingQueue<String>();
      Cancellable cancellable = kafkaClient.getConsumer().prepare()
        .addFromBeginning("test", 1)
        .addFromBeginning("test", 2)
        .consume(new KafkaConsumer.MessageCallback() {
          @Override
          public void onReceived(Iterator<FetchedMessage> messages) {
            while (messages.hasNext()) {
              FetchedMessage message = messages.next();
              queue.offer(Charsets.UTF_8.decode(message.getPayload()).toString() +
                            " " + message.getTopicPartition().getPartition());
            }
          }

          @Override
          public void finished() {
            finishLatch.countDown();
          }
        });

      // Publish messages to partition 1
      KafkaPublisher publisher = kafkaClient.getPublisher(KafkaPublisher.Ack.LEADER_RECEIVED);
      for (int i = 0; i < 10; i++) {
        publisher.prepare("test").add(Charsets.UTF_8.encode("Testing " + i), 1).send().get();
      }
      // Publish messages to partition 2
      for (int i = 0; i < 10; i++) {
        publisher.prepare("test").add(Charsets.UTF_8.encode("Testing " + i), 2).send().get();
      }

      // Check to see if there are 20 items consumed.
      int count = 0;
      List<String> consumed = Lists.newArrayList();
      queue.drainTo(consumed);
      while (consumed.size() != 20 & count++ < 10) {
        TimeUnit.SECONDS.sleep(1);
        queue.drainTo(consumed);
      }

      cancellable.cancel();

      Assert.assertTrue(consumed.size() == 20);

      for (int i = 0; i < consumed.size(); i++) {
        Assert.assertTrue(consumed.get(i).startsWith("Testing " + (i % 10)));
      }

      // Publish one more message, this message shouldn't be received by the callback.
      publisher.prepare("test").add(Charsets.UTF_8.encode("Testing"), 1).send().get();

      Assert.assertNull(queue.poll(3, TimeUnit.SECONDS));

      kafkaClient.stopAndWait();
    } finally {
      zkClient.stopAndWait();
    }
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
