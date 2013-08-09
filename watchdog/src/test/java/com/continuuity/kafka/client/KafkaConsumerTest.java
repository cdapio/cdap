/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.kafka.client;

import com.continuuity.internal.kafka.client.ZKKafkaClientService;
import com.continuuity.weave.common.Services;
import com.continuuity.weave.internal.kafka.EmbeddedKafkaServer;
import com.continuuity.weave.internal.utils.Networks;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class KafkaConsumerTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static InMemoryZKServer zkServer;

  @Test
  public void testStartSequence() throws InterruptedException, IOException {
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    KafkaClientService kafkaClientService = new ZKKafkaClientService(zkClient);

    Futures.getUnchecked(Services.chainStart(zkClient, kafkaClientService));
    try {
      // Semaphore for notifying message has been received.
      final Semaphore semaphore = new Semaphore(0);
      final BlockingQueue<String> receivedMessages = new LinkedBlockingQueue<String>();

      // Create a consumer before kafka server starts
      kafkaClientService.getConsumer().prepare()
                        .addFromBeginning("test", 0)
                        .consume(new KafkaConsumer.MessageCallback() {
        @Override
        public void onReceived(Iterator<FetchedMessage> messages) {
          Iterators.addAll(receivedMessages, Iterators.transform(messages, new Function<FetchedMessage, String>() {
            @Override
            public String apply(FetchedMessage input) {
              return Charsets.UTF_8.decode(input.getPayload()).toString();
            }
          }));
          semaphore.release();
        }

        @Override
        public void finished() {
          // No-op
        }
      });

      // Make sure nothing happen
      Assert.assertFalse(semaphore.tryAcquire(3, TimeUnit.SECONDS));

      // Start kafka server
      Properties kafkaConfig = generateKafkaConfig();
      EmbeddedKafkaServer kafkaServer = new EmbeddedKafkaServer(getClass().getClassLoader(), kafkaConfig);
      kafkaServer.startAndWait();

      // Publish a message
      kafkaClientService.getPublisher(KafkaPublisher.Ack.ALL_RECEIVED)
                        .prepare("test")
                        .add(Charsets.UTF_8.encode("Testing message 1"), 0)
                        .send();

      // Wait for message to arrive
      Assert.assertTrue(semaphore.tryAcquire(5, TimeUnit.SECONDS));
      Assert.assertEquals("Testing message 1", receivedMessages.poll());
      Assert.assertTrue(receivedMessages.isEmpty());

      // Restart the kafka server on different port
      kafkaServer.stopAndWait();
      kafkaConfig.setProperty("port", Integer.toString(Networks.getRandomPort()));
      kafkaServer = new EmbeddedKafkaServer(getClass().getClassLoader(), kafkaConfig);
      kafkaServer.startAndWait();

      // Publish another message
      kafkaClientService.getPublisher(KafkaPublisher.Ack.ALL_RECEIVED)
        .prepare("test")
        .add(Charsets.UTF_8.encode("Testing message 2"), 0)
        .send();

      // Wait for message to arrive
      Assert.assertTrue(semaphore.tryAcquire(10, TimeUnit.SECONDS));
      Assert.assertEquals("Testing message 2", receivedMessages.poll());
      Assert.assertTrue(receivedMessages.isEmpty());

    } finally {
      Futures.getUnchecked(Services.chainStop(kafkaClientService, zkClient));
    }
  }

  @BeforeClass
  public static void init() {
    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();
  }

  @AfterClass
  public static void finish() {
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
