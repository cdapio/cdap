/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.kafka.client;

import com.continuuity.kafka.client.KafkaPublisher;
import com.continuuity.weave.common.Cancellable;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

/**
 * Implementation of {@link com.continuuity.kafka.client.KafkaPublisher} using the kafka scala-java api.
 */
final class SimpleKafkaPublisher implements KafkaPublisher {

  private final String kafkaBrokers;
  private final Ack ack;
  private Producer<Integer, ByteBuffer> producer;

  public SimpleKafkaPublisher(String kafkaBrokers, Ack ack) {
    this.kafkaBrokers = kafkaBrokers;
    this.ack = ack;
  }

  /**
   * Start the publisher. This method must be called before other methods. This method is only to be called
   * by KafkaClientService who own this object.
   * @return A Cancellable for closing this publish.
   */
  Cancellable start() {
    // It should return a Cancellable that is not holding any reference to this class.
    // It is for ZKKafkaClientService to close the producer when the publisher get garbage collected.
    Properties props = new Properties();
    props.put("metadata.broker.list", kafkaBrokers);
    props.put("serializer.class", ByteBufferEncoder.class.getName());
    props.put("key.serializer.class", IntegerEncoder.class.getName());
    props.put("partitioner.class", IntegerPartitioner.class.getName());
    props.put("request.required.acks", Integer.toString(ack.getAck()));
    props.put("compression.codec", "snappy");

    producer = new Producer<Integer, ByteBuffer>(new ProducerConfig(props));
    return new ProducerCancellable(producer);
  }

  @Override
  public Preparer prepare(String topic) {
    return new SimplePreparer(topic);
  }

  private final class SimplePreparer implements Preparer {

    private final String topic;
    private final List<KeyedMessage<Integer, ByteBuffer>> messages;

    private SimplePreparer(String topic) {
      this.topic = topic;
      this.messages = Lists.newLinkedList();
    }

    @Override
    public Preparer add(ByteBuffer message, Object partitionKey) {
      messages.add(new KeyedMessage<Integer, ByteBuffer>(topic, Math.abs(partitionKey.hashCode()), message));
      return this;
    }

    @Override
    public ListenableFuture<Integer> send() {
      int size = messages.size();
      producer.send(messages);

      messages.clear();
      return Futures.immediateFuture(size);
    }
  }

  private static final class ProducerCancellable implements Cancellable {
    private final Producer<Integer, ByteBuffer> producer;

    private ProducerCancellable(Producer<Integer, ByteBuffer> producer) {
      this.producer = producer;
    }

    @Override
    public void cancel() {
      producer.close();
    }
  }
}
