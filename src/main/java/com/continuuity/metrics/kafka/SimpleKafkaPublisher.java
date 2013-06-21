/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.kafka;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

/**
 * Implementation of {@link KafkaPublisher} using the kafka scala-java api.
 */
public final class SimpleKafkaPublisher extends AbstractIdleService implements KafkaPublisher {

  private final String kafkaBrokers;
  private final Ack ack;
  private Producer<Integer, ByteBuffer> producer;

  public SimpleKafkaPublisher(String kafkaBrokers, Ack ack) {
    this.kafkaBrokers = kafkaBrokers;
    this.ack = ack;
  }

  @Override
  public Preparer prepare(String topic) {
    return new SimplePrepare(topic);
  }

  @Override
  protected void startUp() throws Exception {
    Properties props = new Properties();
    props.put("metadata.broker.list", kafkaBrokers);
    props.put("serializer.class", ByteBufferEncoder.class.getName());
    props.put("key.serializer.class", IntegerEncoder.class.getName());
    props.put("partitioner.class", IntegerPartitioner.class.getName());
    props.put("request.required.acks", Integer.toString(ack.getAck()));

    producer = new Producer<Integer, ByteBuffer>(new ProducerConfig(props));
  }

  @Override
  protected void shutDown() throws Exception {
    producer.close();
  }

  private final class SimplePrepare implements Preparer {

    private final String topic;
    private final List<KeyedMessage<Integer, ByteBuffer>> messages;

    private SimplePrepare(String topic) {
      this.topic = topic;
      this.messages = Lists.newLinkedList();
    }

    @Override
    public Preparer add(ByteBuffer message, Object partitionKey) {
      messages.add(new KeyedMessage<Integer, ByteBuffer>(topic, partitionKey.hashCode(), message));
      return this;
    }

    @Override
    public ListenableFuture<Integer> send() {
      Properties props = new Properties();
      props.put("metadata.broker.list", kafkaBrokers);
      props.put("serializer.class", ByteBufferEncoder.class.getName());
      props.put("key.serializer.class", IntegerEncoder.class.getName());
      props.put("partitioner.class", IntegerPartitioner.class.getName());
      props.put("request.required.acks", Integer.toString(ack.getAck()));

      int size = messages.size();
      producer.send(messages);

      messages.clear();
      return Futures.immediateFuture(size);
    }
  }
}
