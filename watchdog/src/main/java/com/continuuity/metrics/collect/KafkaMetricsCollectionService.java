/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.internal.io.DatumWriter;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.kafka.client.KafkaPublisher;
import com.continuuity.metrics.transport.MetricRecord;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 *
 */
public final class KafkaMetricsCollectionService extends AggregatedMetricsCollectionService {

  private final KafkaClientService kafkaClient;
  private final String topic;
  private final KafkaPublisher.Ack ack;
  private final DatumWriter<MetricRecord> recordWriter;
  private final ByteArrayOutputStream encoderOutputStream;
  private final Encoder encoder;

  private KafkaPublisher publisher;

  public KafkaMetricsCollectionService(KafkaClientService kafkaClient,
                                       String topic, DatumWriter<MetricRecord> recordWriter) {
    this(kafkaClient, topic, KafkaPublisher.Ack.FIRE_AND_FORGET, recordWriter);
  }

  public KafkaMetricsCollectionService(KafkaClientService kafkaClient, String topic,
                                       KafkaPublisher.Ack ack, DatumWriter<MetricRecord> recordWriter) {
    this.kafkaClient = kafkaClient;
    this.topic = topic;
    this.ack = ack;
    this.recordWriter = recordWriter;

    // Parent guarantees the publish method would not get called concurrently, hence safe to reuse the same instances.
    this.encoderOutputStream = new ByteArrayOutputStream(1024);
    this.encoder = new BinaryEncoder(encoderOutputStream);
  }

  @Override
  protected void startUp() throws Exception {
    publisher = kafkaClient.getPublisher(ack);
  }

  @Override
  protected void publish(Iterator<MetricRecord> metrics) throws Exception {
    encoderOutputStream.reset();

    KafkaPublisher.Preparer preparer = publisher.prepare(topic);
    while (metrics.hasNext()) {
      // Encode each MetricRecord into bytes and make it an individual kafka message in a message set.
      MetricRecord record = metrics.next();
      recordWriter.encode(record, encoder);
      preparer.add(ByteBuffer.wrap(encoderOutputStream.toByteArray()), record.getContext());
      encoderOutputStream.reset();
    }

    preparer.send();
  }
}
