/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.internal.io.DatumWriter;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.kafka.client.KafkaPublisher;
import com.continuuity.metrics.transport.MetricsRecord;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * A {@link AggregatedMetricsCollectionService} that publish {@link com.continuuity.metrics.transport.MetricsRecord} to kafka. The partition
 * is determined by the metric context.
 */
@Singleton
public final class KafkaMetricsCollectionService extends AggregatedMetricsCollectionService {

  private final KafkaClientService kafkaClient;
  private final String topic;
  private final KafkaPublisher.Ack ack;
  private final DatumWriter<MetricsRecord> recordWriter;
  private final ByteArrayOutputStream encoderOutputStream;
  private final Encoder encoder;

  private KafkaPublisher publisher;

  @Inject
  public KafkaMetricsCollectionService(KafkaClientService kafkaClient,
                                       @Named("metrics.kafka.topic") String topic,
                                       DatumWriter<MetricsRecord> recordWriter) {
    this(kafkaClient, topic, KafkaPublisher.Ack.FIRE_AND_FORGET, recordWriter);
  }

  public KafkaMetricsCollectionService(KafkaClientService kafkaClient, String topic,
                                       KafkaPublisher.Ack ack, DatumWriter<MetricsRecord> recordWriter) {
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
  protected void publish(Iterator<MetricsRecord> metrics) throws Exception {
    encoderOutputStream.reset();

    KafkaPublisher.Preparer preparer = publisher.prepare(topic);
    while (metrics.hasNext()) {
      // Encode each MetricRecord into bytes and make it an individual kafka message in a message set.
      MetricsRecord record = metrics.next();
      recordWriter.encode(record, encoder);
      preparer.add(ByteBuffer.wrap(encoderOutputStream.toByteArray()), record.getContext());
      encoderOutputStream.reset();
    }

    preparer.send();
  }
}
