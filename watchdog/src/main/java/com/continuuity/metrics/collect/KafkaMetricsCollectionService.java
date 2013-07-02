/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.internal.io.DatumWriter;
import com.continuuity.kafka.client.KafkaPublisher;
import com.continuuity.kafka.client.SimpleKafkaPublisher;
import com.continuuity.metrics.transport.MetricRecord;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 *
 */
public final class KafkaMetricsCollectionService extends AggregatedMetricsCollectionService {

  private final String topic;
  private final SimpleKafkaPublisher publisher;
  private final DatumWriter<MetricRecord> recordWriter;
  private final ExposedByteArrayOutputStream encoderOutputStream;
  private final Encoder encoder;

  public KafkaMetricsCollectionService(String kafkaBrokers, String topic, DatumWriter<MetricRecord> recordWriter) {
    this(kafkaBrokers, topic, KafkaPublisher.Ack.FIRE_AND_FORGET, recordWriter);
  }

  public KafkaMetricsCollectionService(String kafkaBrokers, String topic,
                                       KafkaPublisher.Ack ack, DatumWriter<MetricRecord> recordWriter) {
    this.topic = topic;
    this.publisher = new SimpleKafkaPublisher(kafkaBrokers, ack);
    this.recordWriter = recordWriter;

    // Parent guarantees the publish method would not get called concurrently, hence safe to reuse the same instances.
    this.encoderOutputStream = new ExposedByteArrayOutputStream(1024);
    this.encoder = new BinaryEncoder(encoderOutputStream);
  }

  @Override
  protected void startUp() throws Exception {
    publisher.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    publisher.stopAndWait();
  }

  @Override
  protected void publish(Iterator<MetricRecord> metrics) throws Exception {
    encoderOutputStream.reset();

    KafkaPublisher.Preparer preparer = publisher.prepare(topic);
    while (metrics.hasNext()) {
      // Encode each MetricRecord into bytes and make it an individual kafka message in a message set.
      MetricRecord record = metrics.next();
      recordWriter.encode(record, encoder);
      preparer.add(ByteBuffer.wrap(encoderOutputStream.byteArray(), 0, encoderOutputStream.length()),
                   record.getContext());
      encoderOutputStream.reset();
    }

    preparer.send();
  }

  // Just to access the byte[] without introducing an unnecessary copy. Copied from guava.
  private static final class ExposedByteArrayOutputStream extends ByteArrayOutputStream {

    ExposedByteArrayOutputStream(int expectedInputSize) {
      super(expectedInputSize);
    }

    byte[] byteArray() {
      return buf;
    }

    int length() {
      return count;
    }
  }
}
