/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.process;

import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.internal.io.ByteBufferInputStream;
import com.continuuity.internal.io.DatumReader;
import com.continuuity.internal.io.Schema;
import com.continuuity.metrics.transport.MetricsRecord;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A {@link KafkaConsumer.MessageCallback} that decodes message into {@link MetricsRecord} and invoke
 * set of {@link MetricsProcessor}.
 */
public final class MetricsMessageCallback implements KafkaConsumer.MessageCallback {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsMessageCallback.class);

  private final MetricsScope scope;
  private final DatumReader<MetricsRecord> recordReader;
  private final Schema recordSchema;
  private final Set<MetricsProcessor> processors;
  private long recordProcessed;

  public MetricsMessageCallback(MetricsScope scope,
                                Set<MetricsProcessor> processors,
                                DatumReader<MetricsRecord> recordReader,
                                Schema recordSchema) {
    this.scope = scope;
    this.processors = processors;
    this.recordReader = recordReader;
    this.recordSchema = recordSchema;
  }

  @Override
  public void onReceived(Iterator<FetchedMessage> messages) {
    // Decode the metrics records.
    final ByteBufferInputStream is = new ByteBufferInputStream(null);
    List<MetricsRecord> records = ImmutableList.copyOf(
      Iterators.filter(Iterators.transform(messages, new Function<FetchedMessage, MetricsRecord>() {
      @Override
      public MetricsRecord apply(FetchedMessage input) {
        try {
          return recordReader.read(new BinaryDecoder(is.reset(input.getPayload())), recordSchema);
        } catch (IOException e) {
          LOG.warn("Failed to decode message to MetricsRecord. Skipped. {}", e.getMessage(), e);
          return null;
        }
      }
    }), Predicates.notNull()));

    if (records.isEmpty()) {
      LOG.info("No records to process.");
      return;
    }
    // Invoke processors one by one.
    for (MetricsProcessor processor : processors) {
      processor.process(scope, records.iterator());
    }

    recordProcessed += records.size();
    if (recordProcessed % 1000 == 0) {
      LOG.info("{} metrics of {} records processed", scope, recordProcessed);
      LOG.info("Last record time: {}", records.get(records.size() - 1).getTimestamp());
    }
  }

  @Override
  public void finished() {
    // Just log
    LOG.info("Metrics MessageCallback completed.");
  }
}
