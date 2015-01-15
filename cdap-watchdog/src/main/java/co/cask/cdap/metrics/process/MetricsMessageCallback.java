/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.metrics.process;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.io.BinaryDecoder;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.internal.io.DatumReader;
import co.cask.cdap.metrics.transport.MetricValue;
import co.cask.cdap.metrics.transport.MetricsRecord;
import co.cask.common.io.ByteBufferInputStream;
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
  private final DatumReader<MetricValue> recordReader;
  private final Schema recordSchema;
  private final Set<MetricsProcessor> processors;
  private long recordProcessed;

  public MetricsMessageCallback(MetricsScope scope,
                                Set<MetricsProcessor> processors,
                                DatumReader<MetricValue> recordReader,
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
    List<MetricValue> records = ImmutableList.copyOf(
      Iterators.filter(Iterators.transform(messages, new Function<FetchedMessage, MetricValue>() {
      @Override
      public MetricValue apply(FetchedMessage input) {
        try {
          return recordReader.read(new BinaryDecoder(is.reset(input.getPayload())), recordSchema);
        } catch (IOException e) {
          LOG.info("Failed to decode message to MetricValue. Skipped. {}", e.getMessage());
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
      processor.process(scope, new MetricRecordsWrapper(records.iterator()));
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
