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
package co.cask.cdap.metrics.collect;

import co.cask.cdap.common.io.BinaryEncoder;
import co.cask.cdap.common.io.Encoder;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.internal.io.DatumWriter;
import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.metrics.transport.MetricValue;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClient;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * A {@link AggregatedMetricsCollectionService} that publish {@link MetricValue} to kafka. The partition
 * is determined by the metric context.
 */
@Singleton
public final class KafkaMetricsCollectionService extends AggregatedMetricsCollectionService {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsCollectionService.class);

  private final KafkaClient kafkaClient;
  private final String topicPrefix;
  private final KafkaPublisher.Ack ack;
  private final DatumWriter<MetricValue> recordWriter;
  private final ByteArrayOutputStream encoderOutputStream;
  private final Encoder encoder;

  private KafkaPublisher publisher;

  @Inject
  public KafkaMetricsCollectionService(KafkaClient kafkaClient,
                                       @Named(MetricsConstants.ConfigKeys.KAFKA_TOPIC_PREFIX) String topicPrefix,
                                       DatumWriter<MetricValue> recordWriter) {
    this(kafkaClient, topicPrefix, KafkaPublisher.Ack.FIRE_AND_FORGET, recordWriter);
  }

  public KafkaMetricsCollectionService(KafkaClient kafkaClient, String topicPrefix,
                                       KafkaPublisher.Ack ack, DatumWriter<MetricValue> recordWriter) {
    this.kafkaClient = kafkaClient;
    this.topicPrefix = topicPrefix;
    this.ack = ack;
    this.recordWriter = recordWriter;

    // Parent guarantees the publish method would not get called concurrently, hence safe to reuse the same instances.
    this.encoderOutputStream = new ByteArrayOutputStream(1024);
    this.encoder = new BinaryEncoder(encoderOutputStream);
  }

  @Override
  protected void startUp() throws Exception {
    getPublisher();
  }

  @Override
  protected void publish(MetricsScope scope, Iterator<MetricValue> metrics) throws Exception {
    KafkaPublisher publisher = getPublisher();
    if (publisher == null) {
      LOG.warn("Unable to get kafka publisher, will not be able to publish metrics.");
      return;
    }
    encoderOutputStream.reset();

    KafkaPublisher.Preparer preparer = publisher.prepare(topicPrefix + "." + scope.name().toLowerCase());
    while (metrics.hasNext()) {
      // Encode each MetricRecord into bytes and make it an individual kafka message in a message set.
      MetricValue value = metrics.next();
      recordWriter.encode(value, encoder);
      // partitioning by the context
      preparer.add(ByteBuffer.wrap(encoderOutputStream.toByteArray()), getPartitionKey(value));
      encoderOutputStream.reset();
    }

    preparer.send();
  }

  private Integer getPartitionKey(MetricValue value) {
    // TODO: incredibly non-efficient: it is performed for each metrics data point,
    return value.getTags().hashCode();
  }

  private KafkaPublisher getPublisher() {
    if (publisher != null) {
      return publisher;
    }
    try {
      publisher = kafkaClient.getPublisher(ack, Compression.SNAPPY);
    } catch (IllegalStateException e) {
      // can happen if there are no kafka brokers because the kafka server is down.
      publisher = null;
    }
    return publisher;
  }
}
