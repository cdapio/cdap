/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.BinaryEncoder;
import co.cask.cdap.common.io.Encoder;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.internal.io.DatumWriter;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An {@link AggregatedMetricsCollectionService} that uses TMS to publish {@link co.cask.cdap.api.metrics.MetricValues}.
 */
@Singleton
public class MessagingMetricsCollectionService extends AggregatedMetricsCollectionService {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingMetricsCollectionService.class);

  private final MessagingService messagingService;
  private final DatumWriter<MetricValues> recordWriter;
  private final ByteArrayOutputStream encoderOutputStream;
  private final Encoder encoder;
  private final Map<Integer, TopicPayload> topicPayloads;

  @Inject
  MessagingMetricsCollectionService(@Named(Constants.Metrics.TOPIC_PREFIX) String topicPrefix,
                                    @Named(Constants.Metrics.MESSAGING_TOPIC_NUM) int totalTopicNum,
                                    CConfiguration cConf,
                                    MessagingService messagingService,
                                    DatumWriter<MetricValues> recordWriter) {
    Preconditions.checkArgument(totalTopicNum > 0, "Constants.Metrics.MESSAGING_TOPIC_NUM must be a positive integer");
    this.messagingService = messagingService;
    this.recordWriter = recordWriter;

    // Parent guarantees the publish method would not get called concurrently, hence safe to reuse the same instances.
    this.encoderOutputStream = new ByteArrayOutputStream(1024);
    this.encoder = new BinaryEncoder(encoderOutputStream);

    RetryStrategy retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.metrics.");
    this.topicPayloads = new LinkedHashMap<>(totalTopicNum);
    for (int i = 0; i < totalTopicNum; i++) {
      topicPayloads.put(i, new TopicPayload(NamespaceId.SYSTEM.topic(topicPrefix + i), retryStrategy));
    }
  }

  @Override
  protected void publish(Iterator<MetricValues> metrics) throws Exception {
    int size = topicPayloads.size();
    while (metrics.hasNext()) {
      encoderOutputStream.reset();
      MetricValues metricValues = metrics.next();
      // Encode MetricValues into bytes
      recordWriter.encode(metricValues, encoder);
      // Calculate the topic number with the hashcode of MetricValues' tags and store the encoded payload in the
      // corresponding list of the topic number
      topicPayloads.get(Math.abs(metricValues.getTags().hashCode() % size))
        .addPayload(encoderOutputStream.toByteArray());
    }
    publishMetric(topicPayloads.values());
  }

  private void publishMetric(Iterable<TopicPayload> topicPayloads) throws IOException {
    for (TopicPayload topicPayload : topicPayloads) {
      topicPayload.publish(messagingService);
    }
  }

  /**
   * Private to carry payloads to be published to a topic.
   */
  private final class TopicPayload {
    private final TopicId topicId;
    private final List<byte[]> payloads;
    private final RetryStrategy retryStrategy;

    private TopicPayload(TopicId topicId, RetryStrategy retryStrategy) {
      this.topicId = topicId;
      this.retryStrategy = retryStrategy;
      this.payloads = new ArrayList<>();
    }

    void addPayload(byte[] payload) {
      payloads.add(payload);
    }

    void publish(MessagingService messagingService) throws IOException {
      if (payloads.isEmpty()) {
        return;
      }

      int failureCount = 0;
      long startTime = -1L;
      boolean done = false;
      while (!done) {
        try {
          messagingService.publish(StoreRequestBuilder.of(topicId).addPayloads(payloads.iterator()).build());
          payloads.clear();
          done = true;
        } catch (TopicNotFoundException | ServiceUnavailableException e) {
          // These exceptions are retryable due to TMS not completely started
          if (startTime < 0) {
            startTime = System.currentTimeMillis();
          }
          long retryMillis = getRetryStrategy().nextRetry(++failureCount, startTime);
          if (retryMillis < 0) {
            throw new IOException("Failed to publish messages to TMS and exceeded retry limit.", e);
          }
          LOG.debug("Failed to publish messages to TMS due to {}. Will be retried in {} ms.",
                    e.getMessage(), retryMillis);
          try {
            TimeUnit.MILLISECONDS.sleep(retryMillis);
          } catch (InterruptedException e1) {
            // Something explicitly stopping this thread. Simply just break and reset the interrupt flag.
            Thread.currentThread().interrupt();
            done = true;
          }
        }
      }
    }

    private RetryStrategy getRetryStrategy() {
      if (isRunning()) {
        return retryStrategy;
      }
      // If failure happen during shutdown, use a retry strategy that only retry fixed number of times
      return RetryStrategies.timeLimit(5, TimeUnit.SECONDS, RetryStrategies.fixDelay(200, TimeUnit.MILLISECONDS));
    }
  }
}
