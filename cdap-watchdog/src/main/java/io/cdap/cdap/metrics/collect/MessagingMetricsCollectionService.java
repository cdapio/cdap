/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.metrics.collect;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.BinaryEncoder;
import io.cdap.cdap.common.io.DatumWriter;
import io.cdap.cdap.common.io.Encoder;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An {@link AggregatedMetricsCollectionService} that uses TMS to publish {@link io.cdap.cdap.api.metrics.MetricValues}.
 */
@Singleton
public class MessagingMetricsCollectionService extends AggregatedMetricsCollectionService {
  private static final Logger LOG = LoggerFactory.getLogger(MessagingMetricsCollectionService.class);
  private static final Joiner.MapJoiner MAP_JOINER = Joiner.on(',').withKeyValueSeparator("=");

  private final MessagingService messagingService;
  private final DatumWriter<MetricValues> recordWriter;
  private final ByteArrayOutputStream encoderOutputStream;
  private final Encoder encoder;
  private final Map<Integer, TopicPayload> topicPayloads;

  @Inject
  MessagingMetricsCollectionService(CConfiguration cConf,
                                    MessagingService messagingService,
                                    DatumWriter<MetricValues> recordWriter) {
    super(TimeUnit.SECONDS.toMillis(cConf.getInt(Constants.Metrics.METRICS_MINIMUM_RESOLUTION_SECONDS)));

    String topicPrefix = cConf.get(Constants.Metrics.TOPIC_PREFIX);
    int totalTopicNum = cConf.getInt(Constants.Metrics.MESSAGING_TOPIC_NUM);

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
      TopicPayload topicPayload = topicPayloads.get(Math.abs(metricValues.getTags().hashCode() % size));
      // Calculate the topic number with the hashcode of MetricValues' tags and store the encoded payload in the
      // corresponding list of the topic number
      topicPayload.addPayload(encoderOutputStream.toByteArray(), metricValues.getTags(),
                              metricValues.getMetrics().size());
    }
    publishMetric(topicPayloads.values());
  }

  private void publishMetric(Iterable<TopicPayload> topicPayloads) throws IOException, UnauthorizedException {
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
    private int payloadSize;
    private int metricsCount;
    private Map<String, String> metricsTags;


    private TopicPayload(TopicId topicId, RetryStrategy retryStrategy) {
      this.topicId = topicId;
      this.retryStrategy = retryStrategy;
      this.payloads = new ArrayList<>();
      this.payloadSize = 0;
      this.metricsCount = 0;
    }

    void addPayload(byte[] payload, Map<String, String> metricsTags, int metricsCount) {
      payloadSize += payload.length;
      if (this.metricsTags == null) {
        this.metricsTags = metricsTags;
      }
      this.metricsCount += metricsCount;
      payloads.add(payload);
    }

    void publish(MessagingService messagingService) throws IOException, UnauthorizedException {
      if (payloads.isEmpty()) {
        return;
      }

      int failureCount = 0;
      long startTime = -1L;
      boolean done = false;
      boolean interrupted = false;
      while (!done) {
        try {
          // Clear the thread interrupt flag when doing the actual publish.
          // Otherwise publish might get interrupted during shutdown, which has the thread interrupted
          interrupted = Thread.interrupted();
          messagingService.publish(StoreRequestBuilder.of(topicId).addPayloads(payloads).build());
          reset();
          done = true;
        } catch (TopicNotFoundException | ServiceUnavailableException e) {
          // These exceptions are retryable due to TMS not completely started
          if (startTime < 0) {
            startTime = System.currentTimeMillis();
          }
          long retryMillis = getRetryStrategy().nextRetry(++failureCount, startTime);
          if (retryMillis < 0) {
            throw new IOException("Failed to publish metrics to TMS and exceeded retry limit.", e);
          }
          LOG.debug("Failed to publish metrics to TMS due to {}. Will be retried in {} ms.",
                    e.getMessage(), retryMillis);
          if (interrupted) {
            LOG.warn("Retry of publish metrics interrupted. There will be loss of metrics.");
            done = true;
          } else {
            try {
              TimeUnit.MILLISECONDS.sleep(retryMillis);
            } catch (InterruptedException e1) {
              // Something explicitly stopping this thread. Simply just break and reset the interrupt flag.
              Thread.currentThread().interrupt();
              done = true;
            }
          }
        } catch (IOException ioe) {
          String exceptionMessage =
            String.format("Exception while publishing metrics for tags: [%s] to topic '%s' " +
                            "with %s metrics and %s bytes payload",
                          MAP_JOINER.join(metricsTags == null ? Collections.emptyMap() : metricsTags),
                          topicId.getTopic(), metricsCount, payloadSize);
          throw new IOException(exceptionMessage, ioe);
        }
      }

      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }

    private void reset() {
      // clear payloads and reset stats
      payloads.clear();
      payloadSize = 0;
      metricsCount = 0;
      metricsTags = null;
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
