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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.BinaryEncoder;
import co.cask.cdap.common.io.Encoder;
import co.cask.cdap.internal.io.DatumWriter;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * An {@link AggregatedMetricsCollectionService} that uses TMS to publish {@link co.cask.cdap.api.metrics.MetricValues}.
 */
@Singleton
public class MessagingMetricsCollectionService extends AggregatedMetricsCollectionService {

  private final MessagingService messagingService;
  private final List<TopicId> metricsTopics;
  private final int totalTopicNum;
  private final DatumWriter<MetricValues> recordWriter;
  private final ByteArrayOutputStream encoderOutputStream;
  private final Encoder encoder;
  private final List<List<byte[]>> payloadsList;

  @Inject
  public MessagingMetricsCollectionService(@Named(Constants.Metrics.TOPIC_PREFIX) String topicPrefix,
                                           @Named(Constants.Metrics.MESSAGING_TOPIC_NUM) int totalTopicNum,
                                           MessagingService messagingService, DatumWriter<MetricValues> recordWriter) {
    Preconditions.checkArgument(totalTopicNum > 0, "Constants.Metrics.MESSAGING_TOPIC_NUM must be a positive integer");
    this.messagingService = messagingService;
    this.recordWriter = recordWriter;

    // Parent guarantees the publish method would not get called concurrently, hence safe to reuse the same instances.
    this.encoderOutputStream = new ByteArrayOutputStream(1024);
    this.encoder = new BinaryEncoder(encoderOutputStream);
    this.metricsTopics = new ArrayList<>(totalTopicNum);
    for (int i = 0; i < totalTopicNum; i++) {
      this.metricsTopics.add(NamespaceId.SYSTEM.topic(topicPrefix + i));
    }
    this.totalTopicNum = totalTopicNum;
    this.payloadsList = new ArrayList<>(totalTopicNum);
    for (int i = 0; i < totalTopicNum; i++) {
      payloadsList.add(new ArrayList<byte[]>());
    }
  }

  @Override
  protected void publish(Iterator<MetricValues> metrics) throws Exception {
    while (metrics.hasNext()) {
      encoderOutputStream.reset();
      MetricValues metricValues = metrics.next();
      // Encode MetricValues into bytes
      recordWriter.encode(metricValues, encoder);
      // Calculate the topic number with the hashcode of MetricValues' tags and store the encoded payload in the
      // corresponding list of the topic number
      payloadsList.get(Math.abs(metricValues.getTags().hashCode() % totalTopicNum))
        .add(encoderOutputStream.toByteArray());
    }
    publishMetric(payloadsList);
  }

  private void publishMetric(List<List<byte[]>> payloadsList) throws IOException, TopicNotFoundException {
    for (int topicNum = 0; topicNum < totalTopicNum; topicNum++) {
      List<byte[]> payloads = payloadsList.get(topicNum);
      if (!payloads.isEmpty()) {
        messagingService.publish(StoreRequestBuilder.of(metricsTopics.get(topicNum))
                                   .addPayloads(payloads.iterator()).build());
      }
      payloads.clear();
    }
  }
}
