/*
 * Copyright © 2019 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.context.MultiThreadMessagingContext;
import co.cask.cdap.messaging.subscriber.AbstractMessagingPollingService;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * A TMS subscriber service for consuming metrics administrative messages and performance admin operations on the
 * metrics system.
 */
public class MetricsAdminSubscriberService extends AbstractMessagingPollingService<MetricsAdminMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsAdminSubscriberService.class);
  private static final Gson GSON = new Gson();

  // Number of messages to fetch per batch
  private static final int FETCH_SIZE = 100;

  private final MessagingContext messagingContext;
  private final MetricDatasetFactory metricDatasetFactory;
  private final MetricStore metricStore;
  private MetricsConsumerMetaTable metaTable;

  @Inject
  MetricsAdminSubscriberService(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                                MessagingService messagingService, MetricDatasetFactory metricDatasetFactory,
                                MetricStore metricStore) {
    super(NamespaceId.SYSTEM.topic(cConf.get(Constants.Metrics.ADMIN_TOPIC)),
          metricsCollectionService.getContext(ImmutableMap.of(
            Constants.Metrics.Tag.COMPONENT, Constants.Service.METRICS,
            Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
            Constants.Metrics.Tag.TOPIC, cConf.get(Constants.Metrics.ADMIN_TOPIC),
            Constants.Metrics.Tag.CONSUMER, "metrics.admin"
          )),
          FETCH_SIZE, cConf.getLong(Constants.Metrics.ADMIN_POLL_DELAY_MILLIS),
          RetryStrategies.fromConfiguration(cConf, "system.metrics."));

    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.metricDatasetFactory = metricDatasetFactory;
    this.metricStore = metricStore;
  }

  @Override
  protected MessagingContext getMessagingContext() {
    return messagingContext;
  }

  @Override
  protected MetricsAdminMessage decodeMessage(Message message) {
    return GSON.fromJson(message.getPayloadAsString(), MetricsAdminMessage.class);
  }

  @Nullable
  @Override
  protected String loadMessageId() {
    MetricsConsumerMetaTable metaTable = getMetaTable();
    TopicProcessMeta meta = metaTable.getTopicProcessMeta(new TopicIdMetaKey(getTopicId()));
    if (meta == null) {
      return null;
    }
    return Bytes.toString(meta.getMessageId());
  }

  @Nullable
  @Override
  protected String processMessages(Iterator<ImmutablePair<String, MetricsAdminMessage>> messages) {
    MetricsConsumerMetaTable metaTable = getMetaTable();

    List<ImmutablePair<String, MetricsAdminMessage>> pendingMessages =
      StreamSupport.stream(Spliterators.spliteratorUnknownSize(messages, 0), false).collect(Collectors.toList());

    String messageId = null;
    for (ImmutablePair<String, MetricsAdminMessage> messagePair : pendingMessages) {
      MetricsAdminMessage message = messagePair.getSecond();

      switch (message.getType()) {
        case DELETE:
          metricStore.delete(message.getPayload(GSON, MetricDeleteQuery.class));
          break;
        default:
          LOG.warn("Ignore unsupported metrics admin message type: {}", message);
      }

      messageId = messagePair.getFirst();

      // Admin events are rare and typically each task takes a long time, hence we just persist the message ID
      // after processing each message.
      // We only store the message id and ignore other fields.
      TopicProcessMeta meta = new TopicProcessMeta(Bytes.toBytes(messageId), 0, 0, 0, 0);
      metaTable.saveMetricsProcessorStats(Collections.singletonMap(new TopicIdMetaKey(getTopicId()), meta));
    }

    return messageId;
  }

  /**
   * Returns the {@link MetricsConsumerMetaTable} for storing and retrieving consumer meta data.
   */
  private MetricsConsumerMetaTable getMetaTable() {
    while (metaTable == null) {
      if (state() != State.RUNNING) {
        LOG.info("We are shutting down, giving up on acquiring consumer metaTable.");
        break;
      }
      try {
        metaTable = metricDatasetFactory.createConsumerMeta();
      } catch (Exception e) {
        LOG.warn("Cannot access consumer metaTable, will retry in 1 sec.");
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    return metaTable;
  }
}
