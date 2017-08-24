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

package co.cask.cdap.messaging.store.cache;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.messaging.MessagingServiceUtils;
import co.cask.cdap.messaging.cache.MessageCache;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * The default implementation of {@link MessageTableCacheProvider}.
 */
public class DefaultMessageTableCacheProvider implements MessageTableCacheProvider {

  private final CConfiguration cConf;
  private final MetricsCollectionService metricsCollectionService;
  private Map<TopicId, MessageCache<MessageTable.Entry>> topicMessageCaches;
  private volatile boolean initialized;

  @Inject
  DefaultMessageTableCacheProvider(CConfiguration cConf, MetricsCollectionService metricsCollectionService) {
    // Due to circular dependency (see CoreMessagingService), we can't use the MetricsCollectionService in the
    // constructor, hence delay the cache initialization to later time.
    this.cConf = cConf;
    this.metricsCollectionService = metricsCollectionService;
  }


  @Nullable
  @Override
  public MessageCache<MessageTable.Entry> getMessageCache(TopicId topicId) {
    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          Map<TopicId, MessageCache<MessageTable.Entry>> caches = new HashMap<>();

          long cacheSize = cConf.getInt(Constants.MessagingSystem.CACHE_SIZE_MB) * 1024 * 1024;
          Set<TopicId> systemTopics = MessagingServiceUtils.getSystemTopics(cConf, true);
          if (cacheSize > 0 && !systemTopics.isEmpty()) {
            MessageTableEntryWeigher weigher = new MessageTableEntryWeigher();
            MessageTableEntryComparator comparator = new MessageTableEntryComparator();

            // Just evenly distributed the cache among all system topics.
            // More sophisticated logic can be employed at runtime to monitor the metrics from MessageCache
            // for each topic and adjust the soft/hard limit accordingly to maximize efficiency in
            // memory usage and performance
            long hardLimit = cacheSize / systemTopics.size();
            if (hardLimit > 0) {
              // Have reduce trigger as 70% of the hard limit and min retain as 50% of the hard limit
              // In future, it can be adjusted dynamically based on metrics
              MessageCache.Limits limits = new MessageCache.Limits(hardLimit / 2, hardLimit * 7 / 10, hardLimit);
              for (TopicId topic : systemTopics) {
                caches.put(topic, new MessageCache<>(comparator, weigher, limits,
                                                     createMetricsContext(cConf, topic, metricsCollectionService)));
              }
            }
          }

          topicMessageCaches = caches;
          initialized = true;
        }
      }
    }

    return topicMessageCaches.get(topicId);
  }

  @Override
  public void clear() {
    Collection<MessageCache<MessageTable.Entry>> caches;

    synchronized (this) {
      initialized = false;
      caches = topicMessageCaches == null ? null : topicMessageCaches.values();
      topicMessageCaches = null;
    }

    if (caches != null) {
      for (MessageCache<MessageTable.Entry> cache : caches) {
        cache.clear();
      }
    }
  }

  /**
   * Creates a {@link MetricsContext} for {@link MessageCache} to use for the given topic.
   */
  private MetricsContext createMetricsContext(CConfiguration cConf, TopicId topicId,
                                              MetricsCollectionService metricsCollectionService) {
    return metricsCollectionService.getContext(ImmutableMap.of(
      Constants.Metrics.Tag.COMPONENT, Constants.Service.MESSAGING_SERVICE,
      Constants.Metrics.Tag.INSTANCE_ID, cConf.get(Constants.MessagingSystem.CONTAINER_INSTANCE_ID, "0"),
      Constants.Metrics.Tag.NAMESPACE, topicId.getNamespace(),
      Constants.Metrics.Tag.TOPIC, topicId.getTopic()
    ));
  }
}
