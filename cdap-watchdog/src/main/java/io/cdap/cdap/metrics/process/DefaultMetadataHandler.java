/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.metrics.process;

import io.cdap.cdap.proto.id.TopicId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Default implementation for {@link MetadataHandler}.
 * Loads/saves metadata from/to {@link MetricsConsumerMetaTable}
 */
public class DefaultMetadataHandler implements MetadataHandler {

  private final ConcurrentMap<MetricsMetaKey, TopicProcessMeta> topicProcessMetaMap;
  private MetricsConsumerMetaTable metaTable;
  private final String metricsPrefixForDelayMetrics;
  private final MetricsMetaKeyProvider keyProvider;

  public DefaultMetadataHandler(String metricsPrefixForDelayMetrics, MetricsMetaKeyProvider keyProvider) {
    this.topicProcessMetaMap = new ConcurrentHashMap<>();
    this.metricsPrefixForDelayMetrics = metricsPrefixForDelayMetrics;
    this.keyProvider = keyProvider;
  }

  @Override
  public void initCache(List<TopicId> metricsTopics, MetricsConsumerMetaTable metaTable) {
    this.metaTable = metaTable;
    Map<TopicId, MetricsMetaKey> keyMap = keyProvider.getKeys(metricsTopics);
    for (Map.Entry<TopicId, MetricsMetaKey> keyEntry : keyMap.entrySet()) {
      MetricsMetaKey topicIdMetaKey = keyEntry.getValue();
      TopicProcessMeta topicProcessMeta = metaTable.getTopicProcessMeta(topicIdMetaKey);
      if (topicProcessMeta == null || topicProcessMeta.getMessageId() == null) {
        continue;
      }
      String oldestTsMetricName = String.format("%s.topic.%s.oldest.delay.ms",
                                                metricsPrefixForDelayMetrics, keyEntry.getKey());
      String latestTsMetricName = String.format("%s.topic.%s.latest.delay.ms",
                                                metricsPrefixForDelayMetrics, keyEntry.getKey());
      topicProcessMetaMap.put(topicIdMetaKey,
                              new TopicProcessMeta(topicProcessMeta.getMessageId(),
                                                   topicProcessMeta.getOldestMetricsTimestamp(),
                                                   topicProcessMeta.getLatestMetricsTimestamp(),
                                                   topicProcessMeta.getMessagesProcessed(),
                                                   topicProcessMeta.getLastProcessedTimestamp(),
                                                   oldestTsMetricName, latestTsMetricName));
    }
  }

  @Override
  public void updateCache(MetricsMetaKey key, TopicProcessMeta value) {
    topicProcessMetaMap.put(key, value);
  }

  @Override
  public void saveCache(Map<MetricsMetaKey, TopicProcessMeta> topicProcessMetaMap) {
    // topicProcessMetaMap can be empty if the current thread fetches nothing
    // while other threads keep fetching new metrics and haven't updated messageId's of the corresponding topics
    if (topicProcessMetaMap.isEmpty()) {
      return;
    }
    metaTable.saveMetricsProcessorStats(topicProcessMetaMap);
  }

  @Override
  public TopicProcessMeta getTopicProcessMeta(MetricsMetaKey key) {
    return topicProcessMetaMap.get(key);
  }

  @Override
  public Map<MetricsMetaKey, TopicProcessMeta> getCache() {
    //return a copy of current map
    return new HashMap<>(topicProcessMetaMap);
  }
}
