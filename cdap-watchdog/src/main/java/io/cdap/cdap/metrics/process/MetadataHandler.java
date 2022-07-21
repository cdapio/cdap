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

import java.util.List;
import java.util.Map;

/**
 * Interface for handling metrics metadata
 */
public interface MetadataHandler {

  /**
   * Initialize the cache (loading from existing store) for all the topics
   *
   * @param metricsTopics {@link List} of {@link TopicId}
   * @param metaTable {@link MetricsConsumerMetaTable} to read from
   */
  void initCache(List<TopicId> metricsTopics, MetricsConsumerMetaTable metaTable);

  /**
   * Update the cache with new value for the given key.
   *
   * @param key   {@link MetricsMetaKey}
   * @param value {@link TopicProcessMeta}
   */
  void updateCache(MetricsMetaKey key, TopicProcessMeta value);

  /**
   * Save the current cache (save to store)
   *
   * @param topicProcessMetaMap {@link Map} of {@link MetricsMetaKey} key and {@link TopicProcessMeta} values.
   */
  void saveCache(Map<MetricsMetaKey, TopicProcessMeta> topicProcessMetaMap);

  /**
   * Return value for the key from current cache
   *
   * @param key {@link MetricsMetaKey}
   * @return value {@link TopicProcessMeta}
   */
  TopicProcessMeta getTopicProcessMeta(MetricsMetaKey key);

  /**
   * Return a copy of the current cache
   *
   * @return {@link Map<MetricsMetaKey, TopicProcessMeta>}
   */
  Map<MetricsMetaKey, TopicProcessMeta> getCache();

}
