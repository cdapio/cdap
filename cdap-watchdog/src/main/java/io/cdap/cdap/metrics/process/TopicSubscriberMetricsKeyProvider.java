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
import java.util.stream.Collectors;

/**
 * TopicSubscriberMetricsKeyProvider which provides a {@link TopicSubscriberMetaKey} for each {@link TopicId}
 */
public class TopicSubscriberMetricsKeyProvider implements MetricsMetaKeyProvider {

  private final String subscriberId;

  TopicSubscriberMetricsKeyProvider(String subscriberId) {
    this.subscriberId = subscriberId;
  }

  @Override
  public Map<TopicId, MetricsMetaKey> getKeys(List<TopicId> topics) {
    return topics.stream()
      .collect(Collectors.toMap(topicId -> topicId, topicId -> new TopicSubscriberMetaKey(topicId, subscriberId)));
  }
}
