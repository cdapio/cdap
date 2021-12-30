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
 * Provides {@link MetricsMetaKey} for {@link TopicId}
 */
public interface MetricsMetaKeyProvider {
  /**
   * Provide a {@link Map} with {@link TopicId} and
   * {@link MetricsMetaKey} implementation for the given list of {@link TopicId}.
   *
   * @param topics {@link List<TopicId>}
   * @return {@link Map<TopicId, MetricsMetaKey>}
   */
  Map<TopicId, MetricsMetaKey> getKeys(List<TopicId> topics);
}
