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

package co.cask.cdap.metrics.process;

import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.proto.id.TopicId;

import java.util.Arrays;

/**
 * Topic id meta key
 */
public final class TopicIdMetaKey implements MetricsMetaKey {

  private final TopicId topicId;
  private final byte[] key;

  public TopicIdMetaKey(TopicId topicId) {
    this.topicId = topicId;
    this.key = MessagingUtils.toMetadataRowKey(topicId);
  }

  @Override
  public byte[] getKey() {
    return key;
  }

  TopicId getTopicId() {
    return topicId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TopicIdMetaKey that = (TopicIdMetaKey) o;
    // Comparing the key is enough because key and topicId have one-to-one relationship
    return Arrays.equals(getKey(), that.getKey());
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(getKey());
  }
}
