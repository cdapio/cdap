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

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.proto.id.TopicId;

import java.util.Arrays;

/**
 * TopicSubscriberMetaKey provides an implementation of {@link MetricsMetaKey}
 * that uses {@link TopicId} and a {@link String} subscriberId
 */
public class TopicSubscriberMetaKey implements MetricsMetaKey {

  private static final String KEY_FORMAT = "topic:%s:%s:subscriber:%s";
  private static final String PRINT_FORMAT = "TopicSubscriberMetaKey{ key=%s }";

  private final byte[] key;

  TopicSubscriberMetaKey(TopicId topicId, String subscriberId) {
    String formattedKey = String.format(KEY_FORMAT, topicId.getNamespace(), topicId.getTopic(), subscriberId);
    this.key = Bytes.toBytes(formattedKey);
  }

  @Override
  public byte[] getKey() {
    return key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TopicSubscriberMetaKey)) {
      return false;
    }
    TopicSubscriberMetaKey that = (TopicSubscriberMetaKey) o;
    return Arrays.equals(key, that.key);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(key);
  }

  @Override
  public String toString() {
    return String.format(PRINT_FORMAT, Bytes.toString(key));
  }
}
