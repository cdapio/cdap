/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package io.cdap.cdap.messaging.data;

import java.util.Objects;

/**
 * Uniquely identifies a messaging topic.
 */
// TODO use the TopicId in cdap-api
public class TopicId {
  private final String topic;
  private transient Integer hashCode;
  private transient byte[] idBytes;
  private final String namespace;

  public TopicId(String namespace, String topic) {
    if (topic == null) {
      throw new NullPointerException("Topic ID cannot be null.");
    }
    this.topic = topic;
    this.namespace = namespace;
  }

  public String getTopic() {
    return topic;
  }

  public String getNamespace() {
    return namespace;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    TopicId topicId = (TopicId) o;
    return Objects.equals(namespace, topicId.namespace) && Objects.equals(topic, topicId.topic);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace, topic);
    }
    return hashCode;
  }
}
