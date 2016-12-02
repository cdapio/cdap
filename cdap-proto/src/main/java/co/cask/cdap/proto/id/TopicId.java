/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.proto.id;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.element.EntityType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a messaging topic.
 */
public class TopicId extends NamespacedEntityId implements ParentedId<NamespaceId> {
  private final String topic;
  private transient Integer hashCode;
  private transient byte[] idBytes;

  public TopicId(String namespace, String topic) {
    super(namespace, EntityType.TOPIC);
    if (topic == null) {
      throw new NullPointerException("Topic ID cannot be null.");
    }
    ensureValidId("topic", topic);
    this.topic = topic;
  }

  public String getTopic() {
    return topic;
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, topic));
  }

  @SuppressWarnings("unused")
  public static TopicId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new TopicId(next(iterator, "namespace"), nextAndEnd(iterator, "topic"));
  }

  @Override
  public String getEntityName() {
    return getTopic();
  }

  @Override
  public Id toId() {
    throw new UnsupportedOperationException(
      String.format("%s does not have old %s class", TopicId.class.getName(), Id.class.getName()));
  }

  @Override
  public NamespaceId getParent() {
    return new NamespaceId(namespace);
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
