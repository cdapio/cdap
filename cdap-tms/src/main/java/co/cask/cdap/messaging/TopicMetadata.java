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

package co.cask.cdap.messaging;

import co.cask.cdap.proto.id.TopicId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents metadata about a messaging topic.
 */
public class TopicMetadata {

  public static final String TTL_KEY = "ttl";

  private final TopicId topicId;
  private final Map<String, String> properties;
  private final boolean validated;

  /**
   * Creates a new instance for the given topic with the associated properties.
   */
  public TopicMetadata(TopicId topicId, Map<String, String> properties) {
    this(topicId, properties, false);
  }

  /**
   * Creates a new instance for the given topic with the associated properties.
   * The properties provided can optionally be validated to see if it contains valid
   * values for all required properties.
   *
   * @throws IllegalArgumentException if {@code validate} is {@code true} and the provided properties is not valid.
   */
  public TopicMetadata(TopicId topicId, Map<String, String> properties, boolean validate) {
    this.topicId = topicId;
    this.properties = ImmutableMap.copyOf(properties);
    if (validate) {
      validateProperties();
    }
    this.validated = validate;
  }

  /**
   * Creates a new instance for the given topic with the associated properties.
   *
   * @param topicId topic id
   * @param properties a list of key/value pairs that will get converted into a {@link Map}.
   */
  @VisibleForTesting
  public TopicMetadata(TopicId topicId, Object...properties) {
    this(topicId, toMap(properties));
  }

  /**
   * Returns the topic id that this metadata is associated with.
   */
  public TopicId getTopicId() {
    return topicId;
  }

  /**
   * Returns the raw properties for the topic.
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * Returns the time-to-live in seconds property of the topic.
   */
  public long getTTL() {
    if (!validated) {
      validateTTL();
    }
    return Integer.parseInt(properties.get(TTL_KEY));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TopicMetadata that = (TopicMetadata) o;
    return Objects.equals(topicId, that.topicId) && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicId, properties);
  }

  @Override
  public String toString() {
    return "TopicMetadata{" +
      "topicId=" + topicId +
      ", properties=" + properties +
      '}';
  }

  /**
   * Validates all the required properties of the given topic.
   *
   * @throws IllegalArgumentException if any required properties is missing or having invalid values
   */
  private void validateProperties() {
    validateTTL();
  }

  /**
   * Validates the "ttl" property of the given topic.
   *
   * @throws IllegalArgumentException if the ttl value is missing, not a number or <= 0.
   */
  private void validateTTL() {
    String ttl = properties.get(TTL_KEY);
    if (ttl == null) {
      throw new IllegalArgumentException("Missing ttl property from the metadata of topic " + topicId);
    }
    try {
      if (Integer.parseInt(ttl) <= 0) {
        throw new IllegalArgumentException("The ttl property must be greater than zero for topic " + topicId);
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("The ttl property must be a number greater than zero for topic " + topicId, e);
    }
  }

  /**
   * Turns a list of {@link Object} into a {@link Map} by using even index objects as keys and the following odd index
   * objects as values. The {@link Object#toString()} method will be used to convert {@link Object} to {@link String}.
   */
  private static Map<String, String> toMap(Object...properties) {
    if (properties.length % 2 != 0) {
      throw new IllegalArgumentException("The properties size should be even as it should contain key-value pairs");
    }

    Map<String, String> map = new HashMap<>();
    for (int i = 0; i < properties.length; i += 2) {
      map.put(properties[i].toString(), properties[i + 1].toString());
    }
    return map;
  }
}
