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

package co.cask.cdap.api.messaging;

import co.cask.cdap.api.annotation.Beta;

import java.io.IOException;
import java.util.Map;

/**
 * Provides topic administration functions on the transactional messaging system.
 */
@Beta
public interface MessagingAdmin {

  /**
   * Creates a new topic with default topic properties.
   *
   * @param topic name of the topic
   * @throws IOException if failed to communicate with the messaging system. Caller may retry this call or
   *                     use the {@link #getTopicProperties(String)} method verify if the topic has been created.
   * @throws IllegalArgumentException if the topic name is invalid. A valid id should only contain alphanumeric
   *                                  characters and {@code _} or {@code -}.
   * @throws TopicAlreadyExistsException if the topic already exist.
   */
  void createTopic(String topic) throws TopicAlreadyExistsException, IOException;

  /**
   * Creates a new topic.
   *
   * @param topic name of the topic
   * @param properties properties for the topic
   * @throws IOException if failed to communicate with the messaging system. Caller may retry this call or
   *                     use the {@link #getTopicProperties(String)} method to verify if the topic has been created.
   * @throws IllegalArgumentException if the topic name is invalid. A valid id should only contain alphanumeric
   *                                  characters and {@code _} or {@code -}.
   * @throws TopicAlreadyExistsException if the topic already exist.
   */
  void createTopic(String topic, Map<String, String> properties) throws TopicAlreadyExistsException, IOException;

  /**
   * Gets the properties of the given topic
   *
   * @param topic name of the topic
   * @return the topic properties
   * @throws IOException if failed to communicate with the messaging system.
   * @throws IllegalArgumentException if the topic name is invalid. A valid id should only contain alphanumeric
   *                                  characters and {@code _} or {@code -}.
   * @throws TopicNotFoundException if the topic does not exist.
   */
  Map<String, String> getTopicProperties(String topic) throws TopicNotFoundException, IOException;

  /**
   * Updates the properties of the given topic.
   *
   * @param topic name of the topic
   * @param properties the topic properties. This will overwrite existing properties
   * @throws IOException if failed to communicate with the messaging system. Caller may use
   *                     the {@link #getTopicProperties(String)} method to verify if the properties has been updated.
   * @throws IllegalArgumentException if the topic name is invalid. A valid id should only contain alphanumeric
   *                                  characters and {@code _} or {@code -}.
   * @throws TopicNotFoundException if the topic does not exist.
   */
  void updateTopic(String topic, Map<String, String> properties) throws TopicNotFoundException, IOException;

  /**
   * Deletes the given topic
   *
   * @param topic name of the topic
   * @throws IOException if failed to communicate with the messaging system. Caller may retry this call or
   *                     use the {@link #getTopicProperties(String)} method to verify if the topic has been deleted.
   * @throws IllegalArgumentException if the topic name is invalid. A valid id should only contain alphanumeric
   *                                  characters and {@code _} or {@code -}.
   * @throws TopicNotFoundException if the topic does not exist.
   */
  void deleteTopic(String topic) throws TopicNotFoundException, IOException;
}
