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

package io.cdap.cdap.api.messaging;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.security.AccessException;

import java.io.IOException;
import java.util.Map;

/**
 * Provides topic administration functions of the Transactional Messaging System.
 */
@Beta
public interface MessagingAdmin {

  /**
   * Creates a new topic with the default topic properties.
   *
   * @param topic name of the topic
   * @throws IOException if there was a failure to communicate with the messaging system. Caller may retry this call or
   *                     use the {@link #getTopicProperties(String)} method to verify that the topic has been created.
   * @throws IllegalArgumentException if the topic name is invalid. A valid id should only contain alphanumeric
   *                                  characters, {@code _}, or {@code -}.
   * @throws TopicAlreadyExistsException if the topic already exists
   * @throws AccessException if caller do not have proper access to the topic
   */
  void createTopic(String topic) throws TopicAlreadyExistsException, IOException, AccessException;

  /**
   * Creates a new topic.
   *
   * @param topic name of the topic
   * @param properties properties for the topic
   * @throws IOException if there was a failure to communicate with the messaging system. Caller may retry this call or
   *                     use the {@link #getTopicProperties(String)} method to verify that the topic has been created.
   * @throws IllegalArgumentException if the topic name is invalid. A valid id should only contain alphanumeric
   *                                  characters, {@code _}, or {@code -}.
   * @throws TopicAlreadyExistsException if the topic already exists
   * @throws AccessException if caller do not have proper access to the topic
   */
  void createTopic(String topic, Map<String, String> properties)
    throws TopicAlreadyExistsException, IOException, AccessException;

  /**
   * Returns the properties of a topic.
   *
   * @param topic name of the topic
   * @return the topic properties
   * @throws IOException if failed to communicate with the messaging system.
   * @throws IllegalArgumentException if the topic name is invalid. A valid id should only contain alphanumeric
   *                                  characters, {@code _}, or {@code -}.
   * @throws TopicNotFoundException if the topic does not exist
   * @throws AccessException if caller do not have proper access to the topic
   */
  Map<String, String> getTopicProperties(String topic) throws TopicNotFoundException, IOException, AccessException;

  /**
   * Updates the properties of a topic.
   *
   * @param topic name of the topic
   * @param properties the topic properties. This will overwrite the existing properties.
   * @throws IOException if there was a failure to communicate with the messaging system. Caller may use
   *                     the {@link #getTopicProperties(String)} method to verify that the properties have been updated.
   * @throws IllegalArgumentException if the topic name is invalid. A valid id should only contain alphanumeric
   *                                  characters, {@code _}, or {@code -}.
   * @throws TopicNotFoundException if the topic does not exist
   * @throws AccessException if caller do not have proper access to the topic
   */
  void updateTopic(String topic, Map<String, String> properties)
    throws TopicNotFoundException, IOException, AccessException;

  /**
   * Deletes a topic.
   *
   * @param topic name of the topic
   * @throws IOException if there was a failure to communicate with the messaging system. Caller may retry this call or
   *                     use the {@link #getTopicProperties(String)} method to verify that the topic has been deleted.
   * @throws IllegalArgumentException if the topic name is invalid. A valid id should only contain alphanumeric
   *                                  characters, {@code _}, or {@code -}.
   * @throws TopicNotFoundException if the topic does not exist
   * @throws AccessException if caller do not have proper access to the topic
   */
  void deleteTopic(String topic) throws TopicNotFoundException, IOException, AccessException;
}
