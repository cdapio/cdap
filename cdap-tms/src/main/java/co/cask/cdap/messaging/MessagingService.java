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

import co.cask.cdap.messaging.service.StoreRequest;
import co.cask.cdap.proto.id.TopicId;

import java.io.IOException;

/**
 * Defines the interactions with the core messaging system.
 *
 * @see <a href="https://wiki.cask.co/display/CE/Messaging">Design documentation</a>
 */
public interface MessagingService {

  /**
   * Creates a topic with the given metadata.
   *
   * @param topicMetadata topic to be created
   * @throws TopicAlreadyExistsException if the topic to be created already exist
   * @throws IOException if failed to create the topic
   */
  void createTopic(TopicMetadata topicMetadata) throws TopicAlreadyExistsException, IOException;

  /**
   * Updates the metadata of a topic.
   *
   * @param topicMetadata the topic metadata to be updated
   * @throws TopicNotFoundException if the topic doesn't exist
   * @throws IOException if failed to update the topic metadata
   */
  void updateTopic(TopicMetadata topicMetadata) throws TopicNotFoundException, IOException;

  /**
   * Deletes a topic
   *
   * @param topicId the topic to be deleted
   * @throws TopicNotFoundException if the topic doesn't exist
   * @throws IOException if failed to delete the topic
   */
  void deleteTopic(TopicId topicId) throws TopicNotFoundException, IOException;

  /**
   * Returns the metadata of the given topic.
   *
   * @param topicId message topic
   * @return the {@link TopicMetadata} of the given topic.
   * @throws TopicNotFoundException if the topic doesn't exist
   * @throws IOException if failed to retrieve topic metadata.
   */
  TopicMetadata getTopic(TopicId topicId) throws TopicNotFoundException, IOException;

  /**
   * Prepares to fetch messages from the given topic.
   *
   * @param topicId the topic to fetch message from
   * @return a {@link MessageFetcher} for setting up parameters for fetching messages from the messaging system
   * @throws TopicNotFoundException if the topic doesn't exist
   * @throws IOException if failed to fetch messages
   */
  MessageFetcher prepareFetch(TopicId topicId) throws TopicNotFoundException, IOException;

  /**
   * Publishes a list of messages to the messaging system.
   *
   * @param topicId topic to publish to
   * @param messages the {@link StoreRequest} containing messages to be published
   * @throws TopicNotFoundException if the topic doesn't exist
   * @throws IOException if failed to publish messages
   */
  MessageRollback publish(TopicId topicId, StoreRequest messages) throws TopicNotFoundException, IOException;

  /**
   * Stores a list of messages to the messaging system. It is for long / distributed transactional publishing use case.
   *
   * @param topicId topic to store under
   * @param messages the {@link StoreRequest} containing messages to be stored
   * @throws TopicNotFoundException if the topic doesn't exist
   * @throws IOException if failed to store messages
   */
  void storePayload(TopicId topicId, StoreRequest messages) throws TopicNotFoundException, IOException;

  /**
   * Rollbacks messages published to the given topic with the given transaction.
   *
   * @param topicId the topic where the messages were published under
   * @param rollbackInfo the {@link MessageRollback} as returned by the
   *                     {@link #publish(TopicId, StoreRequest)} call,
   *                     which contains information needed for the rollback
   * @throws TopicNotFoundException if the topic doesn't exist
   * @throws IOException if failed to rollback changes
   */
  void rollback(TopicId topicId, MessageRollback rollbackInfo) throws TopicNotFoundException, IOException;
}
