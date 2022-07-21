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

package io.cdap.cdap.messaging;

import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Main interface to interact with the transactional messaging system.
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
   * @throws ServiceUnavailableException if the messaging service is not available
   */
  void createTopic(TopicMetadata topicMetadata) throws TopicAlreadyExistsException, IOException, UnauthorizedException;

  /**
   * Updates the metadata of a topic.
   *
   * @param topicMetadata the topic metadata to be updated
   * @throws TopicNotFoundException if the topic doesn't exist
   * @throws IOException if failed to update the topic metadata
   * @throws ServiceUnavailableException if the messaging service is not available
   */
  void updateTopic(TopicMetadata topicMetadata) throws TopicNotFoundException, IOException, UnauthorizedException;

  /**
   * Deletes a topic
   *
   * @param topicId the topic to be deleted
   * @throws TopicNotFoundException if the topic doesn't exist
   * @throws IOException if failed to delete the topic
   * @throws ServiceUnavailableException if the messaging service is not available
   */
  void deleteTopic(TopicId topicId) throws TopicNotFoundException, IOException, UnauthorizedException;

  /**
   * Returns the metadata of the given topic.
   *
   * @param topicId message topic
   * @return the {@link TopicMetadata} of the given topic.
   * @throws TopicNotFoundException if the topic doesn't exist
   * @throws IOException if failed to retrieve topic metadata.
   * @throws ServiceUnavailableException if the messaging service is not available
   */
  TopicMetadata getTopic(TopicId topicId) throws TopicNotFoundException, IOException, UnauthorizedException;

  /**
   * Returns the list of topics available under the given namespace.
   *
   * @param namespaceId the namespace to list topics under it
   * @return a {@link List} of {@link TopicId}.
   * @throws IOException if failed to retrieve topics.
   * @throws ServiceUnavailableException if the messaging service is not available
   */
  List<TopicId> listTopics(NamespaceId namespaceId) throws IOException, UnauthorizedException;

  /**
   * Prepares to fetch messages from the given topic.
   *
   * @param topicId the topic to fetch message from
   * @return a {@link MessageFetcher} for setting up parameters for fetching messages from the messaging system
   * @throws TopicNotFoundException if the topic doesn't exist
   * @throws IOException if failed to fetch messages
   * @throws ServiceUnavailableException if the messaging service is not available
   */
  MessageFetcher prepareFetch(TopicId topicId) throws TopicNotFoundException, IOException;

  /**
   * Publishes a list of messages to the messaging system.
   *
   * @param request the {@link StoreRequest} containing messages to be published
   * @return if the store request is transactional, then returns a {@link RollbackDetail} containing
   *         information for rollback; otherwise {@code null} will be returned.
   * @throws TopicNotFoundException if the topic doesn't exist
   * @throws IOException if failed to publish messages
   * @throws ServiceUnavailableException if the messaging service is not available
   */
  @Nullable
  RollbackDetail publish(StoreRequest request) throws TopicNotFoundException, IOException, UnauthorizedException;

  /**
   * Stores a list of messages to the messaging system. It is for long / distributed transactional publishing use case.
   *
   * @param request the {@link StoreRequest} containing messages to be stored
   * @throws TopicNotFoundException if the topic doesn't exist
   * @throws IOException if failed to store messages
   * @throws ServiceUnavailableException if the messaging service is not available
   */
  void storePayload(StoreRequest request) throws TopicNotFoundException, IOException, UnauthorizedException;

  /**
   * Rollbacks messages published to the given topic with the given transaction.
   *
   * @param topicId the topic where the messages were published under
   * @param rollbackDetail the {@link RollbackDetail} as returned by the
   *                     {@link #publish(StoreRequest)} call,
   *                     which contains information needed for the rollback
   * @throws TopicNotFoundException if the topic doesn't exist
   * @throws IOException if failed to rollback changes
   * @throws ServiceUnavailableException if the messaging service is not available
   */
  void rollback(TopicId topicId, RollbackDetail rollbackDetail)
    throws TopicNotFoundException, IOException, UnauthorizedException;
}
