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

import co.cask.cdap.messaging.data.PublishableMessage;
import co.cask.cdap.proto.id.TopicId;

import java.util.Iterator;

/**
 * Defines the interactions with the core messaging system.
 *
 * @see <a href="https://wiki.cask.co/display/CE/Messaging">Design documentation</a>
 */
public interface MessagingService {

  /**
   * Prepares to fetch messages from the given topic.
   *
   * @param topicId the topic to fetch message from
   * @return a {@link MessageFetcher} for setting up parameters for fetching messages from the messaging system
   */
  MessageFetcher prepareFetch(TopicId topicId);

  /**
   * Publishes a list of messages to the messaging system.
   *
   * @param topicId topic to publish to
   * @param messages the {@link PublishableMessage}s to be published
   */
  void publish(TopicId topicId, Iterator<PublishableMessage> messages) throws TopicNotFoundException;

  /**
   * Stores a list of messages to the messaging system. It is for long / distributed transactional publishing use case.
   *
   * @param topicId topic to store under
   * @param messages the {@link PublishableMessage}s to be published
   */
  void storePayload(TopicId topicId, Iterator<PublishableMessage> messages) throws TopicNotFoundException;

  /**
   * Rollbacks messages published to the given topic with the given transaction.
   *
   * @param topicId the topic where the messages were published under
   * @param transactionWritePointer the transaction write pointer to rollback
   */
  void rollback(TopicId topicId, long transactionWritePointer) throws TopicNotFoundException;
}
