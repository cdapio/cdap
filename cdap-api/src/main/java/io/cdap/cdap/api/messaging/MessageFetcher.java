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

package co.cask.cdap.api.messaging;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.dataset.lib.CloseableIterator;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Provides message fetching functions of the Transactional Messaging System.
 */
@Beta
public interface MessageFetcher {

  /**
   * Fetches messages from the given topic that were published on or after the given timestamp.
   *
   * @param namespace namespace of the topic
   * @param topic name of the topic
   * @param limit maximum number of messages to fetch
   * @param timestamp timestamp in milliseconds; the publish timestamp of the first {@link Message}
   *                  returned by the resulting {@link CloseableIterator} will be on or after the given timestamp.
   *                  Use {@code timestamp = 0} to poll from the first available message.
   * @return a {@link CloseableIterator} of {@link Message}
   * @throws IllegalArgumentException if the topic name is invalid. A valid id should only contain alphanumeric
   *                                  characters, {@code _}, or {@code -}.
   * @throws IOException if there was a failure to communicate with the messaging system
   * @throws TopicNotFoundException if the give topic doesn't exist
   */
  CloseableIterator<Message> fetch(String namespace, String topic,
                                   int limit, long timestamp) throws TopicNotFoundException, IOException;

  /**
   * Fetches messages from the given topic that were published after a message, identified by the given
   * message id.
   *
   * @param namespace namespace of the topic
   * @param topic name of the topic
   * @param limit maximum number of messages to fetch
   * @param afterMessageId message id returned from the {@link Message#getId()} method from a prior call to
   *                       one of the {@code pollMessages} methods. If it is {@code null}, it will fetch from
   *                       the first available message.
   * @return a {@link CloseableIterator} of {@link Message}
   * @throws IllegalArgumentException if the topic name is invalid. A valid id should only contain alphanumeric
   *                                  characters, {@code _}, or {@code -}.
   * @throws IOException if there was a failure to communicate with the messaging system
   * @throws TopicNotFoundException if the give topic doesn't exist
   */
  CloseableIterator<Message> fetch(String namespace, String topic, int limit,
                                   @Nullable String afterMessageId) throws TopicNotFoundException, IOException;
}
