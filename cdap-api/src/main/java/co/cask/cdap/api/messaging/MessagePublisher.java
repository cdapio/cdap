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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.Beta;
import org.apache.tephra.TransactionFailureException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * Provides access to publish messages to the transactional messaging system.
 * Note that for instances acquired through the {@link MessagingContext#getMessagePublisher()} method and
 * when any of the {@code publish} methods is called within a transactional context,
 * exceptions may not be thrown right away to indicate the error, but rather at the transaction commit time.
 * If {@link Transactional#execute(TxRunnable)} was used to execute the transaction, exception will always be wrapped
 * inside a {@link TransactionFailureException}.
 */
@Beta
public interface MessagePublisher {

  /**
   * Publishes messages to the given topic. Same as calling the
   * {@link #publish(String, String, Charset, String...)} method
   * with charset equals to {@link StandardCharsets#UTF_8}.
   */
  void publish(String namespace, String topic, String...payloads) throws TopicNotFoundException, IOException;

  /**
   * Publishes messages to the given topic. Each payload string will be encoded as byte array using the given
   * charset.
   *
   * @param namespace namespace of the topic
   * @param topic name of the topic
   * @param charset the {@link Charset} for encoding the payload Strings.
   * @param payloads the payloads to publish. Each element in the array will become a {@link Message}.
   * @throws IllegalArgumentException if the topic name is invalid. A valid id should only contain alphanumeric
   *                                  characters and {@code _} or {@code -}.
   * @throws IOException if failed to communicate with the messaging system.
   * @throws TopicNotFoundException if the give topic doesn't exist
   */
  void publish(String namespace, String topic,
               Charset charset, String...payloads) throws TopicNotFoundException, IOException;

  /**
   * Publishes messages to the given topic.
   *
   * @param namespace namespace of the topic
   * @param topic name of the topic
   * @param payloads the payloads to publish. Each element in the array will become a {@link Message}.
   * @throws IllegalArgumentException if the topic name is invalid. A valid id should only contain alphanumeric
   *                                  characters and {@code _} or {@code -}.
   * @throws IOException if failed to communicate with the messaging system
   * @throws TopicNotFoundException if the give topic doesn't exist
   */
  void publish(String namespace, String topic, byte[]...payloads) throws TopicNotFoundException, IOException;

  /**
   * Publishes messages to the given topic. Each payload string will be encoded as byte array using the given
   * charset.
   *
   * @param namespace namespace of the topic
   * @param topic name of the topic
   * @param charset the {@link Charset} for encoding the payload Strings.
   * @param payloads the payloads to publish. Each element in the array will become a {@link Message}.
   * @throws IllegalArgumentException if the topic name is invalid. A valid id should only contain alphanumeric
   *                                  characters and {@code _} or {@code -}.
   * @throws IOException if failed to communicate with the messaging system
   * @throws TopicNotFoundException if the give topic doesn't exist
   */
  void publish(String namespace, String topic,
               Charset charset, Iterator<String> payloads) throws TopicNotFoundException, IOException;

  /**
   * Publishes messages to the given topic.
   *
   * @param namespace namespace of the topic
   * @param topic name of the topic
   * @param payloads the payloads to publish. Each element in the array will become a {@link Message}.
   * @throws IllegalArgumentException if the topic name is invalid. A valid id should only contain alphanumeric
   *                                  characters and {@code _} or {@code -}.
   * @throws IOException if failed to communicate with the messaging system
   * @throws TopicNotFoundException if the give topic doesn't exist
   */
  void publish(String namespace, String topic, Iterator<byte[]> payloads) throws TopicNotFoundException, IOException;
}
