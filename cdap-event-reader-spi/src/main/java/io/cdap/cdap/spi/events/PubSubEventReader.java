/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.spi.events;

import java.util.List;

/**
 * {@link PubSubEventReader} Interface for listening for events from Pub/Sub.
 */
public interface PubSubEventReader {
  /**
   * Method to initialize PubSubEventReader.
   *
   * @param context Configuration properties of reader
   */
  void initialize(PubSubEventReaderContext context);

  /**
   * Pull exactly messages from PubSub if available.
   *
   * @param maxMessages: maximum messages to pull
   * @return List of Messages
   */
  List<ReceivedEvent> pull(int maxMessages);

  /**
   * Sends an acknowledgement response to PubSub.
   *
   * @param ackId Ack Id of Message to acknowledge
   * @throws Exception Invalid ACK ID / Expired ACK ID
   */
  void ack(String ackId) throws Exception;

  /**
   * Sends an non-acknowledgement response to PubSub
   * causing the message to be redelivered.
   *
   * @param ackId Ack Id of Message
   * @throws Exception Invalid ACK ID / Expired ACK ID
   */
  void nack(String ackId) throws Exception;

  /**
   * Returns the identifier for this reader.
   *
   * @return String id for the reader
   */
  String getID();

  /**
   * Close connection to PubSub.
   */
  void close();
}

