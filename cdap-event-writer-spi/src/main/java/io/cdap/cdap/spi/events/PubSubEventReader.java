/*
 * Copyright © 2022 Cask Data, Inc.
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

import java.util.Optional;

/**
 * {@link PubSubEventReader} implementation for sending events to Pub/Sub.
 */
public interface PubSubEventReader {
  /**
   * Method to initialize PubSubEventReader
   * @param context Configuration properties of reader
   */
  void initialize(PubSubEventReaderContext context);

  /**
   * Pull exactly one message from PubSub if available
   *
   * @return Message wrapped in an {@link Optional}
   */
  Optional<ReceivedEvent> pull();

  /**
   * Sends an acknowledgement response to PubSub
   *
   * @param ackId Ack Id of Message to acknowledge
   * @throws Exception Invalid ACK ID / Expired ACK ID
   */
  void ack(String ackId) throws Exception;

  /**
   * Sends an non-acknowledgement response to PubSub
   * Message will be redelivered by.
   *
   * @param ackId Ack Id of Message
   * @throws Exception Invalid ACK ID / Expired ACK ID
   */
  void nack(String ackId) throws Exception;

  /**
   * Returns the identifier for this reader
   *
   * @return String id for the reader
   */
  String getID();

  /**
   * Close connection to PubSub
   */
  void close();
}

