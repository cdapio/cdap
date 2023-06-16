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
public interface PubSubEventReader<T extends Event> extends AutoCloseable {
  /**
   * Method to initialize PubSubEventReader.
   *
   * @param context Configuration properties of reader
   */
  void initialize(PubSubEventReaderContext context);

  /**
   * Pull messages from PubSub if available.
   *
   * @param maxMessages maximum messages to pull
   * @return List of Messages
   */
  List<T> pull(int maxMessages);

  /**
   * Action to perform on successful processing of event.
   *
   * @param ackId Ack Id of Message to acknowledge
   * @throws Exception Invalid ACK ID / Expired ACK ID
   */
  void success(String ackId) throws Exception;

  /**
   * Action to perform on failure of processing event.
   *
   * @param ackId Ack Id of Message
   * @param retry whether to retry event
   * @throws Exception Invalid ACK ID / Expired ACK ID
   */
  void failure(String ackId, boolean retry) throws Exception;

  /**
   * Returns the identifier for this reader.
   *
   * @return String id for the reader
   */
  String getId();

  /**
   * Close connection to PubSub.
   */
  void close();
}

