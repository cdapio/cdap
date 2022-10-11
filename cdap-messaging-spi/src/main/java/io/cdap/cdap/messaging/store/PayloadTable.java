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

package io.cdap.cdap.messaging.store;

import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.messaging.data.TopicId;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * An interface defining the Payload Table operations.
 *
 * @see <a href="https://wiki.cask.co/display/CE/Messaging">Design documentation</a>
 */
public interface PayloadTable extends Closeable {

  /**
   * Represents an entry (row) in the payload table.
   */
  interface Entry {

    /**
     * Returns the topic id that the entry belongs to.
     */
    TopicId getTopicId();

    /**
     * Returns the generation id of the topic.
     */
    int getGeneration();

    /**
     * Returns the message payload.
     */
    byte[] getPayload();

    /**
     * Returns the transaction write pointer for storing the payload.
     */
    long getTransactionWritePointer();

    /**
     * Returns the timestamp in milliseconds when the payload was written to the payload table.
     */
    long getPayloadWriteTimestamp();

    /**
     * Returns the sequence id generated when the payload was written to the payload table.
     */
    short getPayloadSequenceId();
  }

  /**
   * Fetches entries from the payload table under the given topic, starting from the given {@link MessageId}.
   *
   * @param metadata {@link TopicMetadata} of the topic to fetch from
   * @param transactionWritePointer transaction write pointer
   * @param messageId message Id to start from
   * @param inclusive {@code true} to include the entry identified by the given {@link MessageId} as the first message
   * @param limit maximum number of entries to fetch
   * @return a {@link Iterator} of entries
   */
  CloseableIterator<Entry> fetch(TopicMetadata metadata, long transactionWritePointer, MessageId messageId,
                                 boolean inclusive, int limit) throws IOException;

  /**
   * Stores a list of entries to the payload table under the given topic.
   *
   * @param entries a list of entries to store. This method guarantees each {@link Entry} will be consumed right away,
   *                hence it is safe for the {@link Iterator} to reuse the same {@link Entry} instance
   */
  void store(Iterator<? extends Entry> entries) throws IOException;
}
