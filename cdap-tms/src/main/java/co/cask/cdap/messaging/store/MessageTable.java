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

package co.cask.cdap.messaging.store;

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.messaging.RollbackDetail;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.proto.id.TopicId;
import org.apache.tephra.Transaction;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * An interface defining the Message Table operations.
 *
 * @see <a href="https://wiki.cask.co/display/CE/Messaging">Design documentation</a>
 */
public interface MessageTable extends Closeable {

  /**
   * Represents an entry (row) in the message table.
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
     * Returns {@code true} if the entry is a reference to messages stored in payload table.
     */
    boolean isPayloadReference();

    /**
     * Returns {@code true} if the message is a transactional message
     */
    boolean isTransactional();

    /**
     * Returns the transaction write pointer stored in this entry. The value returned will be valid
     * if and only if {@link #isTransactional()} returns {@code true}.
     */
    long getTransactionWritePointer();

    /**
     * Returns the message payload if {@link #isPayloadReference()} return {@code false}; otherwise {@code null}
     * will be returned.
     */
    @Nullable
    byte[] getPayload();

    /**
     * Returns the timestamp in milliseconds when this entry was written to the message table.
     */
    long getPublishTimestamp();

    /**
     * Returns the sequence id generated when this entry was written to the message table.
     */
    short getSequenceId();
  }

  /**
   * Fetches message table entries in the given topic that were publish on or after the given start time.
   *
   * @param metadata metadata of the topic to fetch from
   * @param startTime the publish time to start from
   * @param limit maximum number of messages to fetch
   * @param transaction an optional {@link Transaction} to use for fetching
   * @return a {@link CloseableIterator} of {@link Entry}.
   */
  CloseableIterator<Entry> fetch(TopicMetadata metadata, long startTime,
                                 int limit, @Nullable Transaction transaction) throws IOException;

  /**
   * Fetches message table entries in the given topic, starting from the given {@link MessageId}.
   *
   * @param metadata {@link TopicMetadata} of the the topic to fetch from
   * @param messageId the message id to start fetching from
   * @param inclusive indicate whether to include the given {@link MessageId} as the first message (if still available)
   *                  or not.
   * @param limit maximum number of messages to fetch
   * @param transaction an optional {@link Transaction} to use for fetching
   * @return a {@link CloseableIterator} of {@link Entry}.
   */
  CloseableIterator<Entry> fetch(TopicMetadata metadata, MessageId messageId, boolean inclusive,
                                 int limit, @Nullable Transaction transaction) throws IOException;

  /**
   * Stores a list of entries to the message table under the given topic.
   *
   * @param entries a list of entries to store. This method guarantees each {@link Entry} will be consumed right away,
   *                hence it is safe for the {@link Iterator} to reuse the same {@link Entry} instance.
   */
  void store(Iterator<? extends Entry> entries) throws IOException;

  /**
   * Rollback entries stored earlier, as deleted, under the given topic based on the given information.
   *
   * @param metadata {@link TopicMetadata} of the topic to rollback from
   * @param rollbackDetail {@link RollbackDetail} information required to rollback entries
   */
  void rollback(TopicMetadata metadata, RollbackDetail rollbackDetail) throws IOException;
}
