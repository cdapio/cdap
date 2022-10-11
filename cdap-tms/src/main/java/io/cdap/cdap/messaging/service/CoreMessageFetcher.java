/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

package io.cdap.cdap.messaging.service;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.messaging.MessageFetcher;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.messaging.data.RawMessage;
import io.cdap.cdap.messaging.store.MessageTable;
import io.cdap.cdap.messaging.store.PayloadTable;
import io.cdap.cdap.proto.id.TopicId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Implementation of {@link MessageFetcher} that fetch messages directly
 * from {@link MessageTable} and {@link PayloadTable}.
 */
final class CoreMessageFetcher extends MessageFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(CoreMessageFetcher.class);

  private final TopicMetadata topicMetadata;
  private final TableProvider<MessageTable> messageTableProvider;
  private final TableProvider<PayloadTable> payloadTableProvider;

  CoreMessageFetcher(TopicMetadata topicMetadata,
                     TableProvider<MessageTable> messageTableProvider,
                     TableProvider<PayloadTable> payloadTableProvider) {
    this.topicMetadata = topicMetadata;
    this.messageTableProvider = messageTableProvider;
    this.payloadTableProvider = payloadTableProvider;
  }

  @Override
  public CloseableIterator<RawMessage> fetch() throws IOException {
    MessageTable messageTable = messageTableProvider.get();
    try {
      return new MessageCloseableIterator(messageTable);
    } catch (Throwable t) {
      closeQuietly(messageTable);
      throw t;
    }
  }

  /**
   * Creates a {@link MessageId} from another message id by copying the publish timestamp and
   * sequence id.
   */
  private MessageId createMessageTableMessageId(MessageId messageId) {
    // Create a new MessageId with write timestamp and payload seqId = 0
    byte[] rawId = new byte[MessageId.RAW_ID_SIZE];
    MessageId.putRawId(messageId.getPublishTimestamp(), messageId.getSequenceId(), 0L, (short) 0, rawId, 0);
    return new MessageId(rawId);
  }

  /**
   * Creates a raw message id from the given {@link MessageTable.Entry} and {@link PayloadTable.Entry}.
   *
   * @param messageEntry entry in the message table representing a message
   * @param payloadEntry an optional entry in the payload table if the message payload is stored in the Payload Table
   * @return a byte array representing the raw message id.
   */
  private byte[] createMessageId(MessageTable.Entry messageEntry, @Nullable PayloadTable.Entry payloadEntry) {
    long writeTimestamp = payloadEntry == null ? 0L : payloadEntry.getPayloadWriteTimestamp();
    short payloadSeqId = payloadEntry == null ? 0 : payloadEntry.getPayloadSequenceId();
    return createMessageId(messageEntry, writeTimestamp, payloadSeqId);
  }

  /**
   * Creates a raw message id from the given {@link MessageTable.Entry} and the payload write timestamp and sequence id.
   *
   * @param messageEntry entry in the message table representing a message
   * @param payloadWriteTimestamp the timestamp that the entry was written to the payload table.
   * @param payloadSeqId the sequence id generated when the entry was written to the payload table.
   * @return a byte array representing the raw message id.
   */
  private byte[] createMessageId(MessageTable.Entry messageEntry, long payloadWriteTimestamp, short payloadSeqId) {
    byte[] rawId = new byte[MessageId.RAW_ID_SIZE];
    MessageId.putRawId(messageEntry.getPublishTimestamp(), messageEntry.getSequenceId(),
                       payloadWriteTimestamp, payloadSeqId, rawId, 0);
    return rawId;
  }

  /**
   * Calls the {@link AutoCloseable#close()} on the given {@link AutoCloseable} without throwing exception.
   * If there is exception raised, it will be logged but never thrown out.
   */
  private void closeQuietly(@Nullable AutoCloseable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (Throwable t) {
      LOG.warn("Exception raised when closing Closeable {}", closeable, t);
    }
  }

  /**
   * A {@link CloseableIterator} of {@link RawMessage} implementation that contains the core message fetching logic
   * by combine scanning on both {@link MessageTable} and {@link PayloadTable}.
   */
  private final class MessageCloseableIterator implements CloseableIterator<RawMessage> {

    private final CloseableIterator<MessageTable.Entry> messageIterator;
    private final TopicId topicId;
    private final MessageTable messageTable;
    private RawMessage nextMessage;
    private MessageTable.Entry messageEntry;
    private CloseableIterator<PayloadTable.Entry> payloadIterator;
    private MessageId startOffset;
    private boolean inclusive;
    private int messageLimit;
    private PayloadTable payloadTable;

    MessageCloseableIterator(MessageTable messageTable) throws IOException {
      this.topicId = new TopicId(topicMetadata.getTopicId());
      this.messageTable = messageTable;
      this.inclusive = isIncludeStart();
      this.messageLimit = getLimit();

      long ttl = topicMetadata.getTTL();
      startOffset = getStartOffset() == null ? null : new MessageId(getStartOffset());
      Long startTime = getStartTime();

      // Lower bound of messages that are still valid
      long smallestPublishTime = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(ttl);

      CloseableIterator<MessageTable.Entry> messageIterator;
      // If there is no startOffset or if the publish time in the startOffset is smaller then TTL,
      // do the scanning based on time. The smallest start time should be the currentTime - TTL.
      if (startOffset == null || startOffset.getPublishTimestamp() < smallestPublishTime) {
        long fetchStartTime = Math.max(smallestPublishTime, startTime == null ? smallestPublishTime : startTime);
        messageIterator = messageTable.fetch(topicMetadata, fetchStartTime, messageLimit, getTransaction());
      } else {
        // Start scanning based on the start message id
        if (startOffset.getPayloadWriteTimestamp() != 0L) {
          // This message ID refer to payload table. Scan the message table with the reference message ID inclusively.
          messageIterator = messageTable.fetch(topicMetadata, createMessageTableMessageId(startOffset),
                                               true, messageLimit, getTransaction());
        } else {
          messageIterator = messageTable.fetch(topicMetadata, startOffset, isIncludeStart(),
                                               messageLimit, getTransaction());
        }
      }
      this.messageIterator = messageIterator;
    }

    @Override
    public boolean hasNext() {
      if (messageLimit <= 0) {
        return false;
      }

      // Find the next message
      while (nextMessage == null) {
        // If there is a payload iterator and is not empty, read the next message from the it
        if (payloadIterator != null && payloadIterator.hasNext()) {
          PayloadTable.Entry payloadEntry = payloadIterator.next();
          // messageEntry is guaranteed to be non-null if payloadIterator is non-null
          nextMessage = new RawMessage(createMessageId(messageEntry, payloadEntry), payloadEntry.getPayload());
          break;
        }

        // If there is no payload iterator or it has been exhausted, read the next message from the message iterator
        if (messageIterator.hasNext()) {
          messageEntry = messageIterator.next();
          if (messageEntry.isPayloadReference()) {
            // If the message entry is a reference to payload table, create the payload iterator
            try {
              if (payloadTable == null) {
                payloadTable = payloadTableProvider.get();
              }

              closeQuietly(payloadIterator);

              MessageId payloadStartOffset = startOffset == null
                ? new MessageId(createMessageId(messageEntry, null))
                : new MessageId(createMessageId(messageEntry, startOffset.getPayloadWriteTimestamp(),
                                                startOffset.getPayloadSequenceId()));

              // If startOffset is not used, always fetch with inclusive.
              payloadIterator = payloadTable.fetch(topicMetadata, messageEntry.getTransactionWritePointer(),
                                                   payloadStartOffset, startOffset == null || inclusive, messageLimit);
              // The start offset is only used for the first payloadIterator being constructed.
              startOffset = null;
            } catch (IOException e) {
              throw Throwables.propagate(e);
            }
          } else {
            // Otherwise, the message entry is the next message
            nextMessage = new RawMessage(createMessageId(messageEntry, null), messageEntry.getPayload());
          }
        } else {
          // If there is no more message from the message iterator as well, then no more message to fetch
          break;
        }
      }
      // After the first message, all the sub-sequence table.fetch call should always include all message.
      inclusive = true;
      return nextMessage != null;
    }

    @Override
    public RawMessage next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more message from " + topicId);
      }
      RawMessage message = nextMessage;
      nextMessage = null;
      messageLimit--;
      return message;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported");
    }

    @Override
    public void close() {
      closeQuietly(payloadIterator);
      closeQuietly(messageIterator);
      closeQuietly(payloadTable);
      closeQuietly(messageTable);
    }
  }
}
