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

package co.cask.cdap.messaging.service;

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.messaging.MessageFetcher;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.data.Message;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.messaging.store.PayloadTable;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Implementation of {@link MessageFetcher} that fetch messages directly
 * from {@link MessageTable} and {@link PayloadTable}.
 */
final class CoreMessageFetcher extends MessageFetcher {

  private final TopicMetadata topicMetadata;
  private final MessageTable messageTable;
  private final PayloadTable payloadTable;

  CoreMessageFetcher(TopicMetadata topicMetadata, MessageTable messageTable, PayloadTable payloadTable) {
    this.topicMetadata = topicMetadata;
    this.messageTable = messageTable;
    this.payloadTable = payloadTable;
  }

  @Override
  public CloseableIterator<Message> fetch() throws IOException {
    final TopicId topicId = topicMetadata.getTopicId();
    long ttl = topicMetadata.getTTL();

    MessageId startOffset = getStartOffset();
    Long startTime = getStartTime();

    // Lower bound of messages that are still valid
    long smallestPublishTime = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(ttl);

    final CloseableIterator<MessageTable.Entry> messageIterator;
    // If there is no startOffset or if the publish time in the startOffset is smaller then TTL,
    // do the scanning based on time. The smallest start time should be the currentTime - TTL.
    if (startOffset == null || startOffset.getPublishTimestamp() < smallestPublishTime) {
      long fetchStartTime = Math.max(smallestPublishTime, startTime == null ? smallestPublishTime : startTime);
      messageIterator = messageTable.fetch(topicId, fetchStartTime, getLimit(), getTransaction());
    } else {
      // Start scanning based on the start message id
      if (startOffset.getPayloadWriteTimestamp() != 0L) {
        // This message ID refer to payload table. We scan the message table with the reference message ID inclusively.
        messageIterator = messageTable.fetch(topicId, createMessageTableMessageId(startOffset),
                                             true, getLimit(), getTransaction());
      } else {
        messageIterator = messageTable.fetch(topicId, startOffset, isIncludeStart(), getLimit(), getTransaction());
      }
    }

    return new CloseableIterator<Message>() {

      private Message nextMessage;
      private MessageTable.Entry messageEntry;
      private CloseableIterator<PayloadTable.Entry> payloadIterator;
      private boolean inclusive = isIncludeStart();
      private int messageLimit = getLimit();

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
            nextMessage = new Message(createMessageId(messageEntry, payloadEntry), payloadEntry.getPayload());
            break;
          }

          // If there is no payload iterator or it has been exhausted, read the next message from the message iterator
          if (messageIterator.hasNext()) {
            messageEntry = messageIterator.next();
            if (messageEntry.isPayloadReference()) {
              // If the message entry is a reference to payload table, create the payload iterator
              try {
                payloadIterator = payloadTable.fetch(topicId, messageEntry.getTransactionWritePointer(),
                                                     createMessageId(messageEntry, null),
                                                     inclusive, messageLimit);
              } catch (IOException e) {
                throw Throwables.propagate(e);
              }
            } else {
              // Otherwise, the message entry is the next message
              nextMessage = new Message(createMessageId(messageEntry, null), messageEntry.getPayload());
            }
          } else {
            // If there is no more message from the message iterator as well, then no more message to fetch
            break;
          }
        }
        inclusive = true;
        return nextMessage != null;
      }

      @Override
      public Message next() {
        if (!hasNext()) {
          throw new NoSuchElementException("No more message from " + topicId);
        }
        Message message = nextMessage;
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
        try {
          if (payloadIterator != null) {
            payloadIterator.close();
          }
        } finally {
          messageIterator.close();
        }
      }
    };
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
   * Creates a {@link MessageId} for the given {@link MessageTable.Entry} and {@link PayloadTable.Entry}.
   *
   * @param messageEntry entry in the message table representing a message
   * @param payloadEntry an optional entry in the payload table if the message payload is stored in the Payload Table
   * @return a {@link MessageId} for the given message entry.
   */
  private MessageId createMessageId(MessageTable.Entry messageEntry, @Nullable PayloadTable.Entry payloadEntry) {
    byte[] rawId = new byte[MessageId.RAW_ID_SIZE];
    long writeTimestamp = payloadEntry == null ? 0L : payloadEntry.getPayloadWriteTimestamp();
    short payloadSeqId = payloadEntry == null ? 0 : payloadEntry.getPayloadSequenceId();

    MessageId.putRawId(messageEntry.getPublishTimestamp(), messageEntry.getSequenceId(),
                       writeTimestamp, payloadSeqId, rawId, 0);
    return new MessageId(rawId);
  }
}
