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

import co.cask.cdap.common.utils.TimeProvider;
import co.cask.cdap.messaging.StoreRequest;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.proto.id.TopicId;

import java.io.IOException;
import java.util.Iterator;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link StoreRequestWriter} that writes {@link StoreRequest}s to {@link MessageTable}.
 */
@NotThreadSafe
final class MessageTableStoreRequestWriter extends StoreRequestWriter<MessageTable.Entry> {

  private final MessageTable messageTable;
  private final MutableMessageTableEntry entry;

  MessageTableStoreRequestWriter(MessageTable messageTable, TimeProvider timeProvider) {
    super(timeProvider, true);
    this.messageTable = messageTable;
    this.entry = new MutableMessageTableEntry();
  }

  @Override
  MessageTable.Entry getEntry(TopicId topicId, boolean transactional, long transactionWritePointer,
                              long writeTimestamp, short sequenceId, @Nullable byte[] payload) {
    return entry
      .setTopicId(topicId)
      .setTransactional(transactional)
      .setTransactionWritePointer(transactionWritePointer)
      .setPublishTimestamp(writeTimestamp)
      .setSequenceId(sequenceId)
      .setPayload(payload);
  }

  @Override
  protected void doWrite(Iterator<MessageTable.Entry> entries) throws IOException {
    messageTable.store(entries);
  }

  @Override
  public void close() throws IOException {
    messageTable.close();
  }

  /**
   * A mutable implementation of {@link MessageTable.Entry}.
   */
  private static final class MutableMessageTableEntry implements MessageTable.Entry {

    private TopicId topicId;
    private boolean transactional;
    private long transactionWritePointer;
    private long publishTimestamp;
    private short sequenceId;
    private byte[] payload;

    MutableMessageTableEntry setTopicId(TopicId topicId) {
      this.topicId = topicId;
      return this;
    }

    MutableMessageTableEntry setTransactional(boolean transactional) {
      this.transactional = transactional;
      return this;
    }

    MutableMessageTableEntry setTransactionWritePointer(long transactionWritePointer) {
      this.transactionWritePointer = transactionWritePointer;
      return this;
    }

    MutableMessageTableEntry setPublishTimestamp(long publishTimestamp) {
      this.publishTimestamp = publishTimestamp;
      return this;
    }

    MutableMessageTableEntry setSequenceId(short sequenceId) {
      this.sequenceId = sequenceId;
      return this;
    }

    MutableMessageTableEntry setPayload(@Nullable byte[] payload) {
      this.payload = payload;
      return this;
    }

    @Override
    public TopicId getTopicId() {
      return topicId;
    }

    @Override
    public boolean isPayloadReference() {
      return getPayload() == null;
    }

    @Override
    public boolean isTransactional() {
      return transactional;
    }

    @Override
    public long getTransactionWritePointer() {
      return transactionWritePointer;
    }

    @Nullable
    @Override
    public byte[] getPayload() {
      return payload;
    }

    @Override
    public long getPublishTimestamp() {
      return publishTimestamp;
    }

    @Override
    public short getSequenceId() {
      return sequenceId;
    }
  }
}
