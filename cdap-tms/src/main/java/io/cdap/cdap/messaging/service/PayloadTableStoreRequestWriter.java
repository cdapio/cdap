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

package io.cdap.cdap.messaging.service;

import io.cdap.cdap.common.utils.TimeProvider;
import io.cdap.cdap.messaging.StoreRequest;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.store.PayloadTable;
import io.cdap.cdap.proto.id.TopicId;

import java.io.IOException;
import java.util.Iterator;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link StoreRequestWriter} that writes {@link StoreRequest}s to {@link PayloadTable}.
 */
@NotThreadSafe
final class PayloadTableStoreRequestWriter extends StoreRequestWriter<PayloadTable.Entry> {

  private final PayloadTable payloadTable;
  private final MutablePayloadTableEntry entry;

  PayloadTableStoreRequestWriter(PayloadTable payloadTable, TimeProvider timeProvider) {
    super(timeProvider, false);
    this.payloadTable = payloadTable;
    this.entry = new MutablePayloadTableEntry();
  }

  @Override
  PayloadTable.Entry getEntry(TopicMetadata metadata, boolean transactional, long transactionWritePointer,
                              long writeTimestamp, short sequenceId, @Nullable byte[] payload) {
    return entry
      .setTopicId(new TopicId(metadata.getTopicId()))
      .setGeneration(metadata.getGeneration())
      .setTransactionWritePointer(transactionWritePointer)
      .setPayloadWriteTimestamp(writeTimestamp)
      .setPayloadSequenceId(sequenceId)
      .setPayload(payload);
  }

  @Override
  protected void doWrite(Iterator<PayloadTable.Entry> entries) throws IOException {
    payloadTable.store(entries);
  }

  @Override
  public void close() throws IOException {
    payloadTable.close();
  }

  /**
   * A mutable implementation of {@link PayloadTable.Entry}.
   */
  private static final class MutablePayloadTableEntry implements PayloadTable.Entry {

    private TopicId topicId;
    private int generation;
    private long transactionWritePointer;
    private long writeTimestamp;
    private short sequenceId;
    private byte[] payload;

    MutablePayloadTableEntry setTopicId(TopicId topicId) {
      this.topicId = topicId;
      return this;
    }

    MutablePayloadTableEntry setGeneration(int generation) {
      this.generation = generation;
      return this;
    }

    MutablePayloadTableEntry setTransactionWritePointer(long transactionWritePointer) {
      this.transactionWritePointer = transactionWritePointer;
      return this;
    }

    MutablePayloadTableEntry setPayloadWriteTimestamp(long writeTimestamp) {
      this.writeTimestamp = writeTimestamp;
      return this;
    }

    MutablePayloadTableEntry setPayloadSequenceId(short sequenceId) {
      this.sequenceId = sequenceId;
      return this;
    }

    MutablePayloadTableEntry setPayload(byte[] payload) {
      this.payload = payload;
      return this;
    }

    @Override
    public io.cdap.cdap.messaging.data.TopicId getTopicId() {
      return topicId.toSpiTopicId();
    }

    @Override
    public int getGeneration() {
      return generation;
    }

    @Override
    public byte[] getPayload() {
      return payload;
    }

    @Override
    public long getTransactionWritePointer() {
      return transactionWritePointer;
    }

    @Override
    public long getPayloadWriteTimestamp() {
      return writeTimestamp;
    }

    @Override
    public short getPayloadSequenceId() {
      return sequenceId;
    }
  }
}
