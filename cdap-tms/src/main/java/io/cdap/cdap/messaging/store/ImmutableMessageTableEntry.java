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

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.messaging.MessagingUtils;
import io.cdap.cdap.proto.id.TopicId;

import javax.annotation.Nullable;

/**
 * An immutable implementation of {@link MessageTable.Entry}.
 */
public final class ImmutableMessageTableEntry implements MessageTable.Entry {
  private final TopicId topicId;
  private final int generation;
  private final boolean transactional;
  private final long transactionWritePointer;
  private final byte[] payload;
  private final long publishTimestamp;
  private final short sequenceId;

  public ImmutableMessageTableEntry(byte[] row, @Nullable byte[] payload, @Nullable byte[] txPtr) {
    this.topicId = MessagingUtils.toTopicId(row, 0,
                                            row.length - Bytes.SIZEOF_SHORT - Bytes.SIZEOF_LONG - Bytes.SIZEOF_INT);
    this.generation = Bytes.toInt(row, row.length - Bytes.SIZEOF_SHORT - Bytes.SIZEOF_LONG - Bytes.SIZEOF_INT);

    int topicLength = MessagingUtils.getTopicLengthMessageEntry(row.length);
    this.publishTimestamp = Bytes.toLong(row, topicLength);
    this.sequenceId = Bytes.toShort(row, topicLength + Bytes.SIZEOF_LONG);
    this.transactional = (txPtr != null);
    // since we mark tx as negative when tx is rolled back, we return the absolute value of tx
    this.transactionWritePointer = txPtr == null ? -1 : Math.abs(Bytes.toLong(txPtr));
    this.payload = payload;
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
  public boolean isPayloadReference() {
    return payload == null;
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
