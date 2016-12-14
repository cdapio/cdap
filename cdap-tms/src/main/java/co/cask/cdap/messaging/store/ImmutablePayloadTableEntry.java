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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.proto.id.TopicId;

/**
 * An immutable implementation of {@link PayloadTable.Entry}.
 */
final class ImmutablePayloadTableEntry implements PayloadTable.Entry {
  private final TopicId topicId;
  private final int generation;
  private final long transactionWriterPointer;
  private final long publishTimestamp;
  private final short sequenceId;
  private final byte[] payload;

  ImmutablePayloadTableEntry(byte[] row, byte[] payload) {
    this.topicId = MessagingUtils.toTopicId(row, 0, row.length - Bytes.SIZEOF_SHORT - (2 * Bytes.SIZEOF_LONG)
      - Bytes.SIZEOF_INT);
    this.generation = Bytes.toInt(row, row.length - Bytes.SIZEOF_SHORT - (2 * Bytes.SIZEOF_LONG) - Bytes.SIZEOF_INT);
    this.transactionWriterPointer = Bytes.toLong(row, row.length - Bytes.SIZEOF_SHORT - (2 * Bytes.SIZEOF_LONG));
    this.publishTimestamp = Bytes.toLong(row, row.length - Bytes.SIZEOF_SHORT - Bytes.SIZEOF_LONG);
    this.sequenceId = Bytes.toShort(row, row.length - Bytes.SIZEOF_SHORT);
    this.payload = payload;
  }

  @Override
  public TopicId getTopicId() {
    return topicId;
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
    return transactionWriterPointer;
  }

  @Override
  public long getPayloadWriteTimestamp() {
    return publishTimestamp;
  }

  @Override
  public short getPayloadSequenceId() {
    return sequenceId;
  }
}
