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

package co.cask.cdap.messaging.data;

import co.cask.cdap.api.common.Bytes;

import java.util.Arrays;

/**
 * Uniquely identifies a message in the Messaging System.
 *
 * @see <a href="https://wiki.cask.co/display/CE/Messaging">Design documentation</a>
 */
public class MessageId {

  public static final int RAW_ID_SIZE = Bytes.SIZEOF_LONG + Bytes.SIZEOF_SHORT + Bytes.SIZEOF_LONG + Bytes.SIZEOF_SHORT;

  private final byte[] rawId;
  private final long publishTimestamp;
  private final short sequenceId;
  private final long writeTimestamp;
  private final short payloadSequenceId;

  /**
   * Computes the message raw ID and store it in the given byte array.
   *
   * @param publishTimestamp publish timestamp of the message
   * @param sequenceId publish sequence id of the message
   * @param writeTimestamp write timestamp in the payload table of the message
   * @param payloadSequenceId sequence id in the payload table of the message
   * @param buffer the buffer to encode raw id to
   * @param offset the starting offset in the buffer for storing the raw message id
   * @return the offset in the buffer that points to the index right after then end of the raw message id
   */
  public static int putRawId(long publishTimestamp, short sequenceId,
                             long writeTimestamp, short payloadSequenceId, byte[] buffer, int offset) {

    if (buffer.length - offset < RAW_ID_SIZE) {
      throw new IllegalArgumentException("Not enough size in the buffer to encode Message ID");
    }
    offset = Bytes.putLong(buffer, offset, publishTimestamp);
    offset = Bytes.putShort(buffer, offset, sequenceId);
    offset = Bytes.putLong(buffer, offset, writeTimestamp);
    return Bytes.putShort(buffer, offset, payloadSequenceId);
  }

  /**
   * Creates a instance based on the given raw id bytes. The provided byte array will be store as is without
   * copying.
   */
  public MessageId(byte[] rawId) {
    this.rawId = rawId;

    int offset = 0;
    this.publishTimestamp = Bytes.toLong(rawId, offset);
    offset += Bytes.SIZEOF_LONG;

    this.sequenceId = Bytes.toShort(rawId, offset);
    offset += Bytes.SIZEOF_SHORT;

    this.writeTimestamp = Bytes.toLong(rawId, offset);
    offset += Bytes.SIZEOF_LONG;

    this.payloadSequenceId = Bytes.toShort(rawId, offset);
  }

  /**
   * Returns the publish timestamp in milliseconds of the message.
   */
  public long getPublishTimestamp() {
    return publishTimestamp;
  }

  /**
   * Returns the sequence id generated when the message was written.
   */
  public short getSequenceId() {
    return sequenceId;
  }

  /**
   * Returns the timestamp when the message was written to the Payload Table.
   * If the message is not from the Payload Table, {@code 0} will be returned.
   */
  public long getPayloadWriteTimestamp() {
    return writeTimestamp;
  }

  /**
   * Returns the sequence id generated when the message was written to the Payload Table.
   * If the message is not from the Payload Table, {@code 0} will be returned.
   */
  public short getPayloadSequenceId() {
    return payloadSequenceId;
  }

  /**
   * Returns the raw bytes representation of the message id.
   */
  public byte[] getRawId() {
    return rawId;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(rawId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof MessageId)) {
      return false;
    }

    MessageId other = (MessageId) o;
    return Arrays.equals(rawId, other.getRawId());
  }

  public String toString() {
    return "MessageId {" +
      "publishTimestamp=" + publishTimestamp +
      ", sequenceId=" + sequenceId +
      ", writeTimestamp=" + writeTimestamp +
      ", payloadSequenceId=" + payloadSequenceId +
      '}';
  }
}
