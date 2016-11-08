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

import java.util.Objects;

/**
 *
 */
public class MessageId {
  private final long publishTimestamp;
  private final short sequenceId;
  private final long writeTimestamp;
  private final short payloadSequenceId;

  public MessageId(long publishTimestamp, short sequenceId, long writeTimestamp, short payloadSequenceId) {
    this.publishTimestamp = publishTimestamp;
    this.sequenceId = sequenceId;
    this.writeTimestamp = writeTimestamp;
    this.payloadSequenceId = payloadSequenceId;
  }

  public long getPublishTimestamp() {
    return publishTimestamp;
  }

  public short getSequenceId() {
    return sequenceId;
  }

  public long getWriteTimestamp() {
    return writeTimestamp;
  }

  public short getPayloadSequenceId() {
    return payloadSequenceId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(publishTimestamp, sequenceId, writeTimestamp, payloadSequenceId);
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
    return this.publishTimestamp == other.publishTimestamp &&
      this.sequenceId == other.sequenceId &&
      this.writeTimestamp == other.writeTimestamp &&
      this.payloadSequenceId == other.payloadSequenceId;
  }
}
