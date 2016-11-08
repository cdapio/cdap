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
public class PayloadId {
  private final long transactionWritePointer;
  private final long writeTimestamp;
  private final short payloadSequenceId;

  public PayloadId(long transactionWritePointer, long writeTimestamp, short payloadSequenceId) {
    this.transactionWritePointer = transactionWritePointer;
    this.writeTimestamp = writeTimestamp;
    this.payloadSequenceId = payloadSequenceId;
  }

  public long getTransactionWritePointer() {
    return transactionWritePointer;
  }

  public long getWriteTimestamp() {
    return writeTimestamp;
  }

  public short getPayloadSequenceId() {
    return payloadSequenceId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(transactionWritePointer, writeTimestamp, payloadSequenceId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof PayloadId)) {
      return false;
    }

    PayloadId other = (PayloadId) o;
    return this.transactionWritePointer == other.transactionWritePointer &&
      this.writeTimestamp == other.writeTimestamp &&
      this.payloadSequenceId == other.payloadSequenceId;
  }
}
