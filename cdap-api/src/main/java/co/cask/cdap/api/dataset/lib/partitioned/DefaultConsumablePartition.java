/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.lib.partitioned;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.nio.ByteBuffer;

/**
 * Default implementation of {@link ConsumablePartition}.
 */
public final class DefaultConsumablePartition implements ConsumablePartition {
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(PartitionKey.class, new PartitionKeyCodec()).create();

  private final PartitionKey partitionKey;

  private ProcessState processState;
  private int numFailures;
  // the timestamp when it was marked IN_PROGRESS. it will be 0 if the ProcessState is AVAILABLE
  private long timestamp;

  public DefaultConsumablePartition(PartitionKey partitionKey) {
    this(partitionKey, ProcessState.AVAILABLE, 0, 0);
  }

  public DefaultConsumablePartition(PartitionKey partitionKey, ProcessState processState,
                                    long timestamp, int numFailures) {
    this.partitionKey = partitionKey;
    this.processState = processState;
    this.timestamp = timestamp;
    this.numFailures = numFailures;
  }

  @Override
  public PartitionKey getPartitionKey() {
    return partitionKey;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public int getNumFailures() {
    return numFailures;
  }

  @Override
  public int incrementNumFailures() {
    return ++numFailures;
  }

  @Override
  public ProcessState getProcessState() {
    return processState;
  }

  @Override
  public void setProcessState(ProcessState processState) {
    this.processState = processState;
  }

  @Override
  public void take() {
    if (processState != ProcessState.AVAILABLE) {
      throw new IllegalStateException();
    }
    processState = ProcessState.IN_PROGRESS;
  }


  @Override
  public void retry() {
    if (processState != ProcessState.IN_PROGRESS) {
      throw new IllegalStateException();
    }
    processState = ProcessState.AVAILABLE;
    numFailures++;
    timestamp = 0;
  }

  @Override
  public void complete() {
    if (processState != ProcessState.IN_PROGRESS) {
      throw new IllegalStateException();
    }
    processState = ProcessState.COMPLETED;
  }

  @Override
  public void discard() {
    if (processState != ProcessState.IN_PROGRESS) {
      throw new IllegalStateException();
    }
    processState = ProcessState.DISCARDED;
  }

  static DefaultConsumablePartition fromBytes(byte[] bytes) {
    ByteBuffer bb = ByteBuffer.wrap(bytes);

    ProcessState processState = ProcessState.fromByte(bb.get());
    int keyLength = bb.getInt();
    byte[] stringBytes = new byte[keyLength];
    bb.get(stringBytes, 0, keyLength);
    long timestamp = bb.getLong();
    int numFailures = bb.getInt();
    return new DefaultConsumablePartition(GSON.fromJson(Bytes.toString(stringBytes), PartitionKey.class),
                                   processState, timestamp, numFailures);
  }

  public byte[] toBytes() {
    byte[] partitionKeyBytes = Bytes.toBytes(GSON.toJson(partitionKey));
    // 1 byte for the ProcessState
    int numBytes = 1;
    numBytes += Bytes.SIZEOF_INT;
    numBytes += partitionKeyBytes.length;
    numBytes += Bytes.SIZEOF_LONG;
    numBytes += Bytes.SIZEOF_INT;

    ByteBuffer bb = ByteBuffer.allocate(numBytes);

    bb.put(processState.toByte());
    bb.putInt(partitionKeyBytes.length);
    bb.put(partitionKeyBytes);
    bb.putLong(getTimestamp());
    bb.putInt(getNumFailures());

    return bb.array();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DefaultConsumablePartition that = (DefaultConsumablePartition) o;

    return numFailures == that.numFailures && timestamp == that.timestamp
      && partitionKey.equals(that.partitionKey) && processState == that.processState;
  }

  @Override
  public int hashCode() {
    int result = partitionKey.hashCode();
    result = 31 * result + processState.hashCode();
    result = 31 * result + numFailures;
    result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
    return result;
  }
}
