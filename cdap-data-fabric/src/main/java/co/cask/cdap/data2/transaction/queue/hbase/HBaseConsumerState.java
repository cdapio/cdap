/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.data2.transaction.queue.hbase;

import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * Represents state of queue consumer.
 */
public final class HBaseConsumerState {

  // It's intentionally not using Bytes.EMPTY_BYTE_ARRAY so that this class can be used by CoProcessor.
  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final int LONG_BYTES = Long.SIZE / Byte.SIZE;

  private final long groupId;
  private final int instanceId;
  private final byte[] startRow;
  private final byte[] consumerStateColumn;

  /**
   * Creates a list of {@link HBaseConsumerState} instance based on the given HBase result.
   */
  public static List<HBaseConsumerState> create(Result result) {
    return create(result.getFamilyMap(QueueEntryRow.COLUMN_FAMILY));
  }

  /**
   * Creates a list of {@link HBaseConsumerState} instance based on the given state map.
   */
  public static List<HBaseConsumerState> create(SortedMap<byte[], byte[]> stateMap) {
    List<HBaseConsumerState> states = new ArrayList<HBaseConsumerState>(stateMap.size());
    for (Map.Entry<byte[], byte[]> entry : stateMap.entrySet()) {
      // Intentionally using HBase Bytes.
      long groupId = Bytes.toLong(entry.getKey());
      int instanceId = Bytes.toInt(entry.getKey(), LONG_BYTES);
      states.add(new HBaseConsumerState(entry.getValue(), groupId, instanceId));
    }
    return states;
  }

  /**
   * Creates an instance based on the given HBase result for the given groupId and instanceId.
   */
  public HBaseConsumerState(Result result, long groupId, int instanceId) {
    this.groupId = groupId;
    this.instanceId = instanceId;
    this.consumerStateColumn = HBaseQueueAdmin.getConsumerStateColumn(groupId, instanceId);
    KeyValue keyValue = result.getColumnLatest(QueueEntryRow.COLUMN_FAMILY, consumerStateColumn);
    this.startRow = keyValue == null ?  EMPTY_BYTES : keyValue.getValue();
  }

  public HBaseConsumerState(byte[] startRow, long groupId, int instanceId) {
    this.startRow = startRow;
    this.groupId = groupId;
    this.instanceId = instanceId;
    this.consumerStateColumn = HBaseQueueAdmin.getConsumerStateColumn(groupId, instanceId);
  }

  public byte[] getStartRow() {
    return startRow;
  }

  public long getGroupId() {
    return groupId;
  }

  public int getInstanceId() {
    return instanceId;
  }

  /**
   * Updates {@link Put} action for updating the state to HBase.
   *
   * @param put The update will be modified to the given instance.
   */
  public Put updatePut(Put put) {
    put.add(QueueEntryRow.COLUMN_FAMILY, consumerStateColumn, startRow);
    return put;
  }

  /**
   * Adds a delete of this consumer state to the given Delete object.
   */
  public Delete delete(Delete delete) {
    delete.deleteColumns(QueueEntryRow.COLUMN_FAMILY, consumerStateColumn);
    return delete;
  }
}
