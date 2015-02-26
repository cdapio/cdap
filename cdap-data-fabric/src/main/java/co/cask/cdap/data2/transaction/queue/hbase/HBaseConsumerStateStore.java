/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.transaction.queue.QueueConfigurer;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.util.Map;

/**
 * Class for persisting consumer state per queue consumer.
 */
final class HBaseConsumerStateStore extends AbstractDataset implements QueueConfigurer {

  private final QueueName queueName;
  private final Table table;
  private final byte[] minStartRow;

  HBaseConsumerStateStore(QueueName queueName, Table table) {
    super(Constants.SYSTEM_NAMESPACE + "." + QueueConstants.QueueType.QUEUE, table);
    this.queueName = queueName;
    this.table = table;
    this.minStartRow = QueueEntryRow.getQueueEntryRowKey(queueName, 0L, 0);
  }

  /**
   * Returns the consumer state as stored in the state store for the given consumer.
   */
  HBaseConsumerState getState(long groupId, int instanceId) {
    byte[] stateColumn = getConsumerStateColumn(groupId, instanceId);
    byte[] startRow = table.get(queueName.toBytes(), stateColumn);
    Preconditions.checkState(startRow != null,
                             "No consumer state found. GroupId: %s, InstanceId: %s", groupId, instanceId);

    return new HBaseConsumerState(startRow, groupId, instanceId);
  }

  /**
   * Updates the start row state of the given consumer.
   */
  void updateState(long groupId, int instanceId, byte[] startRow) {
    byte[] stateColumn = getConsumerStateColumn(groupId, instanceId);
    table.put(queueName.toBytes(), stateColumn, startRow);
  }

  /**
   * Remove all states related to the queue that this state store is representing.
   */
  void clear() {
    // Scan and delete all rows prefix with queueName
    Scanner scanner = table.scan(queueName.toBytes(), Bytes.stopKeyForPrefix(queueName.toBytes()));
    Row row = scanner.next();
    while (row != null) {
      table.delete(row.getRow());
      row = scanner.next();
    }
  }

  /**
   * Changes the number of consumer instances of the given consumer group. The consumer group configuration needs to be
   * existed already.
   *
   * @param groupId groupId of the consumer group
   * @param instances number of instances to change to
   */
  @Override
  public void configureInstances(long groupId, int instances) {
    // Get all latest entry row key of all existing instances
    Map<Integer, byte[]> startRows = fetchStartRows(groupId);
    int oldInstances = startRows.size();

    // Nothing to do if size doesn't change
    if (oldInstances == instances) {
      return;
    }
    // Compute and applies changes
    updateGroupInstances(groupId, instances, startRows);
  }

  /**
   * Changes the configuration of all consumer groups. For groups with existing states but not in the given group info,
   * their states will be removed.
   *
   * @param groupInfo map from groupId to number of instances for that group
   */
  @Override
  public void configureGroups(Map<Long, Integer> groupInfo) {
    byte[] rowKey = queueName.toBytes();
    Row row = table.get(rowKey);

    // Generate existing groupInfo, also find smallest rowKey from existing group if there is any
    Map<byte[], byte[]> columns = row.getColumns();
    if (columns == null) {
      columns = ImmutableSortedMap.of();
    }
    Map<Long, Integer> oldGroupInfo = Maps.newHashMap();
    byte[] smallest = decodeGroupInfo(groupInfo, columns, oldGroupInfo);

    // For groups that are removed, simply delete the columns
    for (long removeGroupId : Sets.difference(oldGroupInfo.keySet(), groupInfo.keySet())) {
      for (int instanceId = 0; instanceId < oldGroupInfo.get(removeGroupId); instanceId++) {
        table.delete(rowKey, getConsumerStateColumn(removeGroupId, instanceId));
      }
    }

    // For each group that changed (either a new group or number of instances change), update the startRow
    for (Map.Entry<Long, Integer> entry : groupInfo.entrySet()) {
      long groupId = entry.getKey();
      int instances = entry.getValue();
      if (!oldGroupInfo.containsKey(groupId)) {
        // For new group, simply put with smallest rowKey from other group or an empty byte array if none exists.
        for (int i = 0; i < instances; i++) {
          table.put(rowKey, getConsumerStateColumn(groupId, i), smallest == null ? minStartRow : smallest);
        }
      } else if (oldGroupInfo.get(groupId) != instances) {
        // compute the mutations needed using the change instances logic
        updateGroupInstances(groupId, instances, getStartRowsFromColumns(filterByGroup(columns, groupId)));
      }
    }
  }

  /**
   * Fetches start row states of all instances for a given consumer group.
   *
   * @param groupId consumer group Id
   * @return a map from instanceId to start row state
   */
  private Map<Integer, byte[]> fetchStartRows(long groupId) {
    // Gets all columns with the groupId as prefix
    Row row = table.get(queueName.toBytes(),
                        getConsumerStateColumn(groupId, 0),
                        getConsumerStateColumn(groupId, Integer.MAX_VALUE), Integer.MAX_VALUE);
    return getStartRowsFromColumns(row.getColumns());
  }

  /**
   * Decodes start row states from the given column map.
   *
   * @return a Map from instanceId to start row
   */
  private Map<Integer, byte[]> getStartRowsFromColumns(Map<byte[], byte[]> columns) {
    ImmutableMap.Builder<Integer, byte[]> startRows = ImmutableMap.builder();
    for (Map.Entry<byte[], byte[]> entry : columns.entrySet()) {
      startRows.put(getInstanceIdFromColumn(entry.getKey()), entry.getValue());
    }
    return startRows.build();
  }

  private Map<byte[], byte[]> filterByGroup(Map<byte[], byte[]> columns, final long groupId) {
    return Maps.filterKeys(columns, new Predicate<byte[]>() {
      @Override
      public boolean apply(byte[] column) {
        return groupId == getGroupIdFromColumn(column);
      }
    });
  }

  private long getGroupIdFromColumn(byte[] column) {
    return Bytes.toLong(column);
  }

  private int getInstanceIdFromColumn(byte[] column) {
    return Bytes.toInt(column, Bytes.SIZEOF_LONG);
  }

  /**
   * Returns the column qualifier for the consumer state column. The qualifier is formed by
   * {@code <groupId><instanceId>}.
   * @param groupId Group ID of the consumer
   * @param instanceId Instance ID of the consumer
   * @return A new byte[] which is the column qualifier.
   */
  private byte[] getConsumerStateColumn(long groupId, int instanceId) {
    byte[] column = new byte[Longs.BYTES + Ints.BYTES];
    Bytes.putLong(column, 0, groupId);
    Bytes.putInt(column, Longs.BYTES, instanceId);
    return column;
  }

  /**
   * Update number of instances for the given group.
   *
   * @param groupId consumer group Id
   * @param instances number of consumer instances in the group
   * @param startRows existing consumers start row states
   */
  private void updateGroupInstances(long groupId, int instances, Map<Integer, byte[]> startRows) {
    // Find smallest startRow among existing instances
    byte[] smallest = null;
    for (byte[] startRow : startRows.values()) {
      if (smallest == null || Bytes.BYTES_COMPARATOR.compare(startRow, smallest) < 0) {
        smallest = startRow;
      }
    }
    Preconditions.checkArgument(smallest != null, "No startRow found for consumer group %s", groupId);

    int oldInstances = startRows.size();

    // When group size changed, reset all instances startRow to smallest startRow
    byte[] rowKey = queueName.toBytes();
    for (Map.Entry<Integer, byte[]> entry : startRows.entrySet()) {
      int instanceId = entry.getKey();
      byte[] stateColumn = getConsumerStateColumn(groupId, instanceId);
      if (instanceId < instances) {
        // Updates to smallest rowKey
        table.put(rowKey, stateColumn, smallest);
      } else {
        // Delete old instances
        table.delete(rowKey, stateColumn);
      }
    }
    // For all new instances, set startRow to smallest
    for (int instanceId = oldInstances; instanceId < instances; instanceId++) {
      table.put(rowKey, getConsumerStateColumn(groupId, instanceId), smallest);
    }
  }

  /**
   * Decodes group information from the given column values.
   *
   * @param groupInfo The current groupInfo
   * @param columns Map from column name (groupId + instanceId) to column value (start row) to decode
   * @param oldGroupInfo The map to store the decoded group info
   * @return the smallest start row among all the decoded value if the groupId exists in the current groupInfo
   */
  private byte[] decodeGroupInfo(Map<Long, Integer> groupInfo,
                                 Map<byte[], byte[]> columns, Map<Long, Integer> oldGroupInfo) {
    byte[] smallest = null;

    for (Map.Entry<byte[], byte[]> entry : columns.entrySet()) {
      // Consumer state column is named as "<groupId><instanceId>"
      long groupId = getGroupIdFromColumn(entry.getKey());

      // Map key is sorted by groupId then instanceId, hence keep putting the instance + 1 will gives the group size.
      oldGroupInfo.put(groupId, getInstanceIdFromColumn(entry.getKey()) + 1);

      // Update smallest if the group still exists from the new groups.
      if (groupInfo.containsKey(groupId)
        && (smallest == null || Bytes.BYTES_COMPARATOR.compare(entry.getValue(), smallest) < 0)) {
        smallest = entry.getValue();
      }
    }
    return smallest;
  }
}
