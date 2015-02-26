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
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Class for persisting consumer state per queue consumer.
 */
final class HBaseConsumerStateStore implements Closeable {

  private final QueueName queueName;
  private final HTable hTable;

  HBaseConsumerStateStore(QueueName queueName, HTable hTable) {
    this.queueName = queueName;
    this.hTable = hTable;
  }

  /**
   * Returns the start row as stored in the state store.
   */
  HBaseConsumerState getState(long groupId, int instanceId) throws IOException {
    return new HBaseConsumerState(fetchStartRow(groupId, instanceId), groupId, instanceId);
  }

  /**
   * Updates the start row state of the given consumer.
   */
  void updateState(long groupId, int instanceId, byte[] startRow) throws IOException {
    byte[] stateColumn = getConsumerStateColumn(groupId, instanceId);

    Put put = new Put(queueName.toBytes());
    put.add(QueueEntryRow.COLUMN_FAMILY, stateColumn, startRow);
    hTable.put(put);
    hTable.flushCommits();
  }

  /**
   * Changes the number of consumer instances of the given consumer group. The consumer group configuration needs to be
   * existed already.
   *
   * @param groupId groupId of the consumer group
   * @param instances number of instances to change to
   * @throws Exception if failed to change number of instances.
   */
  void configureInstances(long groupId, int instances) throws Exception {
    byte[] rowKey = queueName.toBytes();

    // Get all latest entry row key of all existing instances
    Map<Integer, byte[]> startRows = fetchStartRows(groupId);
    int oldInstances = startRows.size();

    // Nothing to do if size doesn't change
    if (oldInstances == instances) {
      return;
    }
    // Compute and applies changes
    hTable.batch(getConfigMutations(groupId, instances, rowKey, startRows, new ArrayList<Mutation>()));
    hTable.flushCommits();
  }

  /**
   * Changes the configuration of all consumer groups. For groups with existing states but not in the given group info,
   * their states will be removed.
   *
   * @param groupInfo map from groupId to number of instances for that group
   * @throws Exception if failed to change consumer group configuration
   */
  void configureGroups(Map<Long, Integer> groupInfo) throws Exception {
    byte[] rowKey = queueName.toBytes();

    // Get the whole row
    Result result = hTable.get(new Get(rowKey));

    // Generate existing groupInfo, also find smallest rowKey from existing group if there is any
    NavigableMap<byte[], byte[]> columns = result.getFamilyMap(QueueEntryRow.COLUMN_FAMILY);
    if (columns == null) {
      columns = ImmutableSortedMap.of();
    }
    Map<Long, Integer> oldGroupInfo = Maps.newHashMap();
    byte[] smallest = decodeGroupInfo(groupInfo, columns, oldGroupInfo);

    List<Mutation> mutations = Lists.newArrayList();

    // For groups that are removed, simply delete the columns
    Sets.SetView<Long> removedGroups = Sets.difference(oldGroupInfo.keySet(), groupInfo.keySet());
    if (!removedGroups.isEmpty()) {
      Delete delete = new Delete(rowKey);
      for (long removeGroupId : removedGroups) {
        for (int i = 0; i < oldGroupInfo.get(removeGroupId); i++) {
          delete.deleteColumns(QueueEntryRow.COLUMN_FAMILY,
                               getConsumerStateColumn(removeGroupId, i));
        }
      }
      if (!delete.isEmpty()) {
        mutations.add(delete);
      }
    }

    // For each group that changed (either a new group or number of instances change), update the startRow
    Put put = new Put(rowKey);
    for (Map.Entry<Long, Integer> entry : groupInfo.entrySet()) {
      long groupId = entry.getKey();
      int instances = entry.getValue();
      if (!oldGroupInfo.containsKey(groupId)) {
        // For new group, simply put with smallest rowKey from other group or an empty byte array if none exists.
        for (int i = 0; i < instances; i++) {
          put.add(QueueEntryRow.COLUMN_FAMILY, getConsumerStateColumn(groupId, i),
                  smallest == null ? Bytes.EMPTY_BYTE_ARRAY : smallest);
        }
      } else if (oldGroupInfo.get(groupId) != instances) {
        // compute the mutations needed using the change instances logic
        Map<byte[], byte[]> startRows = columns.subMap(getConsumerStateColumn(groupId, 0),
                                                       getConsumerStateColumn(groupId, oldGroupInfo.get(groupId)));
        mutations = getConfigMutations(groupId, instances, rowKey, getStartRowsFromColumns(startRows), mutations);
      }
    }
    if (!put.isEmpty()) {
      mutations.add(put);
    }

    // Compute and applies changes
    if (!mutations.isEmpty()) {
      hTable.batch(mutations);
      hTable.flushCommits();
    }
  }

  @Override
  public void close() throws IOException {
    hTable.close();
  }

  /**
   * Fetches the start row state for the given consumer instance.
   */
  private byte[] fetchStartRow(long groupId, int instanceId) throws IOException {
    byte[] stateColumn = getConsumerStateColumn(groupId, instanceId);

    Get get = new Get(queueName.toBytes());
    get.addColumn(QueueEntryRow.COLUMN_FAMILY, stateColumn);
    get.setMaxVersions(1);

    Result result = hTable.get(get);
    if (result.isEmpty()) {
      throw new IOException("Failed to load consumer state. GroupId: " + groupId + ", InstanceId: " + instanceId);
    }
    return result.getValue(QueueEntryRow.COLUMN_FAMILY, stateColumn);
  }

  /**
   * Fetches start row states of all instances for a given consumer group.
   *
   * @param groupId consumer group Id
   * @return a map from instanceId to start row state
   */
  private Map<Integer, byte[]> fetchStartRows(long groupId) throws IOException {
    Get get = new Get(queueName.toBytes());
    get.addFamily(QueueEntryRow.COLUMN_FAMILY);
    get.setFilter(new ColumnPrefixFilter(Bytes.toBytes(groupId)));
    get.setMaxVersions(1);
    Result result = hTable.get(get);

    if (result.isEmpty()) {
      return ImmutableMap.of();
    }

    return getStartRowsFromColumns(result.getFamilyMap(QueueEntryRow.COLUMN_FAMILY));
  }

  /**
   * Decodes start row states from the given column map.
   */
  private Map<Integer, byte[]> getStartRowsFromColumns(Map<byte[], byte[]> columns) {
    ImmutableMap.Builder<Integer, byte[]> startRows = ImmutableMap.builder();
    for (Map.Entry<byte[], byte[]> entry : columns.entrySet()) {
      startRows.put(getInstanceIdFromColumn(entry.getKey()), entry.getValue());
    }
    return startRows.build();
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
   * Computes the mutations needed for the given consumer group.
   *
   * @param groupId consumer group Id
   * @param instances number of consumer instances in the group
   * @param rowKey row key to used for all the mutations
   * @param startRows existing consumers start row states
   * @param mutations List for storing the resulting mutations
   */
  private List<Mutation> getConfigMutations(long groupId, int instances, byte[] rowKey,
                                            Map<Integer, byte[]> startRows, List<Mutation> mutations) {
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
    Put put = new Put(rowKey);
    Delete delete = new Delete(rowKey);
    for (Map.Entry<Integer, byte[]> entry : startRows.entrySet()) {
      int instanceId = entry.getKey();
      byte[] stateColumn = getConsumerStateColumn(groupId, instanceId);
      if (instanceId < instances) {
        // Updates to smallest rowKey
        put.add(QueueEntryRow.COLUMN_FAMILY, stateColumn, smallest);
      } else {
        // Delete old instances
        delete.deleteColumns(QueueEntryRow.COLUMN_FAMILY, stateColumn);
      }
    }
    // For all new instances, set startRow to smallest
    for (int i = oldInstances; i < instances; i++) {
      put.add(QueueEntryRow.COLUMN_FAMILY, getConsumerStateColumn(groupId, i), smallest);
    }
    if (!put.isEmpty()) {
      mutations.add(put);
    }
    if (!delete.isEmpty()) {
      mutations.add(delete);
    }

    return mutations;
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
