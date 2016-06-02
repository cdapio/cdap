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
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.collect.AllCollector;
import co.cask.cdap.common.collect.AllPairCollector;
import co.cask.cdap.common.collect.Collector;
import co.cask.cdap.common.collect.PairCollector;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.ConsumerGroupConfig;
import co.cask.cdap.data2.transaction.queue.QueueConfigurer;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.tephra.Transaction;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Class for persisting consumer state per queue consumer.
 */
public final class HBaseConsumerStateStore extends AbstractDataset implements QueueConfigurer {

  private static final Gson GSON = new Gson();

  private final QueueName queueName;
  private final Table table;
  private final byte[] barrierScanStartRow;
  private final byte[] barrierScanEndRow;
  private Transaction transaction;

  HBaseConsumerStateStore(String datasetName, QueueName queueName, Table table) {
    super(datasetName, table);
    this.queueName = queueName;
    this.table = table;
    this.barrierScanStartRow = Bytes.add(queueName.toBytes(), QueueEntryRow.getQueueEntryRowKey(queueName, 0L, 0));
    this.barrierScanEndRow = Bytes.stopKeyForPrefix(
      Bytes.add(queueName.toBytes(), QueueEntryRow.getQueueEntryRowKey(queueName, Long.MAX_VALUE, 0)));
  }

  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    this.transaction = tx;
  }

  /**
   * Returns the internal dataset table. Only for QueueAdmin to use.
   */
  Table getInternalTable() {
    return table;
  }

  /**
   * Returns the consumer state as stored in the state store for the given consumer.
   */
  HBaseConsumerState getState(long groupId, int instanceId) {
    // Lookup the start row for the given instance, also search the barriers that bound the start row
    ConsumerState consumerState = getConsumerState(groupId, instanceId);
    QueueBarrier previousBarrier = consumerState.getPreviousBarrier();
    QueueBarrier nextBarrier = consumerState.getNextBarrier();
    if (previousBarrier == null && nextBarrier == null) {
      throw new IllegalStateException(
        String.format("Unable to find barrier information for consumer. Queue: %s, GroupId: %d, InstanceId:%d",
                      queueName, groupId, instanceId));
    }

    // There are three possible cases:
    // 1. previousBarrier == null. It means in old compat mode. Since in old queue we didn't record the
    //                             consumer group config, we assume it's the same as the one recorded by
    //                             the nextBarrier (which was written when Flow start for the first time with new queue)
    // 2. nextBarrier == null. It means pasted the last barrier. The consumer scan is unbounded
    // 3. both not null. The scan is bounded by the nextBarrier and
    //                   the consumer group config is described by the previousBarrier.
    ConsumerGroupConfig groupConfig = previousBarrier != null ? previousBarrier.getGroupConfig()
                                                             : nextBarrier.getGroupConfig();
    ConsumerConfig consumerConfig = new ConsumerConfig(groupConfig, instanceId);
    return new HBaseConsumerState(consumerConfig,
                                  consumerState.getConsumerStartRow(),
                                  previousBarrier == null ? null : previousBarrier.getStartRow(),
                                  nextBarrier == null ? null : nextBarrier.getStartRow());
  }

  /**
   * Checks if consumers in the given group have consumed all entries up to the given start row.
   *
   * @param groupId consumer group to check
   * @param minStartRow the minimum start row that the consumers has been consumed up to
   * @return true if all the required consumers has consumed up to the given start row, false otherwise
   */
  boolean isAllConsumed(long groupId, byte[] minStartRow) {
    Map<Integer, byte[]> startRows = fetchStartRows(groupId, Integer.MAX_VALUE);
    for (byte[] startRow : startRows.values()) {
      if (Bytes.compareTo(startRow, minStartRow) < 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if consumers in the given group has consumed all entries up to the given start row.
   * Only instances that has instance id the same as the given {@link ConsumerConfig} modules group size
   * will be verified.
   */
  boolean isAllConsumed(ConsumerConfig consumerConfig, byte[] minStartRow) {
    Map<Integer, byte[]> startRows = fetchStartRows(consumerConfig.getGroupId(), Integer.MAX_VALUE);
    for (Map.Entry<Integer, byte[]> entry : startRows.entrySet()) {
      if (entry.getKey() % consumerConfig.getGroupSize() != consumerConfig.getInstanceId()) {
        continue;
      }
      if (Bytes.compareTo(entry.getValue(), minStartRow) < 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Updates the start row state of the given consumer.
   */
  void updateState(long groupId, int instanceId, byte[] startRow) {
    byte[] stateColumn = getConsumerStateColumn(groupId, instanceId);
    table.put(queueName.toBytes(), stateColumn, startRow);
  }

  /**
   * Called by consumer to signal process completion up to the current barrier that the consumer is in.
   */
  void completed(long groupId, int instanceId) {
    // Get the current consumer state to get the end barrier info
    ConsumerState consumerState = getConsumerState(groupId, instanceId);
    QueueBarrier nextBarrier = consumerState.getNextBarrier();
    if (nextBarrier == null) {
      // End row shouldn't be null if this method is called
      throw new IllegalArgumentException(
        String.format("No end barrier information for consumer. Queue: %s, GroupId: %d, InstanceId: %d",
                      queueName, groupId, instanceId));
    }

    byte[] stateColumn = getConsumerStateColumn(groupId, instanceId);

    // If the instance exists in the next barrier, set the start row to the barrier start
    if (instanceId < nextBarrier.getGroupConfig().getGroupSize()) {
      table.put(queueName.toBytes(), stateColumn, nextBarrier.getStartRow());
      return;
    }

    // If the instance has been removed in the next barrier,
    // find the next start barrier that this instance needs to consume from
    try (Scanner scanner = table.scan(Bytes.add(queueName.toBytes(), nextBarrier.getStartRow()), barrierScanEndRow)) {
      Row row;
      boolean found = false;
      while (!found && (row = scanner.next()) != null) {
        QueueBarrier queueBarrier = decodeBarrierInfo(row, groupId);
        if (queueBarrier == null || instanceId >= queueBarrier.getGroupConfig().getGroupSize()) {
          continue;
        }
        table.put(queueName.toBytes(), stateColumn, queueBarrier.getStartRow());
        found = true;
      }
      if (!found) {
        // Remove the state since this consumer instance is not longer active
        table.delete(queueName.toBytes(), stateColumn);
      }
    }
  }

  /**
   * Remove all states related to the queue that this state store is representing.
   */
  void clear() {
    // Scan and delete all barrier rows
    try (Scanner scanner = table.scan(barrierScanStartRow, barrierScanEndRow)) {
      Row row = scanner.next();
      while (row != null) {
        table.delete(row.getRow());
        row = scanner.next();
      }
      // Also delete the consumer state rows
      table.delete(queueName.toBytes());
    }
  }

  @Override
  public void configureInstances(long groupId, int instances) {
    // Find the last barrier info to get the existing group config
    List<QueueBarrier> queueBarriers = scanBarriers(groupId, new AllCollector<QueueBarrier>())
                                                  .finish(new ArrayList<QueueBarrier>());
    Preconditions.checkState(!queueBarriers.isEmpty(), "No queue configuration found for group %s", groupId);
    QueueBarrier queueBarrier = queueBarriers.get(queueBarriers.size() - 1);
    ConsumerGroupConfig oldGroupConfig = queueBarrier.getGroupConfig();
    ConsumerGroupConfig groupConfig = new ConsumerGroupConfig(groupId, instances,
                                                              oldGroupConfig.getDequeueStrategy(),
                                                              oldGroupConfig.getHashKey());

    byte[] startRow = QueueEntryRow.getQueueEntryRowKey(queueName, transaction.getWritePointer(), 0);
    Put put = new Put(Bytes.add(queueName.toBytes(), startRow));
    put.add(Bytes.toBytes(groupConfig.getGroupId()), GSON.toJson(groupConfig));
    table.put(put);

    // For instances that don't have start row, set the start row to barrier start row
    // We fetches all instances here for cleanup of barrier info later.
    Map<Integer, byte[]> startRows = fetchStartRows(groupId, Integer.MAX_VALUE);
    for (int instanceId = 0; instanceId < instances; instanceId++) {
      if (!startRows.containsKey(instanceId)) {
        table.put(queueName.toBytes(), getConsumerStateColumn(groupId, instanceId), startRow);
      }
    }

    // Remove barrier info that all instances has passed the start row it records
    Deque<byte[]> deletes = Lists.newLinkedList();
    for (QueueBarrier info : queueBarriers) {
      boolean allPassed = true;
      for (byte[] instanceStartRow : startRows.values()) {
        if (Bytes.compareTo(instanceStartRow, info.getStartRow()) <= 0) {
          allPassed = false;
          break;
        }
      }
      if (!allPassed) {
        break;
      }
      deletes.add(Bytes.add(queueName.toBytes(), info.getStartRow()));
    }
    // Retain the last barrier info
    if (deletes.size() > 1) {
      deletes.removeLast();
      byte[] column = Bytes.toBytes(groupId);
      for (byte[] delete : deletes) {
        table.delete(delete, column);
      }
    }
  }

  @Override
  public void configureGroups(Iterable<? extends ConsumerGroupConfig> groupConfigs) {
    com.google.common.collect.Table<Long, Integer, byte[]> startRows = fetchAllStartRows();

    // Writes a new barrier info for all the groups
    byte[] startRow = QueueEntryRow.getQueueEntryRowKey(queueName, transaction.getWritePointer(), 0);
    Put put = new Put(Bytes.add(queueName.toBytes(), startRow));
    Set<Long> groupsIds = Sets.newHashSet();
    for (ConsumerGroupConfig groupConfig : groupConfigs) {
      long groupId = groupConfig.getGroupId();
      if (!groupsIds.add(groupId)) {
        throw new IllegalArgumentException("Same consumer group is provided multiple times");
      }
      put.add(Bytes.toBytes(groupId), GSON.toJson(groupConfig));

      // For new instance, set the start row to barrier start row
      for (int instanceId = 0; instanceId < groupConfig.getGroupSize(); instanceId++) {
        if (!startRows.contains(groupId, instanceId)) {
          table.put(queueName.toBytes(), getConsumerStateColumn(groupId, instanceId), startRow);
        }
      }
    }

    // Remove all states for groups that are removed.
    deleteRemovedGroups(table.get(queueName.toBytes()), groupsIds);

    // Remove all barriers for groups that are removed.
    // Also remove barriers that have all consumers consumed pass that barrier
    // Multimap from groupId to barrier start rows. Ordering need to be maintained as the scan order.
    Multimap<Long, byte[]> deletes = LinkedHashMultimap.create();
    try (Scanner scanner = table.scan(barrierScanStartRow, barrierScanEndRow)) {
      Row row = scanner.next();
      while (row != null) {
        deleteRemovedGroups(row, groupsIds);

        // Check all instances in all groups
        for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
          QueueBarrier barrier = decodeBarrierInfo(row.getRow(), entry.getValue());
          if (barrier == null) {
            continue;
          }
          long groupId = barrier.getGroupConfig().getGroupId();
          boolean delete = true;
          // Check if all instances in a group has consumed passed the current barrier
          for (int instanceId = 0; instanceId < barrier.getGroupConfig().getGroupSize(); instanceId++) {
            byte[] consumerStartRow = startRows.get(groupId, instanceId);
            if (consumerStartRow == null || Bytes.compareTo(consumerStartRow, barrier.getStartRow()) < 0) {
              delete = false;
              break;
            }
          }
          if (delete) {
            deletes.put(groupId, row.getRow());
          }
        }
        row = scanner.next();
      }
    }

    // Remove barries that have all consumers consumed passed it
    for (Map.Entry<Long, Collection<byte[]>> entry : deletes.asMap().entrySet()) {
      // Retains the last barrier info
      if (entry.getValue().size() <= 1) {
        continue;
      }
      Deque<byte[]> rows = Lists.newLinkedList(entry.getValue());
      rows.removeLast();
      byte[] groupColumn = Bytes.toBytes(entry.getKey());
      for (byte[] rowKey : rows) {
        table.delete(rowKey, groupColumn);
      }
    }

    table.put(put);
  }

  private void deleteRemovedGroups(Row row, Set<Long> existingGroups) {
    for (byte[] column : row.getColumns().keySet()) {
      long groupId = getGroupIdFromColumn(column);
      if (!existingGroups.contains(groupId)) {
        table.delete(row.getRow(), column);
      }
    }
  }

  public Transaction getTransaction() {
    return transaction;
  }

  /**
   * Gets all barrier information for the given group. The information are sorted in the order of
   * the barrier changes.
   */
  public List<QueueBarrier> getAllBarriers(long groupId) {
    return scanBarriers(groupId, new AllCollector<QueueBarrier>()).finish(new ArrayList<QueueBarrier>());
  }

  /**
   * Gets all barrier information for all groups. The information are sorted in the order of
   * the barrier changes.
   */
  public Multimap<Long, QueueBarrier> getAllBarriers() {
    return scanBarriers(new AllPairCollector<Long, QueueBarrier>()).finishMultimap(
      LinkedHashMultimap.<Long, QueueBarrier>create());
  }

  void getLatestConsumerGroups(Collection<? super ConsumerGroupConfig> result) {
    try (Scanner scanner = table.scan(barrierScanStartRow, barrierScanEndRow)) {
      // Get the last row
      Row lastRow = null;
      Row row = scanner.next();
      while (row != null) {
        lastRow = row;
        row = scanner.next();
      }

      if (lastRow == null) {
        throw new IllegalStateException("No consumer group information. Queue: " + queueName);
      }

      for (Map.Entry<byte[], byte[]> entry : lastRow.getColumns().entrySet()) {
        result.add(GSON.fromJson(new String(entry.getValue(), Charsets.UTF_8), ConsumerGroupConfig.class));
      }
    }
  }

  private PairCollector<Long, QueueBarrier> scanBarriers(PairCollector<Long, QueueBarrier> collector) {
    try (Scanner scanner = table.scan(barrierScanStartRow, barrierScanEndRow)) {
      Row row;
      while ((row = scanner.next()) != null) {
        Map<Long, QueueBarrier> info = decodeBarrierInfo(row);
        if (info != null) {
          for (Map.Entry<Long, QueueBarrier> entry : info.entrySet()) {
            if (!collector.addElement(entry)) {
              return collector;
            }
          }
        }
      }
    }
    return collector;
  }

  private Collector<QueueBarrier> scanBarriers(long groupId, Collector<QueueBarrier> collector) {
    try (Scanner scanner = table.scan(barrierScanStartRow, barrierScanEndRow)) {
      Row row;
      while ((row = scanner.next()) != null) {
        QueueBarrier info = decodeBarrierInfo(row, groupId);
        if (info != null && !collector.addElement(info)) {
          break;
        }
      }
    }
    return collector;
  }

  /**
   * Fetches start row states of all instances for a given consumer group.
   *
   * @param groupId consumer group Id
   * @param instances number of instances in the consumer group
   * @return a map from instanceId to start row state
   */
  private Map<Integer, byte[]> fetchStartRows(long groupId, int instances) {
    // Gets all columns with the groupId as prefix
    int stopInstanceId = (instances == Integer.MAX_VALUE) ? Integer.MAX_VALUE : instances + 1;
    Row row = table.get(queueName.toBytes(),
                        getConsumerStateColumn(groupId, 0),
                        getConsumerStateColumn(groupId, stopInstanceId), instances);
    return getStartRowsFromColumns(row.getColumns());
  }

  private com.google.common.collect.Table<Long, Integer, byte[]> fetchAllStartRows() {
    com.google.common.collect.Table<Long, Integer, byte[]> result = HashBasedTable.create();
    Row row = table.get(queueName.toBytes());
    for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
      long groupId = getGroupIdFromColumn(entry.getKey());
      int instanceId = getInstanceIdFromColumn(entry.getKey());
      result.put(groupId, instanceId, entry.getValue());
    }

    return result;
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

  private ConsumerState getConsumerState(long groupId, int instanceId) {
    byte[] stateColumn = getConsumerStateColumn(groupId, instanceId);
    byte[] startRow = table.get(queueName.toBytes(), stateColumn);
    if (startRow == null) {
      // Should not be null
      throw new IllegalStateException(
        String.format("Unable to find consumer state. Queue: %s, GroupId: %d, InstanceId: %d",
                      queueName, groupId, instanceId));
    }

    // Find the barrier info for this consumer
    Queue<QueueBarrier> queueBarriers = scanBarriers(groupId, new BarrierInfoCollector(startRow))
      .finish(new LinkedList<QueueBarrier>());
    if (queueBarriers.isEmpty()) {
      throw new IllegalStateException(
        String.format("Unable to barrier info for consumer. Queue: %s, GroupId: %d, InstanceId: %d, StartRow: %s",
                      queueName, groupId, instanceId, Bytes.toStringBinary(startRow)));
    }

    QueueBarrier queueBarrier = queueBarriers.poll();
    if (Bytes.compareTo(queueBarrier.getStartRow(), startRow) > 0) {
      // If the barrier info is > consumer start row, it is the end info and the start info should be null
      return new ConsumerState(startRow, null, queueBarrier);
    }
    return new ConsumerState(startRow, queueBarrier, queueBarriers.poll());
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

  private Map<Long, QueueBarrier> decodeBarrierInfo(Row row) {
    Map<Long, QueueBarrier> barrierInfo = Maps.newHashMap();
    Map<byte[], byte[]> columns = row.getColumns();
    for (byte[] columnKey : columns.keySet()) {
      byte[] groupInfo = row.get(columnKey);
      QueueBarrier barrier = decodeBarrierInfo(row.getRow(), groupInfo);
      if (barrier != null) {
        barrierInfo.put(barrier.getGroupConfig().getGroupId(), barrier);
      }
    }
    return barrierInfo;
  }

  @Nullable
  private QueueBarrier decodeBarrierInfo(Row row, long groupId) {
    return decodeBarrierInfo(row.getRow(), row.get(Bytes.toBytes(groupId)));
  }

  @Nullable
  private QueueBarrier decodeBarrierInfo(byte[] rowKey, @Nullable byte[] groupInfo) {
    if (groupInfo == null) {
      return null;
    }
    ConsumerGroupConfig groupConfig = GSON.fromJson(new String(groupInfo, Charsets.UTF_8), ConsumerGroupConfig.class);
    byte[] startRow = Arrays.copyOfRange(rowKey, queueName.toBytes().length, rowKey.length);
    return new QueueBarrier(groupConfig, startRow);
  }

  /**
   * Internal representation of consumer state.
   */
  private static final class ConsumerState {
    private final byte[] consumerStartRow;
    private final QueueBarrier previousBarrier;
    private final QueueBarrier nextBarrier;

    private ConsumerState(byte[] consumerStartRow,
                          @Nullable QueueBarrier previousBarrier, @Nullable QueueBarrier nextBarrier) {
      this.consumerStartRow = consumerStartRow;
      this.previousBarrier = previousBarrier;
      this.nextBarrier = nextBarrier;
    }

    public byte[] getConsumerStartRow() {
      return consumerStartRow;
    }

    @Nullable
    public QueueBarrier getNextBarrier() {
      return nextBarrier;
    }

    @Nullable
    public QueueBarrier getPreviousBarrier() {
      return previousBarrier;
    }
  }

  /**
   * A collector for collection start and end barriers that bound the given queue entry row.
   * If the queue entry row has both start and end barriers, then two {@link QueueBarrier} will
   * be added to the collection in the {@link #finish} method. If only start or end barrier exist,
   * then only one {@link QueueBarrier} will be added.
   */
  private static final class BarrierInfoCollector implements Collector<QueueBarrier> {

    private final byte[] startRow;
    private final Deque<QueueBarrier> infos = Lists.newLinkedList();

    private BarrierInfoCollector(byte[] startRow) {
      this.startRow = startRow;
    }

    @Override
    public boolean addElement(QueueBarrier queueBarrier) {
      if (Bytes.compareTo(startRow, queueBarrier.getStartRow()) < 0) {
        infos.add(queueBarrier);
        return false;
      }

      infos.poll();
      infos.add(queueBarrier);

      return true;
    }

    @Override
    public <T extends Collection<? super QueueBarrier>> T finish(T collection) {
      collection.addAll(infos);
      return collection;
    }
  }
}
