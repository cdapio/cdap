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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.DequeueResult;
import co.cask.cdap.data2.transaction.queue.AbstractQueueConsumer;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.queue.QueueScanner;
import co.cask.tephra.Transaction;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Queue consumer for HBase.
 */
abstract class HBaseQueueConsumer extends AbstractQueueConsumer {

  private final HTable hTable;
  private final HBaseConsumerState state;
  private final HBaseConsumerStateStore stateStore;
  private final byte[] queueRowPrefix;
  private final HBaseQueueStrategy queueStrategy;
  private boolean closed;
  private boolean canConsume;
  private boolean completed;

  /**
   * Creates a HBaseQueue2Consumer.
   *
   * @param hTable The HTable instance to use for communicating with HBase. This consumer is responsible for closing it.
   * @param queueName Name of the queue.
   * @param consumerState The persisted state of this consumer.
   * @param stateStore The store for persisting state for this consumer.
   */
  HBaseQueueConsumer(CConfiguration cConf, HTable hTable, QueueName queueName,
                     HBaseConsumerState consumerState, HBaseConsumerStateStore stateStore,
                     HBaseQueueStrategy queueStrategy) {
    // For HBase, eviction is done at table flush time, hence no QueueEvictor is needed.
    super(cConf, consumerState.getConsumerConfig(), queueName, consumerState.getStartRow());
    this.hTable = hTable;
    this.state = consumerState;
    this.stateStore = stateStore;
    this.queueRowPrefix = QueueEntryRow.getQueueRowPrefix(queueName);
    this.queueStrategy = queueStrategy;
    this.canConsume = false;
  }

  @Override
  public DequeueResult<byte[]> dequeue(int maxBatchSize) throws IOException {
    DequeueResult<byte[]> result = super.dequeue(maxBatchSize);

    if (canConsume && result.isEmpty() && state.getNextBarrier() != null) {
      long groupId = state.getConsumerConfig().getGroupId();
      int instanceId = state.getConsumerConfig().getInstanceId();
      stateStore.completed(groupId, instanceId);
      completed = true;
    }

    return result;
  }

  @Override
  protected boolean claimEntry(byte[] rowKey, byte[] claimedStateValue) throws IOException {
    Put put = new Put(queueStrategy.getActualRowKey(getConfig(), rowKey));
    put.add(QueueEntryRow.COLUMN_FAMILY, stateColumnName, claimedStateValue);
    return hTable.checkAndPut(put.getRow(), QueueEntryRow.COLUMN_FAMILY,
                              stateColumnName, null, put);
  }

  @Override
  protected void updateState(Set<byte[]> rowKeys, byte[] stateColumnName, byte[] stateContent) throws IOException {
    if (rowKeys.isEmpty()) {
      return;
    }
    List<Put> puts = Lists.newArrayListWithCapacity(rowKeys.size());
    for (byte[] rowKey : rowKeys) {
      Put put = new Put(queueStrategy.getActualRowKey(getConfig(), rowKey));
      put.add(QueueEntryRow.COLUMN_FAMILY, stateColumnName, stateContent);
      puts.add(put);
    }
    hTable.put(puts);
    hTable.flushCommits();
  }

  @Override
  protected void undoState(Set<byte[]> rowKeys, byte[] stateColumnName) throws IOException, InterruptedException {
    if (rowKeys.isEmpty()) {
      return;
    }
    List<Row> ops = Lists.newArrayListWithCapacity(rowKeys.size());
    for (byte[] rowKey : rowKeys) {
      Delete delete = new Delete(queueStrategy.getActualRowKey(getConfig(), rowKey));
      delete.deleteColumns(QueueEntryRow.COLUMN_FAMILY, stateColumnName);
      ops.add(delete);
    }
    hTable.batch(ops);
    hTable.flushCommits();
  }

  @Override
  protected QueueScanner getScanner(byte[] startRow, byte[] stopRow, int numRows) throws IOException {
    if (!canConsume) {
      // Need to wait if nothing every consumers reached the last barrier
      byte[] barrierStartRow = state.getPreviousBarrier();
      canConsume = barrierStartRow == null || stateStore.isAllConsumed(getConfig().getGroupId(), barrierStartRow);
      if (!canConsume) {
        return QueueScanner.EMPTY;
      }
    }

    Scan scan = createScan(startRow, getScanStopRow(stopRow), numRows);

    /** TODO: Remove when {@link DequeueScanAttributes#ATTR_QUEUE_ROW_PREFIX} is removed. It is for transition. **/
    DequeueScanAttributes.setQueueRowPrefix(scan, queueRowPrefix);
    DequeueScanAttributes.set(scan, getConfig());
    DequeueScanAttributes.set(scan, transaction);

    return queueStrategy.createScanner(getConfig(), hTable, scan, numRows);
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    Closeables.closeQuietly(queueStrategy);
    Closeables.closeQuietly(stateStore);
    Closeables.closeQuietly(hTable);
  }

  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    stateStore.startTx(tx);
    completed = false;
  }

  @Override
  public boolean rollbackTx() throws Exception {
    boolean result = super.rollbackTx();
    return stateStore.rollbackTx() && result;
  }

  @Override
  public boolean commitTx() throws Exception {
    return super.commitTx() && stateStore.commitTx();
  }

  @Override
  public void postTxCommit() {
    stateStore.postTxCommit();
    if (completed) {
      Closeables.closeQuietly(this);
    }
  }

  boolean isClosed() {
    return closed;
  }

  @Override
  protected void updateStartRow(byte[] startRow) {
    if (canConsume && !completed) {
      ConsumerConfig consumerConfig = getConfig();
      stateStore.updateState(consumerConfig.getGroupId(), consumerConfig.getInstanceId(), startRow);
    }
  }

  protected abstract Scan createScan(byte[] startRow, byte[] stopRow, int numRows);

  private byte[] getScanStopRow(byte[] stopRow) {
    byte[] barrierEndRow = state.getNextBarrier();
    return barrierEndRow == null || Bytes.compareTo(stopRow, barrierEndRow) < 0 ? stopRow : barrierEndRow;
  }
}
