/*
 * Copyright 2014 Cask, Inc.
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
import co.cask.cdap.data2.queue.QueueEntry;
import co.cask.cdap.data2.transaction.queue.AbstractQueueProducer;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.queue.QueueMetrics;
import com.continuuity.tephra.Transaction;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 *
 */
public final class HBaseQueueProducer extends AbstractQueueProducer implements Closeable {

  private final byte[] queueRowPrefix;
  private final HTable hTable;
  private final List<byte[]> rollbackKeys;

  public HBaseQueueProducer(HTable hTable, QueueName queueName, QueueMetrics queueMetrics) {
    super(queueMetrics, queueName);
    this.queueRowPrefix = QueueEntryRow.getQueueRowPrefix(queueName);
    this.rollbackKeys = Lists.newArrayList();
    this.hTable = hTable;
  }

  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    rollbackKeys.clear();
  }

  @Override
  public void close() throws IOException {
    hTable.close();
  }

  /**
   * Persist queue entries into HBase.
   */
  protected int persist(Iterable<QueueEntry> entries, Transaction transaction) throws IOException {
    long writePointer = transaction.getWritePointer();
    byte[] rowKeyPrefix = Bytes.add(queueRowPrefix, Bytes.toBytes(writePointer));
    int count = 0;
    List<Put> puts = Lists.newArrayList();
    int bytes = 0;

    for (QueueEntry entry : entries) {
      // Row key = queue_name + writePointer + counter
      byte[] rowKey = Bytes.add(rowKeyPrefix, Bytes.toBytes(count++));
      rowKey = HBaseQueueAdmin.ROW_KEY_DISTRIBUTOR.getDistributedKey(rowKey);

      rollbackKeys.add(rowKey);
      // No need to write ts=writePointer, as the row key already contains the writePointer
      Put put = new Put(rowKey);
      put.add(QueueEntryRow.COLUMN_FAMILY,
              QueueEntryRow.DATA_COLUMN,
              entry.getData());
      put.add(QueueEntryRow.COLUMN_FAMILY,
              QueueEntryRow.META_COLUMN,
              QueueEntry.serializeHashKeys(entry.getHashKeys()));

      puts.add(put);

      bytes += entry.getData().length;
    }
    hTable.put(puts);
    hTable.flushCommits();

    return bytes;
  }

  @Override
  protected void doRollback() throws Exception {
    // If nothing to rollback, simply return
    if (rollbackKeys.isEmpty()) {
      return;
    }

    // Delete the persisted entries
    List<Delete> deletes = Lists.newArrayList();
    for (byte[] rowKey : rollbackKeys) {
      Delete delete = new Delete(rowKey);
      deletes.add(delete);
    }
    hTable.delete(deletes);
    hTable.flushCommits();
  }
}
