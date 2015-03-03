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

import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.queue.QueueEntry;
import co.cask.cdap.data2.transaction.queue.AbstractQueueProducer;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.queue.QueueMetrics;
import co.cask.tephra.Transaction;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * A {@link co.cask.cdap.data2.queue.QueueProducer} that uses HBase as the storage for queue entries and consumers
 * states. The row key has the following format:
 *
 * <pre>
 * {@code
 *
 * row_key = <salt_prefix> <row_key_base>
 * salt_prefix = 1 byte hash of <row_key_base>
 * row_key_base = <queue_prefix> <write_point> <counter>
 * queue_prefix = <name_hash> <queue_name>
 * name_hash = First byte of MD5 of <queue_name>
 * queue_name = flowlet_name + "/" + output_name
 * write_pointer = 8 bytes long value of the write pointer of the transaction
 * counter = 4 bytes int value of a monotonic increasing number assigned for each entry written in the same transaction
 * }
 * </pre>
 */
public class HBaseQueueProducer extends AbstractQueueProducer implements Closeable {

  private final HBaseQueueStrategy queueStrategy;
  private final byte[] queueRowPrefix;
  private final HTable hTable;
  private final List<byte[]> rollbackKeys;

  public HBaseQueueProducer(HTable hTable, QueueName queueName,
                            QueueMetrics queueMetrics, HBaseQueueStrategy queueStrategy) {
    super(queueMetrics, queueName);
    this.queueStrategy = queueStrategy;
    // Base row key = queue_name + writePointer + counter
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
    int count = 0;
    List<Put> puts = Lists.newArrayList();
    int bytes = 0;

    List<byte[]> rowKeys = Lists.newArrayList();
    long writePointer = transaction.getWritePointer();
    for (QueueEntry entry : entries) {
      rowKeys.clear();
      queueStrategy.getRowKeys(entry, queueRowPrefix, writePointer, count, rowKeys);
      rollbackKeys.addAll(rowKeys);

      byte[] metaData = QueueEntry.serializeHashKeys(entry.getHashKeys());
      for (byte[] rowKey : rowKeys) {
        // No need to write ts=writePointer, as the row key already contains the writePointer
        Put put = new Put(rowKey);
        put.add(QueueEntryRow.COLUMN_FAMILY, QueueEntryRow.DATA_COLUMN, entry.getData());
        put.add(QueueEntryRow.COLUMN_FAMILY, QueueEntryRow.META_COLUMN, metaData);

        puts.add(put);

        bytes += entry.getData().length;
      }
      count++;
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
