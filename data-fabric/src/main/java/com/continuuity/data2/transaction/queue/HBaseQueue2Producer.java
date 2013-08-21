/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data2.transaction.Transaction;
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
public final class HBaseQueue2Producer extends AbstractQueue2Producer implements Closeable {

  private final byte[] queueRowPrefix;
  private final HTable hTable;
  private final List<byte[]> rollbackKeys;

  public HBaseQueue2Producer(HTable hTable, QueueName queueName, QueueMetrics queueMetrics) {
    super(queueMetrics);
    this.queueRowPrefix = HBaseQueueUtils.getQueueRowPrefix(queueName);
    this.rollbackKeys = Lists.newArrayList();
    this.hTable = hTable;
  }

  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    rollbackKeys.clear();
  }

  @Override
  public boolean rollbackTx() throws Exception {
    // If nothing to rollback, simply return
    if (rollbackKeys.isEmpty()) {
      return true;
    }

    // Delete the persisted entries
    List<Delete> deletes = Lists.newArrayList();
    for (byte[] rowKey : rollbackKeys) {
      Delete delete = new Delete(rowKey);
      deletes.add(delete);
    }
    hTable.delete(deletes);
    hTable.flushCommits();

    return true;
  }

  @Override
  public void close() throws IOException {
    hTable.close();
  }

  /**
   * Persist queue entries into HBase.
   */
  protected void persist(Iterable<QueueEntry> entries, Transaction transaction) throws IOException {
    long writePointer = transaction.getWritePointer();
    byte[] rowKeyPrefix = Bytes.add(queueRowPrefix, Bytes.toBytes(writePointer));
    int count = 0;
    List<Put> puts = Lists.newArrayList();

    for (QueueEntry entry : entries) {
      // Row key = queue_name + writePointer + counter
      byte[] rowKey = Bytes.add(rowKeyPrefix, Bytes.toBytes(count++));
      rollbackKeys.add(rowKey);
      // No need to write ts=writePointer, as the row key already contains the writePointer
      Put put = new Put(rowKey);
      put.add(HBaseQueueConstants.COLUMN_FAMILY,
              HBaseQueueConstants.DATA_COLUMN,
              entry.getData());
      put.add(HBaseQueueConstants.COLUMN_FAMILY,
              HBaseQueueConstants.META_COLUMN,
              QueueEntry.serializeHashKeys(entry.getHashKeys()));

      puts.add(put);
    }
    hTable.put(puts);
    hTable.flushCommits();
  }
}
