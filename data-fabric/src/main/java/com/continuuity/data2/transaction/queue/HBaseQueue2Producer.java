/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 */
public final class HBaseQueue2Producer implements Queue2Producer, TransactionAware {

  private final Queue<QueueEntry> queue;
  private final QueueName queueName;
  private final HTable hTable;
  private final List<byte[]> rollbackKeys;
  private Transaction transaction;


  public HBaseQueue2Producer(HTable hTable, QueueName queueName) {
    this.queue = new ConcurrentLinkedQueue<QueueEntry>();
    this.queueName = queueName;
    this.rollbackKeys = Lists.newArrayList();
    this.hTable = hTable;
  }

  @Override
  public void enqueue(QueueEntry entry) throws IOException {
    if (transaction != null) {
      // Simply buffer it in memory.
      queue.add(entry);
    } else {
      persist(ImmutableList.of(entry));
    }
  }

  @Override
  public void enqueue(Iterable <QueueEntry> entries) throws IOException {
    if (transaction != null) {
      Iterables.addAll(queue, entries);
    } else {
      persist(entries);
    }
  }

  @Override
  public void startTx(Transaction tx) {
    queue.clear();
    rollbackKeys.clear();
    transaction = tx;
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    // Always empty changes, as enqueue is append only, nothing could be conflict.
    return ImmutableList.of();
  }

  @Override
  public boolean commitTx() throws Exception {
    persist(queue);
    return true;
  }

  @Override
  public void postTxCommit() {
    // nothing to do
  }


  @Override
  public boolean rollbackTx() throws Exception {
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

  /**
   * Persist queue entries into HBase.
   */
  private void persist(Iterable<QueueEntry> entries) throws IOException {
    // TODO: What key should it be if transaction is null?
    long writePointer = transaction.getWritePointer();
    byte[] rowKeyPrefix = Bytes.add(queueName.toBytes(), Bytes.toBytes(writePointer));
    int count = 0;
    List<Put> puts = Lists.newArrayList();

    for (QueueEntry entry : entries) {
      // Row key = queue_name + writePointer + counter
      byte[] rowKey = Bytes.add(rowKeyPrefix, Bytes.toBytes(count++));
      if (transaction != null) {
        rollbackKeys.add(rowKey);
      }
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
