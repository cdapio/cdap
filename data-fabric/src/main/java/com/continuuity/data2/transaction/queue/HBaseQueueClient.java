/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.QueueClient;
import com.continuuity.data2.queue.QueueConsumer;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A {@link TransactionAware} implementation of {@link QueueClient} using HBase as the backing store.
 */
public final class HBaseQueueClient implements QueueClient, TransactionAware {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseQueueClient.class);

  // 4M write buffer for HTable
  private static final int DEFAULT_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;

  private final Queue<QueueEntry> queue;
  private final QueueName queueName;
  private final HTable hTable;
  private final List<byte[]> rollbackKeys;
  private Transaction transaction;


  public HBaseQueueClient(Configuration hConf, String tableName, QueueName queueName) throws IOException {
    this(new HBaseAdmin(hConf), tableName, queueName);
  }


  public HBaseQueueClient(HBaseAdmin admin, String tableName, QueueName queueName) throws IOException {
    this.queue = new ConcurrentLinkedQueue<QueueEntry>();
    this.queueName = queueName;
    this.rollbackKeys = Lists.newArrayList();

    HBaseUtils.createTableIfNotExists(admin, tableName,
                                      HBaseQueueConstants.COLUMN_FAMILY, HBaseQueueConstants.MAX_CREATE_TABLE_WAIT);

    HTable hTable = new HTable(admin.getConfiguration(), tableName);
    // TODO: make configurable
    hTable.setWriteBufferSize(DEFAULT_WRITE_BUFFER_SIZE);
    hTable.setAutoFlush(false);
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
  public QueueConsumer createConsumer(ConsumerConfig consumerConfig) throws IOException {
    HTable consumerTable = new HTable(hTable.getConfiguration(), hTable.getTableName());
    consumerTable.setWriteBufferSize(DEFAULT_WRITE_BUFFER_SIZE);
    consumerTable.setAutoFlush(false);

    return new HBaseQueueConsumer(consumerConfig, consumerTable, queueName);
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
    // TODO: What key should it be if transaction is null.
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
