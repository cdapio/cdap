/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.common.queue.QueueName;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.transaction.queue.AbstractQueue2Consumer;
import com.continuuity.data2.transaction.queue.QueueEntryRow;
import com.continuuity.data2.transaction.queue.QueueScanner;
import com.continuuity.hbase.wd.DistributedScanner;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Queue consumer for HBase.
 */
abstract class HBaseQueue2Consumer extends AbstractQueue2Consumer {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseQueue2Consumer.class);

  // Persist latest start row every n entries consumed.
  // The smaller this number, the more frequent latest startRow is persisted, which makes more
  // entries could be evicted since startRow is used by the eviction logic to determine what can be evicted.
  // The down side of decreasing this value is more overhead on each postCommit call for writing to HBase.
  private static final int PERSIST_START_ROW_LIMIT = 1000;

  private final HTable hTable;
  private final HBaseConsumerStateStore stateStore;
  private boolean closed;

  // Executes distributed scans
  private final ExecutorService scansExecutor;

  /**
   * Creates a HBaseQueue2Consumer.
   * @param consumerConfig Configuration of the consumer.
   * @param hTable The HTable instance to use for communicating with HBase. This consumer is responsible for closing it.
   * @param queueName Name of the queue.
   * @param consumerState The persisted state of this consumer.
   * @param stateStore The store for persisting state for this consumer.
   */
  HBaseQueue2Consumer(ConsumerConfig consumerConfig, HTable hTable, QueueName queueName,
                      HBaseConsumerState consumerState, HBaseConsumerStateStore stateStore) {
    // For HBase, eviction is done at table flush time, hence no QueueEvictor is needed.
    super(consumerConfig, queueName);
    this.hTable = hTable;

    // Using the "direct handoff" approach, new threads will only be created
    // if it is necessary and will grow unbounded. This could be bad but in DistributedScanner
    // we only create as many Runnables as there are buckets data is distributed to. It means
    // it also scales when buckets amount changes.
    this.scansExecutor = new ThreadPoolExecutor(1, 20,
                                       60, TimeUnit.SECONDS,
                                       new SynchronousQueue<Runnable>(),
                                       Threads.newDaemonThreadFactory("queue-consumer-scan"));
    ((ThreadPoolExecutor) this.scansExecutor).allowCoreThreadTimeOut(true);

    this.stateStore = stateStore;
    byte[] startRow = consumerState.getStartRow();

    if (startRow != null && startRow.length > 0) {
      this.startRow = startRow;
    }
  }

  @Override
  protected boolean claimEntry(byte[] rowKey, byte[] claimedStateValue) throws IOException {
    rowKey = HBaseQueueAdmin.ROW_KEY_DISTRIBUTOR.getDistributedKey(rowKey);
    Put put = new Put(rowKey);
    put.add(QueueEntryRow.COLUMN_FAMILY, stateColumnName, claimedStateValue);
    return hTable.checkAndPut(rowKey, QueueEntryRow.COLUMN_FAMILY,
                              stateColumnName, null, put);
  }

  @Override
  protected void updateState(Set<byte[]> rowKeys, byte[] stateColumnName, byte[] stateContent) throws IOException {
    if (rowKeys.isEmpty()) {
      return;
    }
    List<Put> puts = Lists.newArrayListWithCapacity(rowKeys.size());
    for (byte[] rowKey : rowKeys) {
      rowKey = HBaseQueueAdmin.ROW_KEY_DISTRIBUTOR.getDistributedKey(rowKey);
      Put put = new Put(rowKey);
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
      rowKey = HBaseQueueAdmin.ROW_KEY_DISTRIBUTOR.getDistributedKey(rowKey);
      Delete delete = new Delete(rowKey);
      delete.deleteColumn(QueueEntryRow.COLUMN_FAMILY, stateColumnName);
      ops.add(delete);
    }
    hTable.batch(ops);
    hTable.flushCommits();
  }

  @Override
  protected QueueScanner getScanner(byte[] startRow, byte[] stopRow, int numRows) throws IOException {
    // Scan the table for queue entries.
    Scan scan = createScan(startRow, stopRow, numRows);

    DequeueScanAttributes.setQueueRowPrefix(scan, getQueueName());
    DequeueScanAttributes.set(scan, getConfig());
    DequeueScanAttributes.set(scan, transaction);

    ResultScanner scanner = DistributedScanner.create(hTable, scan, HBaseQueueAdmin.ROW_KEY_DISTRIBUTOR, scansExecutor);
    return new HBaseQueueScanner(scanner, numRows);
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    try {
      stateStore.saveState(new HBaseConsumerState(startRow, getConfig().getGroupId(), getConfig().getInstanceId()));
    } finally {
      scansExecutor.shutdownNow();
      hTable.close();
      closed = true;
    }
  }

  @Override
  public void postTxCommit() {
    super.postTxCommit();
    if (commitCount >= PERSIST_START_ROW_LIMIT) {
      try {
        stateStore.saveState(new HBaseConsumerState(startRow, getConfig().getGroupId(), getConfig().getInstanceId()));
        commitCount = 0;
      } catch (IOException e) {
        LOG.error("Failed to persist start row to HBase.", e);
      }
    }
  }

  protected abstract Scan createScan(byte[] startRow, byte[] stopRow, int numRows);

  private class HBaseQueueScanner implements QueueScanner {
    private final ResultScanner scanner;
    private final LinkedList<Result> cached = Lists.newLinkedList();
    private final int numRows;

    public HBaseQueueScanner(ResultScanner scanner, int numRows) {
      this.scanner = scanner;
      this.numRows = numRows;
    }

    @Override
    public ImmutablePair<byte[], Map<byte[], byte[]>> next() throws IOException {
      while (true) {
        if (cached.size() > 0) {
          Result result = cached.removeFirst();
          Map<byte[], byte[]> row = result.getFamilyMap(QueueEntryRow.COLUMN_FAMILY);
          return ImmutablePair.of(HBaseQueueAdmin.ROW_KEY_DISTRIBUTOR.getOriginalKey(result.getRow()), row);
        }
        Result[] results = scanner.next(numRows);
        if (results.length == 0) {
          return null;
        }
        Collections.addAll(cached, results);
      }
    }

    @Override
    public void close() throws IOException {
      scanner.close();
    }
  }
}
