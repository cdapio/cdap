/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.transaction.queue.AbstractQueue2Consumer;
import com.continuuity.data2.transaction.queue.ConsumerEntryState;
import com.continuuity.data2.transaction.queue.QueueConstants;
import com.continuuity.data2.transaction.queue.QueueScanner;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.BitComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Queue consumer for HBase.
 */
final class HBaseQueue2Consumer extends AbstractQueue2Consumer {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseQueue2Consumer.class);

  // Persist latest start row every n entries consumed.
  private static final int PERSIST_START_ROW_LIMIT = 10000;

  private final HTable hTable;
  private final Filter processedStateFilter;
  private final HBaseConsumerStateStore stateStore;
  private boolean closed;

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
    this.processedStateFilter = createStateFilter();
    this.stateStore = stateStore;
    byte[] startRow = consumerState.getStartRow();

    if (startRow != null && startRow.length > 0) {
      this.startRow = startRow;
    }
  }

  @Override
  protected boolean claimEntry(byte[] rowKey, byte[] claimedStateValue) throws IOException {
    Put put = new Put(rowKey);
    put.add(QueueConstants.COLUMN_FAMILY, stateColumnName, claimedStateValue);
    return hTable.checkAndPut(rowKey, QueueConstants.COLUMN_FAMILY,
                              stateColumnName, null, put);
  }

  @Override
  protected void updateState(Set<byte[]> rowKeys, byte[] stateColumnName, byte[] stateContent) throws IOException {
    if (rowKeys.isEmpty()) {
      return;
    }
    List<Put> puts = Lists.newArrayListWithCapacity(rowKeys.size());
    for (byte[] rowKey : rowKeys) {
      Put put = new Put(rowKey);
      put.add(QueueConstants.COLUMN_FAMILY, stateColumnName, stateContent);
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
      Delete delete = new Delete(rowKey);
      delete.deleteColumn(QueueConstants.COLUMN_FAMILY, stateColumnName);
      ops.add(delete);
    }
    hTable.batch(ops);
    hTable.flushCommits();
  }

  @Override
  protected QueueScanner getScanner(byte[] startRow, byte[] stopRow, int numRows) throws IOException {
    // Scan the table for queue entries.
    Scan scan = new Scan();
    scan.setCaching(numRows);
    scan.setStartRow(startRow);
    scan.setStopRow(stopRow);
    scan.addColumn(QueueConstants.COLUMN_FAMILY, QueueConstants.DATA_COLUMN);
    scan.addColumn(QueueConstants.COLUMN_FAMILY, QueueConstants.META_COLUMN);
    scan.addColumn(QueueConstants.COLUMN_FAMILY, stateColumnName);
    scan.setFilter(createFilter());

    ResultScanner scanner = hTable.getScanner(scan);
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

  /**
   * Creates a HBase filter that will filter out rows that that has committed state = PROCESSED.
   */
  private Filter createFilter() {
    return new FilterList(FilterList.Operator.MUST_PASS_ONE, processedStateFilter, new SingleColumnValueFilter(
      QueueConstants.COLUMN_FAMILY, stateColumnName, CompareFilter.CompareOp.GREATER,
      new BinaryPrefixComparator(Bytes.toBytes(transaction.getReadPointer()))
    ));
  }

  /**
   * Creates a HBase filter that will filter out rows with state column state = PROCESSED (ignoring transaction).
   */
  private Filter createStateFilter() {
    byte[] processedMask = new byte[Ints.BYTES * 2 + 1];
    processedMask[processedMask.length - 1] = ConsumerEntryState.PROCESSED.getState();
    return new SingleColumnValueFilter(QueueConstants.COLUMN_FAMILY, stateColumnName,
                                       CompareFilter.CompareOp.NOT_EQUAL,
                                       new BitComparator(processedMask, BitComparator.BitwiseOp.AND));
  }

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
          Map<byte[], byte[]> row = result.getFamilyMap(QueueConstants.COLUMN_FAMILY);
          return ImmutablePair.of(result.getRow(), row);
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
