/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueResult;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.hadoop.hbase.KeyValue;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

/**
 *
 */
final class HBaseQueue2Consumer implements Queue2Consumer, TransactionAware {

  // TODO: Make these configurable.
  private static final int MAX_CACHE_ROWS = 100;

  private static final DequeueResult EMPTY_RESULT = new DequeueResult() {
    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public Collection<byte[]> getData() {
      return ImmutableList.of();
    }
  };

  private final ConsumerConfig consumerConfig;
  private final HTable hTable;
  private final QueueName queueName;
  private final SortedMap<byte[], Entry> entryCache;
  private final SortedMap<byte[], Entry> consumingEntries;
  private final Function<byte[], byte[]> rowKeyToChangeTx;
  private final byte[] stateColumnName;
  private byte[] startRow;
  private Transaction transaction;

  HBaseQueue2Consumer(ConsumerConfig consumerConfig, HTable hTable, QueueName queueName) {
    this.consumerConfig = consumerConfig;
    this.hTable = hTable;
    this.queueName = queueName;
    this.entryCache = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    this.consumingEntries = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    this.startRow = queueName.toBytes();
    this.stateColumnName = Bytes.add(HBaseQueueConstants.STATE_COLUMN_PREFIX,
                                     Bytes.toBytes(consumerConfig.getGroupId()));

    byte[] tableName = hTable.getTableName();
    final byte[] changeTxPrefix = ByteBuffer.allocate(tableName.length + 1)
                                      .put((byte) tableName.length)
                                      .put(tableName)
                                      .array();

    rowKeyToChangeTx = new Function<byte[], byte[]>() {
      @Override
      public byte[] apply(byte[] rowKey) {
        return Bytes.add(changeTxPrefix, rowKey);
      }
    };
  }

  @Override
  public DequeueResult dequeue() throws IOException {
    return dequeue(1);
  }

  @Override
  public DequeueResult dequeue(int maxBatchSize) throws IOException {
    Preconditions.checkArgument(maxBatchSize > 0, "Batch size must be > 0.");

    while (consumingEntries.size() < maxBatchSize && getEntries(consumingEntries, maxBatchSize)) {

      // For FIFO, need to try claiming the entry if group size > 1
      if (consumerConfig.getDequeueStrategy() == DequeueStrategy.FIFO && consumerConfig.getGroupSize() > 1) {
        Iterator<Map.Entry<byte[], Entry>> iterator = consumingEntries.entrySet().iterator();
        while (iterator.hasNext()) {
          Entry entry = iterator.next().getValue();

          // If the state is already in CLAIMED state, no need to claim it again
          // It happens for rollbacked entries or restart from failure
          // The pickup logic in populateCache and shouldInclude() make sure that's the case
          if (entry.getState() == null) {
            Put put = new Put(entry.getRowKey());
            byte[] stateValue = encodeStateColumn(ConsumerEntryState.CLAIMED);
            put.add(HBaseQueueConstants.COLUMN_FAMILY, stateColumnName, stateValue);
            boolean claimed = hTable.checkAndPut(entry.getRowKey(), HBaseQueueConstants.COLUMN_FAMILY,
                                                 stateColumnName, null, put);
            // If not able to claim it, remove it, and move to next one.
            if (!claimed) {
              iterator.remove();
              continue;
            }
          }
        }
      }
    }

    // If nothing get dequeued, return the empty result.
    if (consumingEntries.isEmpty()) {
      return EMPTY_RESULT;
    }

    return new DequeueResultImpl(consumingEntries.values());
  }

  @Override
  public void startTx(Transaction tx) {
    consumingEntries.clear();
    this.transaction = tx;
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    return ImmutableSet.copyOf(Iterators.transform(consumingEntries.keySet().iterator(), rowKeyToChangeTx));
  }

  @Override
  public boolean commitTx() throws Exception {
    if (consumingEntries.isEmpty()) {
      return true;
    }

    byte[] stateContent = encodeStateColumn(ConsumerEntryState.PROCESSED);

    List<Put> puts = Lists.newArrayListWithCapacity(consumingEntries.size());
    for (byte[] rowKey : consumingEntries.keySet()) {
      Put put = new Put(rowKey);
      put.add(HBaseQueueConstants.COLUMN_FAMILY, stateColumnName, stateContent);
      puts.add(put);
    }

    hTable.put(puts);
    hTable.flushCommits();
    return true;
  }

  @Override
  public boolean rollbackTx() throws Exception {
    if (consumingEntries.isEmpty()) {
      return true;
    }

    // Put the consuming entries back to cache
    entryCache.putAll(consumingEntries);

    // Revert changes in HBase rows
    List<Row> ops = Lists.newArrayListWithCapacity(consumingEntries.size());

    // If it is FIFO, restore to the CLAIMED state. This instance will retry it on the next dequeue.
    if (consumerConfig.getDequeueStrategy() == DequeueStrategy.FIFO && consumerConfig.getGroupSize() > 1) {
      byte[] stateContent = encodeStateColumn(ConsumerEntryState.CLAIMED);
      for (byte[] rowKey : consumingEntries.keySet()) {
        Put put = new Put(rowKey);
        put.add(HBaseQueueConstants.COLUMN_FAMILY, stateColumnName, stateContent);
        ops.add(put);
      }
    } else {
      for (byte[] rowKey : consumingEntries.keySet()) {
        Delete delete = new Delete(rowKey);
        delete.deleteColumn(HBaseQueueConstants.COLUMN_FAMILY, stateColumnName);
        ops.add(delete);
      }
    }

    hTable.batch(ops);
    hTable.flushCommits();
    return true;
  }

  /**
   * Try to dequeue (claim) entries up to a maximum size.
   * @param entries For claimed entries to fill in.
   * @param maxBatchSize Maximum number of entries to claim.
   * @return The entries instance.
   * @throws IOException
   */
  private boolean getEntries(SortedMap<byte[], Entry> entries, int maxBatchSize) throws IOException {
    boolean hasEntry = fetchFromCache(entries, maxBatchSize);

    // If not enough entries from the cache, try to get more.
    if (entries.size() < maxBatchSize) {
      populateRowCache(entries.keySet());
      hasEntry = fetchFromCache(entries, maxBatchSize) || hasEntry;
    }

    return hasEntry;
  }

  private boolean fetchFromCache(SortedMap<byte[], Entry> entries, int maxBatchSize) {
    if (entryCache.isEmpty()) {
      return false;
    }

    Iterator<Map.Entry<byte[], Entry>> iterator = entryCache.entrySet().iterator();
    while (entries.size() < maxBatchSize && iterator.hasNext()) {
      Map.Entry<byte[], Entry> entry = iterator.next();
      entries.put(entry.getKey(), entry.getValue());
      iterator.remove();
    }
    return true;
  }

  private void populateRowCache(Set<byte[]> excludeRows) throws IOException {
    // Scan the table for queue entries.
    Scan scan = new Scan();
    scan.setCaching(MAX_CACHE_ROWS);
    scan.setStartRow(startRow);
    scan.setStopRow(getStopRow());
    scan.addColumn(HBaseQueueConstants.COLUMN_FAMILY, HBaseQueueConstants.DATA_COLUMN);
    scan.addColumn(HBaseQueueConstants.COLUMN_FAMILY, HBaseQueueConstants.META_COLUMN);
    scan.addColumn(HBaseQueueConstants.COLUMN_FAMILY, stateColumnName);

    // TODO: Need more test before enabling it.
//    scan.setFilter(createFilter());

    long readPointer = transaction.getReadPointer();
    long[] excludedList = transaction.getExcludedList();

    ResultScanner scanner = hTable.getScanner(scan);
    // Try fill up the cache with at most MAX_CACHE_ROWS
    while (entryCache.size() < MAX_CACHE_ROWS) {
      Result[] results = scanner.next(MAX_CACHE_ROWS);
      if (results.length == 0) {
        // No more result, breaking out.
        break;
      }
      for (Result result : results) {
        byte[] rowKey = result.getRow();

        if (excludeRows.contains(rowKey)) {
          continue;
        }

        // Row key is queue_name + writePointer + counter
        long writePointer = Bytes.toLong(rowKey, queueName.toBytes().length, Longs.BYTES);

        // If writes later than the reader pointer, abort the loop, as entries that comes later are all uncommitted.
        if (writePointer > readPointer) {
          break;
        }

        // If the write is in the excluded list, ignore it.
        if (Arrays.binarySearch(excludedList, writePointer) >= 0) {
          continue;
        }

        // Based on the strategy to determine if include the given entry or not.
        KeyValue metaColumn = result.getColumnLatest(HBaseQueueConstants.COLUMN_FAMILY,
                                                     HBaseQueueConstants.META_COLUMN);
        KeyValue stateColumn = result.getColumnLatest(HBaseQueueConstants.COLUMN_FAMILY,
                                                      stateColumnName);

        int counter = Bytes.toInt(rowKey, rowKey.length - 4, Ints.BYTES);
        if (!shouldInclude(writePointer, counter, metaColumn, stateColumn)) {
          continue;
        }

        entryCache.put(rowKey, new Entry(rowKey,
                                         result.getValue(HBaseQueueConstants.COLUMN_FAMILY,
                                                         HBaseQueueConstants.DATA_COLUMN),
                                         result.getValue(HBaseQueueConstants.COLUMN_FAMILY,
                                                         stateColumnName)));
      }
    }
    scanner.close();
  }

  private Filter createFilter() {
    byte[] processedMask = new byte[Ints.BYTES * 2 + 1];
    processedMask[processedMask.length - 1] = ConsumerEntryState.PROCESSED.getState();
    Filter stateFilter = new SingleColumnValueFilter(HBaseQueueConstants.COLUMN_FAMILY, stateColumnName,
                                                     CompareFilter.CompareOp.NOT_EQUAL,
                                                     new BitComparator(processedMask, BitComparator.BitwiseOp.AND));

    return new FilterList(FilterList.Operator.MUST_PASS_ONE, stateFilter, new SingleColumnValueFilter(
      HBaseQueueConstants.COLUMN_FAMILY, stateColumnName, CompareFilter.CompareOp.GREATER,
      new BinaryPrefixComparator(Bytes.toBytes(transaction.getReadPointer()))
    ));
  }

  private byte[] encodeStateColumn(ConsumerEntryState state) {
    // State column content is encoded as (writePointer) + (instanceId) + (state)
    byte[] stateContent = new byte[Longs.BYTES + Ints.BYTES + 1];
    Bytes.putLong(stateContent, 0, transaction.getWritePointer());
    Bytes.putInt(stateContent, Longs.BYTES, consumerConfig.getInstanceId());
    Bytes.putByte(stateContent, Longs.BYTES + Ints.BYTES, state.getState());
    return stateContent;
  }

  private long getStateWritePointer(KeyValue stateColumn) {
    return Bytes.toLong(stateColumn.getBuffer(), stateColumn.getValueOffset(), Longs.BYTES);
  }

  private int getStateInstanceId(KeyValue stateColumn) {
    return Bytes.toInt(stateColumn.getBuffer(), stateColumn.getValueOffset() + Longs.BYTES, Ints.BYTES);
  }

  private ConsumerEntryState getState(KeyValue stateColumn) {
    return ConsumerEntryState.fromState(
      stateColumn.getBuffer()[stateColumn.getValueOffset() + Longs.BYTES + Ints.BYTES]);
  }

  private boolean shouldInclude(long enqueueWritePointer, int counter,
                                KeyValue metaColumn, KeyValue stateColumn) throws IOException {
    if (stateColumn != null) {
      // If the state is written by the current transaction, ignore it, as it's processing
      long stateWritePointer = getStateWritePointer(stateColumn);
      if (stateWritePointer == transaction.getWritePointer()) {
        return false;
      }

      // If the state was updated by a different consumer instance that is still active, ignore this entry.
      // The assumption is, the corresponding instance is either processing (claimed)
      // or going to process it (due to rollback/restart).
      // This only applies to FIFO, as for hash and rr, repartition needs to happen if group size change.
      int stateInstanceId = getStateInstanceId(stateColumn);
      if (consumerConfig.getDequeueStrategy() == DequeueStrategy.FIFO
          && stateInstanceId < consumerConfig.getGroupSize()
          && stateInstanceId != consumerConfig.getInstanceId()) {
        return false;
      }

      // If state is PROCESSED and committed, ignore it
      long[] excludedList = transaction.getExcludedList();
      ConsumerEntryState state = getState(stateColumn);
      if (state == ConsumerEntryState.PROCESSED
          && stateWritePointer <= transaction.getReadPointer()
          && Arrays.binarySearch(excludedList, stateWritePointer) < 0) {

        // If the PROCESSED entry write pointer is smaller than smallest in excluded list, then it must be processed.
        if (excludedList.length == 0 || excludedList[0] > enqueueWritePointer) {
          startRow = getNextRow(enqueueWritePointer, counter);
        }
        return false;
      }
    }

    switch (consumerConfig.getDequeueStrategy()) {
      case FIFO:
        // Always try to process (claim) if using FIFO. The resolution will be done by atomically setting state
        // to CLAIMED
        return true;
      case ROUND_ROBIN: {
        int hashValue = Objects.hashCode(enqueueWritePointer, counter);
        return consumerConfig.getInstanceId() == (hashValue % consumerConfig.getGroupSize());
      }
      case HASH: {
        Map<String, Integer> hashKeys = QueueEntry.deserializeHashKeys(metaColumn.getBuffer(),
                                                                       metaColumn.getValueOffset(),
                                                                       metaColumn.getValueLength());
        Integer hashValue = hashKeys.get(consumerConfig.getHashKey());
        if (hashValue == null) {
          // If no such hash key, default it to instance 0.
          return consumerConfig.getInstanceId() == 0;
        }
        // Assign to instance based on modulus on the hashValue.
        return consumerConfig.getInstanceId() == (hashValue % consumerConfig.getGroupSize());
      }
      default:
        throw new UnsupportedOperationException("Strategy " + consumerConfig.getDequeueStrategy() + " not supported.");
    }
  }

  /**
   * Gets the stop row for scan. Stop row is queueName + (readPointer + 1).
   */
  private byte[] getStopRow() {
    return Bytes.add(queueName.toBytes(), Bytes.toBytes(transaction.getReadPointer() + 1L));
  }

  private byte[] getNextRow(long writePointer, int count) {
    return Bytes.add(queueName.toBytes(), Bytes.toBytes(writePointer), Bytes.toBytes(count + 1));
  }

  private static final class Entry {
    private final byte[] rowKey;
    private final byte[] data;
    private final byte[] state;

    private Entry(byte[] rowKey, byte[] data, byte[] state) {
      this.rowKey = rowKey;
      this.data = data;
      this.state = state;
    }

    private byte[] getRowKey() {
      return rowKey;
    }

    private byte[] getData() {
      return data;
    }

    private byte[] getState() {
      return state;
    }
  }

  private static final class DequeueResultImpl implements DequeueResult {

    private final List<byte[]> data;

    DequeueResultImpl(Collection<Entry> entries) {
      ImmutableList.Builder<byte[]> builder = ImmutableList.builder();
      for (Entry entry : entries) {
        builder.add(entry.getData());
      }
      this.data = builder.build();
    }

    @Override
    public boolean isEmpty() {
      return data.isEmpty();
    }

    @Override
    public Collection<byte[]> getData() {
      return data;
    }
  }
}
