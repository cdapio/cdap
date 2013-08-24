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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Uninterruptibles;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
final class HBaseQueue2Consumer implements Queue2Consumer, TransactionAware, Closeable {

  // TODO: Make these configurable.
  private static final int MAX_CACHE_ROWS = 100;
  private static final long EVICTION_TIMEOUT_SECONDS = 10;
  // How many commits to trigger eviction.
  private static final int EVICTION_LIMIT = 1000;

  private static final Logger LOG = LoggerFactory.getLogger(HBaseQueue2Consumer.class);

  private static final Function<HBaseQueueEntry, byte[]> ENTRY_TO_BYTE_ARRAY = new Function<HBaseQueueEntry, byte[]>() {
    @Override
    public byte[] apply(HBaseQueueEntry input) {
      return input.getData();
    }
  };

  private final ConsumerConfig consumerConfig;
  private final HTable hTable;
  private final QueueName queueName;
  private final SortedMap<byte[], HBaseQueueEntry> entryCache;
  private final SortedMap<byte[], HBaseQueueEntry> consumingEntries;
  private final byte[] stateColumnName;
  private final byte[] queueRowPrefix;
  private final Filter processedStateFilter;
  private final QueueEvictor queueEvictor;
  private byte[] startRow;
  private Transaction transaction;
  private boolean committed;
  private int commitCount;

  HBaseQueue2Consumer(ConsumerConfig consumerConfig, HTable hTable, QueueName queueName, QueueEvictor queueEvictor) {
    this.consumerConfig = consumerConfig;
    this.hTable = hTable;
    this.queueName = queueName;
    this.entryCache = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    this.consumingEntries = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    this.queueRowPrefix = QueueUtils.getQueueRowPrefix(queueName);
    this.startRow = queueRowPrefix;
    this.stateColumnName = Bytes.add(QueueConstants.STATE_COLUMN_PREFIX,
                                     Bytes.toBytes(consumerConfig.getGroupId()));
    this.processedStateFilter = createStateFilter();
    this.queueEvictor = queueEvictor;
  }

  @Override
  public QueueName getQueueName() {
    return queueName;
  }

  @Override
  public ConsumerConfig getConfig() {
    return consumerConfig;
  }

  @Override
  public DequeueResult dequeue() throws IOException {
    return dequeue(1);
  }

  @Override
  public DequeueResult dequeue(int maxBatchSize) throws IOException {
    Preconditions.checkArgument(maxBatchSize > 0, "Batch size must be > 0.");

    while (consumingEntries.size() < maxBatchSize && getEntries(consumingEntries, maxBatchSize)) {

      // ANDREAS: this while loop should stop once getEntries/populateCache reaches the end of the queue. Currently, it
      // will retry as long as it gets at least one entry in every round, even if that is an entry that must be ignored
      // because it cannot be claimed.
      // ANDREAS: It could be a problem that we always read to the end of the queue. This way one flowlet instance may
      // always all entries, while others are idle.

      // For FIFO, need to try claiming the entry if group size > 1
      if (consumerConfig.getDequeueStrategy() == DequeueStrategy.FIFO && consumerConfig.getGroupSize() > 1) {
        Iterator<Map.Entry<byte[], HBaseQueueEntry>> iterator = consumingEntries.entrySet().iterator();
        while (iterator.hasNext()) {
          HBaseQueueEntry entry = iterator.next().getValue();

          // If the state is already in CLAIMED state, no need to claim it again
          // It happens for rollbacked entries or restart from failure
          // The pickup logic in populateCache and shouldInclude() make sure that's the case
          // ANDREAS: but how do we know that it was claimed by THIS consumer. If there are multiple consumers,
          // then they all will pick it up, right?
          if (entry.getState() == null) {
            Put put = new Put(entry.getRowKey());
            byte[] stateValue = encodeStateColumn(ConsumerEntryState.CLAIMED);
            put.add(QueueConstants.COLUMN_FAMILY, stateColumnName, stateValue);
            boolean claimed = hTable.checkAndPut(entry.getRowKey(), QueueConstants.COLUMN_FAMILY,
                                                 stateColumnName, null, put);
            // If not able to claim it, remove it, and move to next one.
            if (!claimed) {
              iterator.remove();
            }
          }
        }
      }
    }

    // If nothing get dequeued, return the empty result.
    if (consumingEntries.isEmpty()) {
      return DequeueResult.EMPTY_RESULT;
    }

    return new HBaseDequeueResult(consumingEntries.values());
  }

  @Override
  public void startTx(Transaction tx) {
    consumingEntries.clear();
    this.transaction = tx;
    this.committed = false;
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    // No conflicts guaranteed in dequeue logic.
    return ImmutableList.of();
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
      put.add(QueueConstants.COLUMN_FAMILY, stateColumnName, stateContent);
      puts.add(put);
    }

    hTable.put(puts);
    hTable.flushCommits();
    committed = true;
    return true;
  }

  @Override
  public void postTxCommit() {
    commitCount++;
    if (commitCount >= EVICTION_LIMIT) {
      commitCount = 0;
      // Fire and forget
      queueEvictor.evict(transaction);
    }
  }

  @Override
  public boolean rollbackTx() throws Exception {
    if (consumingEntries.isEmpty()) {
      return true;
    }

    // Put the consuming entries back to cache
    entryCache.putAll(consumingEntries);

    // If not committed, no need to update HBase.
    if (!committed) {
      return true;
    }

    // Revert changes in HBase rows
    List<Row> ops = Lists.newArrayListWithCapacity(consumingEntries.size());

    // If it is FIFO, restore to the CLAIMED state. This instance will retry it on the next dequeue.
    // ANDREAS: this is only needed if commitTx() was called to ack the entries.
    // TERENCE: Agree. Fixed
    if (consumerConfig.getDequeueStrategy() == DequeueStrategy.FIFO && consumerConfig.getGroupSize() > 1) {
      byte[] stateContent = encodeStateColumn(ConsumerEntryState.CLAIMED);
      for (byte[] rowKey : consumingEntries.keySet()) {
        Put put = new Put(rowKey);
        put.add(QueueConstants.COLUMN_FAMILY, stateColumnName, stateContent);
        ops.add(put);
      }
    } else {
      for (byte[] rowKey : consumingEntries.keySet()) {
        Delete delete = new Delete(rowKey);
        delete.deleteColumn(QueueConstants.COLUMN_FAMILY, stateColumnName);
        ops.add(delete);
      }
    }

    hTable.batch(ops);
    hTable.flushCommits();
    return true;
  }

  @Override
  public void close() throws IOException {
    try {
      if (transaction != null) {
        // Use whatever last transaction for eviction.
        // Has to block until eviction is completed
        Uninterruptibles.getUninterruptibly(queueEvictor.evict(transaction),
                                            EVICTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
    } catch (ExecutionException e) {
      LOG.warn("Failed to perform queue eviction.", e.getCause());
    } catch (TimeoutException e) {
      LOG.warn("Timeout when performing queue eviction.", e);
    } finally {
      hTable.close();
    }
  }

  /**
   * Try to dequeue (claim) entries up to a maximum size.
   * @param entries For claimed entries to fill in.
   * @param maxBatchSize Maximum number of entries to claim.
   * @return The entries instance.
   * @throws IOException
   */
  private boolean getEntries(SortedMap<byte[], HBaseQueueEntry> entries, int maxBatchSize) throws IOException {
    boolean hasEntry = fetchFromCache(entries, maxBatchSize);

    // If not enough entries from the cache, try to get more.
    // ANDREAS: I think this is wrong. If the batch=10, and the cache has 5 entries, but populateCache cannot
    // fetch more entries, then we have 5 and should return true. But this code will return false.
    // TERENCE: If there are 5 entries in the cache, the first call to fetchFromCache will return true,
    // the second call to fetchFromCache from call to populateCache will return false, but
    // hasEntry = false || true => true, hence returning true.
    if (entries.size() < maxBatchSize) {
      populateRowCache(entries.keySet());
      hasEntry = fetchFromCache(entries, maxBatchSize) || hasEntry;
    }

    return hasEntry;
  }

  private boolean fetchFromCache(SortedMap<byte[], HBaseQueueEntry> entries, int maxBatchSize) {
    if (entryCache.isEmpty()) {
      return false;
    }

    Iterator<Map.Entry<byte[], HBaseQueueEntry>> iterator = entryCache.entrySet().iterator();
    while (entries.size() < maxBatchSize && iterator.hasNext()) {
      Map.Entry<byte[], HBaseQueueEntry> entry = iterator.next();
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
    // ANDREAS it seems that startRow never gets updated. That means we will always rescan entries that we have
    // already read and decided to ignore.
    // TERENCE: The update is done in the shouldInclude() method.
    scan.setStopRow(getStopRow());
    scan.addColumn(QueueConstants.COLUMN_FAMILY, QueueConstants.DATA_COLUMN);
    scan.addColumn(QueueConstants.COLUMN_FAMILY, QueueConstants.META_COLUMN);
    scan.addColumn(QueueConstants.COLUMN_FAMILY, stateColumnName);
    scan.setFilter(createFilter());

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
        long writePointer = Bytes.toLong(rowKey, queueRowPrefix.length, Longs.BYTES);

        // If writes later than the reader pointer, abort the loop, as entries that comes later are all uncommitted.
        if (writePointer > readPointer) {
          // ANDREAS: since we limit the scan to end at getStopRow(), I don't think this can ever happen? Also,
          // it would not be visible under the read pointer... but why do we not limit the scan's versions to the
          // read pointer?
          // TERENCE: I think you are right, this condition is not needed. We are not using scan versions because
          // entries are written with timestamp, not with enqueue write pointer.
          break;
        }

        // If the write is in the excluded list, ignore it.
        if (Arrays.binarySearch(excludedList, writePointer) >= 0) {
          continue;
        }

        // Based on the strategy to determine if include the given entry or not.
        KeyValue metaColumn = result.getColumnLatest(QueueConstants.COLUMN_FAMILY,
                                                     QueueConstants.META_COLUMN);
        KeyValue stateColumn = result.getColumnLatest(QueueConstants.COLUMN_FAMILY,
                                                      stateColumnName);

        int counter = Bytes.toInt(rowKey, rowKey.length - 4, Ints.BYTES);
        if (!shouldInclude(writePointer, counter, metaColumn, stateColumn)) {
          continue;
        }

        entryCache.put(rowKey, new HBaseQueueEntry(rowKey,
                                         result.getValue(QueueConstants.COLUMN_FAMILY,
                                                         QueueConstants.DATA_COLUMN),
                                         result.getValue(QueueConstants.COLUMN_FAMILY,
                                                         stateColumnName)));
      }
    }
    scanner.close();
  }

  /**
   * Creates a HBase filter that will filter out rows that that has committed state = PROCESSED
   */
  private Filter createFilter() {
    return new FilterList(FilterList.Operator.MUST_PASS_ONE, processedStateFilter, new SingleColumnValueFilter(
      QueueConstants.COLUMN_FAMILY, stateColumnName, CompareFilter.CompareOp.GREATER,
      new BinaryPrefixComparator(Bytes.toBytes(transaction.getReadPointer()))
    ));
  }

  /**
   * Creates a HBase filter that will filter out rows with state column state = PROCESSED (ignoring transaction)
   */
  private Filter createStateFilter() {
    byte[] processedMask = new byte[Ints.BYTES * 2 + 1];
    processedMask[processedMask.length - 1] = ConsumerEntryState.PROCESSED.getState();
    return new SingleColumnValueFilter(QueueConstants.COLUMN_FAMILY, stateColumnName,
                                       CompareFilter.CompareOp.NOT_EQUAL,
                                       new BitComparator(processedMask, BitComparator.BitwiseOp.AND));
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
    return Bytes.add(queueRowPrefix, Bytes.toBytes(transaction.getReadPointer() + 1L));
  }

  private byte[] getNextRow(long writePointer, int count) {
    return Bytes.add(queueRowPrefix, Bytes.toBytes(writePointer), Bytes.toBytes(count + 1));
  }

  /**
   * Implementation of dequeue result.
   */
  private final class HBaseDequeueResult implements DequeueResult {

    private final List<HBaseQueueEntry> entries;

    private HBaseDequeueResult(Iterable<HBaseQueueEntry> entries) {
      this.entries = ImmutableList.copyOf(entries);
    }

    @Override
    public boolean isEmpty() {
      return entries.isEmpty();
    }

    @Override
    public void skip() {
      // Simply put all entries into consumingEntries and clear those up from the entry cache as well.
      for (HBaseQueueEntry entry : entries) {
        consumingEntries.put(entry.getRowKey(), entry);
        entryCache.remove(entry.getRowKey());
      }
    }

    @Override
    public Iterator<byte[]> iterator() {
      if (isEmpty()) {
        return Iterators.emptyIterator();
      }
      return Iterators.transform(entries.iterator(), ENTRY_TO_BYTE_ARRAY);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("size", entries.size())
        .add("queue", queueName)
        .add("config", consumerConfig)
        .toString();
    }
  }
}
