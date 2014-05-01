/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.utils.ImmutablePair;
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
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;

/**
 * Common queue consumer for persisting engines such as HBase and LevelDB.
 */
public abstract class AbstractQueue2Consumer implements Queue2Consumer, TransactionAware, Closeable {

  private static final DequeueResult<byte[]> EMPTY_RESULT = DequeueResult.Empty.result();

  // TODO: Make these configurable.
  // Minimum number of rows to fetch per scan.
  private static final int MIN_FETCH_ROWS = 100;
  // Multiple of batches to fetch per scan.
  // Number of rows to scan = max(MIN_FETCH_ROWS, dequeueBatchSize * groupSize * PREFETCH_BATCHES)
  private static final int PREFETCH_BATCHES = 10;

  private static final Function<SimpleQueueEntry, byte[]> ENTRY_TO_BYTE_ARRAY =
    new Function<SimpleQueueEntry, byte[]>() {
    @Override
    public byte[] apply(SimpleQueueEntry input) {
      return input.getData();
    }
  };

  private final ConsumerConfig consumerConfig;
  private final QueueName queueName;
  private final SortedMap<byte[], SimpleQueueEntry> entryCache;
  private final NavigableMap<byte[], SimpleQueueEntry> consumingEntries;
  protected final byte[] stateColumnName;
  private final byte[] queueRowPrefix;
  protected byte[] startRow;
  private byte[] scanStartRow;
  protected Transaction transaction;
  private boolean committed;
  protected int commitCount;

  protected abstract boolean claimEntry(byte[] rowKey, byte[] stateContent) throws IOException;
  protected abstract void updateState(Set<byte[]> rowKeys, byte[] stateColumnName, byte[] stateContent)
    throws IOException;
  protected abstract void undoState(Set<byte[]> rowKeys, byte[] stateColumnName)
    throws IOException, InterruptedException;
  protected abstract QueueScanner getScanner(byte[] startRow, byte[] stopRow, int numRows) throws IOException;

  protected AbstractQueue2Consumer(ConsumerConfig consumerConfig, QueueName queueName) {
    this.consumerConfig = consumerConfig;
    this.queueName = queueName;
    this.entryCache = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    this.consumingEntries = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    this.queueRowPrefix = QueueEntryRow.getQueueRowPrefix(queueName);
    this.startRow = getRowKey(0L, 0);
    this.stateColumnName = Bytes.add(QueueEntryRow.STATE_COLUMN_PREFIX,
                                     Bytes.toBytes(consumerConfig.getGroupId()));
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
  public DequeueResult<byte[]> dequeue() throws IOException {
    return dequeue(1);
  }

  @Override
  public DequeueResult<byte[]> dequeue(int maxBatchSize) throws IOException {
    Preconditions.checkArgument(maxBatchSize > 0, "Batch size must be > 0.");

    // pre-compute the "claimed" state content in case of FIFO.
    byte[] claimedStateValue = null;
    if (consumerConfig.getDequeueStrategy() == DequeueStrategy.FIFO && consumerConfig.getGroupSize() > 1) {
      claimedStateValue = encodeStateColumn(ConsumerEntryState.CLAIMED);
    }
    while (consumingEntries.size() < maxBatchSize && getEntries(consumingEntries, maxBatchSize)) {

      // ANDREAS: this while loop should stop once getEntries/populateCache reaches the end of the queue. Currently, it
      // will retry as long as it gets at least one entry in every round, even if that is an entry that must be ignored
      // because it cannot be claimed.
      // ANDREAS: It could be a problem that we always read to the end of the queue. This way one flowlet instance may
      // always all entries, while others are idle.

      // For FIFO, need to try claiming the entry if group size > 1
      if (consumerConfig.getDequeueStrategy() == DequeueStrategy.FIFO && consumerConfig.getGroupSize() > 1) {
        Iterator<Map.Entry<byte[], SimpleQueueEntry>> iterator = consumingEntries.entrySet().iterator();
        while (iterator.hasNext()) {
          SimpleQueueEntry entry = iterator.next().getValue();

          if (entry.getState() == null ||
            QueueEntryRow.getStateInstanceId(entry.getState()) >= consumerConfig.getGroupSize()) {
            // If not able to claim it, remove it, and move to next one.
            if (!claimEntry(entry.getRowKey(), claimedStateValue)) {
              iterator.remove();
            }
          }
        }
      }
    }

    // If nothing get dequeued, return the empty result.
    if (consumingEntries.isEmpty()) {
      return EMPTY_RESULT;
    }

    return new SimpleDequeueResult(consumingEntries.values());
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
    updateState(consumingEntries.keySet(), stateColumnName, stateContent);
    commitCount += consumingEntries.size();
    committed = true;
    return true;
  }

  @Override
  public void postTxCommit() {
    if (scanStartRow != null) {
      if (!consumingEntries.isEmpty()) {
        // Start row can be updated to the largest rowKey in the consumingEntries (now is consumed)
        // that is smaller than or equal to scanStartRow
        byte[] floorKey = consumingEntries.floorKey(scanStartRow);
        if (floorKey != null) {
          startRow = floorKey;
        }
      } else {
        // If the dequeue has empty result, startRow can advance to scanStartRow
        startRow = Arrays.copyOf(scanStartRow, scanStartRow.length);
      }
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
    commitCount -= consumingEntries.size();

    // Revert changes in HBase rows
    // If it is FIFO, restore to the CLAIMED state. This instance will retry it on the next dequeue.
    if (consumerConfig.getDequeueStrategy() == DequeueStrategy.FIFO && consumerConfig.getGroupSize() > 1) {
      byte[] stateContent = encodeStateColumn(ConsumerEntryState.CLAIMED);
      updateState(consumingEntries.keySet(), stateColumnName, stateContent);
    } else {
      undoState(consumingEntries.keySet(), stateColumnName);
    }
    return true;
  }

  /**
   * Try to dequeue (claim) entries up to a maximum size.
   * @param entries For claimed entries to fill in.
   * @param maxBatchSize Maximum number of entries to claim.
   * @return The entries instance.
   * @throws java.io.IOException
   */
  private boolean getEntries(SortedMap<byte[], SimpleQueueEntry> entries, int maxBatchSize) throws IOException {
    boolean hasEntry = fetchFromCache(entries, maxBatchSize);

    // If not enough entries from the cache, try to get more.
    // ANDREAS: I think this is wrong. If the batch=10, and the cache has 5 entries, but populateCache cannot
    // fetch more entries, then we have 5 and should return true. But this code will return false.
    // TERENCE: If there are 5 entries in the cache, the first call to fetchFromCache will return true,
    // the second call to fetchFromCache from call to populateCache will return false, but
    // hasEntry = false || true => true, hence returning true.
    if (entries.size() < maxBatchSize) {
      populateRowCache(entries.keySet(), maxBatchSize);
      hasEntry = fetchFromCache(entries, maxBatchSize) || hasEntry;
    }

    return hasEntry;
  }

  private boolean fetchFromCache(SortedMap<byte[], SimpleQueueEntry> entries, int maxBatchSize) {
    if (entryCache.isEmpty()) {
      return false;
    }

    Iterator<Map.Entry<byte[], SimpleQueueEntry>> iterator = entryCache.entrySet().iterator();
    while (entries.size() < maxBatchSize && iterator.hasNext()) {
      Map.Entry<byte[], SimpleQueueEntry> entry = iterator.next();
      entries.put(entry.getKey(), entry.getValue());
      iterator.remove();
    }
    return true;
  }

  private void populateRowCache(Set<byte[]> excludeRows, int maxBatchSize) throws IOException {

    long readPointer = transaction.getReadPointer();

    // Scan the table for queue entries.
    int numRows = Math.max(MIN_FETCH_ROWS, maxBatchSize * PREFETCH_BATCHES);
    if (scanStartRow == null) {
      scanStartRow = Arrays.copyOf(startRow, startRow.length);
    }
    QueueScanner scanner = getScanner(scanStartRow,
                                      QueueEntryRow.getStopRowForTransaction(queueRowPrefix, transaction),
                                      numRows);
    try {
      // Try fill up the cache
      boolean firstScannedRow = true;

      while (entryCache.size() < numRows) {
        ImmutablePair<byte[], Map<byte[], byte[]>> entry = scanner.next();
        if (entry == null) {
          // No more result, breaking out.
          break;
        }

        byte[] rowKey = entry.getFirst();
        if (excludeRows.contains(rowKey)) {
          continue;
        }

        // Row key is queue_name + writePointer + counter
        long writePointer = Bytes.toLong(rowKey, queueRowPrefix.length, Longs.BYTES);

        // If it is first row returned by the scanner and was written before the earliest in progress,
        // it's safe to advance scanStartRow to current row because nothing can be written before this row.
        if (firstScannedRow && writePointer < transaction.getFirstInProgress()) {
          firstScannedRow = false;
          scanStartRow = Arrays.copyOf(rowKey, rowKey.length);
        }

        // If writes later than the reader pointer, abort the loop, as entries that comes later are all uncommitted.
        // this is probably not needed due to the limit of the scan to the stop row, but to be safe...
        if (writePointer > readPointer) {
          break;
        }
        // If the write is in the excluded list, ignore it.
        if (transaction.isExcluded(writePointer)) {
          continue;
        }

        // Based on the strategy to determine if include the given entry or not.
        byte[] dataBytes = entry.getSecond().get(QueueEntryRow.DATA_COLUMN);
        byte[] metaBytes = entry.getSecond().get(QueueEntryRow.META_COLUMN);

        if (dataBytes == null || metaBytes == null) {
          continue;
        }

        byte[] stateBytes = entry.getSecond().get(stateColumnName);

        int counter = Bytes.toInt(rowKey, rowKey.length - 4, Ints.BYTES);
        if (!shouldInclude(writePointer, counter, metaBytes, stateBytes)) {
          continue;
        }

        entryCache.put(rowKey, new SimpleQueueEntry(rowKey, dataBytes, stateBytes));
      }
    } finally {
      scanner.close();
    }
  }

  private byte[] encodeStateColumn(ConsumerEntryState state) {
    // State column content is encoded as (writePointer) + (instanceId) + (state)
    byte[] stateContent = new byte[Longs.BYTES + Ints.BYTES + 1];
    Bytes.putLong(stateContent, 0, transaction.getWritePointer());
    Bytes.putInt(stateContent, Longs.BYTES, consumerConfig.getInstanceId());
    Bytes.putByte(stateContent, Longs.BYTES + Ints.BYTES, state.getState());
    return stateContent;
  }

  private boolean shouldInclude(long enqueueWritePointer, int counter,
                                byte[] metaValue, byte[] stateValue) throws IOException {

    QueueEntryRow.CanConsume canConsume =
      QueueEntryRow.canConsume(consumerConfig, transaction, enqueueWritePointer, counter, metaValue, stateValue);

    if (QueueEntryRow.CanConsume.NO_INCLUDING_ALL_OLDER == canConsume) {
      scanStartRow = getNextRow(scanStartRow, enqueueWritePointer, counter);
      return false;
    }

    return QueueEntryRow.CanConsume.YES == canConsume;
  }

  /**
   * Creates a new byte[] that gives the entry row key for the given enqueue transaction and counter.
   */
  private byte[] getRowKey(long writePointer, int count) {
    byte[] row = Arrays.copyOf(queueRowPrefix, queueRowPrefix.length + Longs.BYTES + Ints.BYTES);
    Bytes.putLong(row, queueRowPrefix.length, writePointer);
    Bytes.putInt(row, queueRowPrefix.length + Longs.BYTES, count);
    return row;
  }

  /**
   * Get the next row based on the given write pointer and counter. It modifies the given row byte[] in place
   * and returns it.
   */
  private byte[] getNextRow(byte[] row, long writePointer, int count) {
    Bytes.putLong(row, queueRowPrefix.length, writePointer);
    Bytes.putInt(row, queueRowPrefix.length + Longs.BYTES, count + 1);
    return row;
  }

  @Override
  public String getName() {
    return getClass().getSimpleName() + "(queue = " + queueName + ")";
  }

  /**
   * Implementation of dequeue result.
   */
  private final class SimpleDequeueResult implements DequeueResult<byte[]> {

    private final List<SimpleQueueEntry> entries;

    private SimpleDequeueResult(Iterable<SimpleQueueEntry> entries) {
      this.entries = ImmutableList.copyOf(entries);
    }

    @Override
    public boolean isEmpty() {
      return entries.isEmpty();
    }

    @Override
    public void reclaim() {
      // Simply put all entries into consumingEntries and clear those up from the entry cache as well.
      for (SimpleQueueEntry entry : entries) {
        consumingEntries.put(entry.getRowKey(), entry);
        entryCache.remove(entry.getRowKey());
      }
    }

    @Override
    public int size() {
      return entries.size();
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
