/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.utils.ImmutablePair;
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
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Uninterruptibles;
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
 * Common queue consumer for persisting engines such as HBase and LevelDB.
 */
public abstract class AbstractQueue2Consumer implements Queue2Consumer, TransactionAware, Closeable {

  // TODO: Make these configurable.
  protected static final int MAX_CACHE_ROWS = 100;
  private static final long EVICTION_TIMEOUT_SECONDS = 10;
  // How many commits to trigger eviction.
  private static final int EVICTION_LIMIT = 1000;

  private static final Logger LOG = LoggerFactory.getLogger(AbstractQueue2Consumer.class);

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
  private final SortedMap<byte[], SimpleQueueEntry> consumingEntries;
  protected final byte[] stateColumnName;
  private final byte[] queueRowPrefix;
  private final QueueEvictor queueEvictor;
  private byte[] startRow;
  protected Transaction transaction;
  private boolean committed;
  private int commitCount;

  protected abstract boolean claimEntry(byte[] rowKey, byte[] stateContent) throws IOException;
  protected abstract void updateState(Set<byte[]> rowKeys, byte[] stateColumnName, byte[] stateContent)
    throws IOException;
  protected abstract void undoState(Set<byte[]> rowKeys, byte[] stateColumnName)
    throws IOException, InterruptedException;
  protected abstract QueueScanner getScanner(byte[] startRow, byte[] stopRow) throws IOException;

  protected AbstractQueue2Consumer(ConsumerConfig consumerConfig, QueueName queueName, QueueEvictor queueEvictor) {
    this.consumerConfig = consumerConfig;
    this.queueName = queueName;
    this.entryCache = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    this.consumingEntries = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    this.queueRowPrefix = QueueUtils.getQueueRowPrefix(queueName);
    this.startRow = queueRowPrefix;
    this.stateColumnName = Bytes.add(QueueConstants.STATE_COLUMN_PREFIX,
                                     Bytes.toBytes(consumerConfig.getGroupId()));
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

          // If the state is already in CLAIMED state, no need to claim it again
          // It happens for rolled-backed entries or restart from failure
          // The pickup logic in populateCache and shouldInclude() make sure that's the case
          // ANDREAS: but how do we know that it was claimed by THIS consumer. If there are multiple consumers,
          // then they all will pick it up, right?
          // TERENCE: The populateCache has logic to make sure only THIS consumer will pick it up again, except for
          // the case that group size reduced and the claimed consumer is no longer available.
          if (entry.getState() == null || getStateInstanceId(entry.getState()) >= consumerConfig.getGroupSize()) {
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
      return DequeueResult.EMPTY_RESULT;
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
    // TODO pre-compute this in constructor
    byte[] stateContent = encodeStateColumn(ConsumerEntryState.PROCESSED);
    updateState(consumingEntries.keySet(), stateColumnName, stateContent);
    commitCount += consumingEntries.size();
    committed = true;
    return true;
  }

  @Override
  public void postTxCommit() {
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
    }
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
      populateRowCache(entries.keySet());
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

  private void populateRowCache(Set<byte[]> excludeRows) throws IOException {

    long readPointer = transaction.getReadPointer();
    long[] excludedList = transaction.getExcludedList();

    // Scan the table for queue entries.
    QueueScanner scanner = getScanner(startRow, QueueUtils.getStopRowForTransaction(queueRowPrefix, transaction));

    try {
      // Try fill up the cache with at most MAX_CACHE_ROWS
      while (entryCache.size() < MAX_CACHE_ROWS) {
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

        // If writes later than the reader pointer, abort the loop, as entries that comes later are all uncommitted.
        // this is probably not needed due to the limit of the scan to the stop row, but to be safe...
        if (writePointer > readPointer) {
          break;
        }
        // If the write is in the excluded list, ignore it.
        if (Arrays.binarySearch(excludedList, writePointer) >= 0) {
          continue;
        }

        // Based on the strategy to determine if include the given entry or not.
        byte[] metaBytes = entry.getSecond().get(QueueConstants.META_COLUMN);
        byte[] stateBytes = entry.getSecond().get(stateColumnName);

        int counter = Bytes.toInt(rowKey, rowKey.length - 4, Ints.BYTES);
        if (!shouldInclude(writePointer, counter, metaBytes, stateBytes)) {
          continue;
        }

        entryCache.put(rowKey,
                       new SimpleQueueEntry(rowKey, entry.getSecond().get(QueueConstants.DATA_COLUMN), stateBytes));
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

  private long getStateWritePointer(byte[] stateBytes) {
    return Bytes.toLong(stateBytes, 0, Longs.BYTES);
  }

  private int getStateInstanceId(byte[] stateBytes) {
    return Bytes.toInt(stateBytes, Longs.BYTES, Ints.BYTES);
  }

  private ConsumerEntryState getState(byte[] stateBytes) {
    return ConsumerEntryState.fromState(stateBytes[Longs.BYTES + Ints.BYTES]);
  }

  private boolean shouldInclude(long enqueueWritePointer, int counter,
                                byte[] metaValue, byte[] stateValue) throws IOException {
    if (stateValue != null) {
      // If the state is written by the current transaction, ignore it, as it's processing
      long stateWritePointer = getStateWritePointer(stateValue);
      if (stateWritePointer == transaction.getWritePointer()) {
        return false;
      }

      // If the state was updated by a different consumer instance that is still active, ignore this entry.
      // The assumption is, the corresponding instance is either processing (claimed)
      // or going to process it (due to rollback/restart).
      // This only applies to FIFO, as for hash and rr, repartition needs to happen if group size change.
      int stateInstanceId = getStateInstanceId(stateValue);
      if (consumerConfig.getDequeueStrategy() == DequeueStrategy.FIFO
          && stateInstanceId < consumerConfig.getGroupSize()
          && stateInstanceId != consumerConfig.getInstanceId()) {
        return false;
      }

      // If state is PROCESSED and committed, ignore it
      long[] excludedList = transaction.getExcludedList();
      ConsumerEntryState state = getState(stateValue);
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
        Map<String, Integer> hashKeys = QueueEntry.deserializeHashKeys(metaValue);
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

  private byte[] getNextRow(long writePointer, int count) {
    return Bytes.add(queueRowPrefix, Bytes.toBytes(writePointer), Bytes.toBytes(count + 1));
  }

  /**
   * Implementation of dequeue result.
   */
  private final class SimpleDequeueResult implements DequeueResult {

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
