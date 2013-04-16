package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.common.utils.Bytes;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.ttqueue.internal.EntryMeta;
import com.continuuity.data.operation.ttqueue.internal.GroupState;
import com.continuuity.data.table.VersionedColumnarTable;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class TTQueueNewOnVCTable implements TTQueue {

  private static final Logger LOG = LoggerFactory.getLogger(TTQueueNewOnVCTable.class);
  protected final VersionedColumnarTable table;
  private final byte [] queueName;
  final TransactionOracle oracle;

  public static final String TTQUEUE_BATCH_SIZE_DEFAULT = "ttqueue.batch.size.default";
  public static final String TTQUEUE_EVICT_INTERVAL_SECS = "ttqueue.evict.interval.secs";
  public static final String TTQUEUE_MAX_CRASH_DEQUEUE_TRIES = "ttqueue.max.crash.dequeue.tries";
  public static final String TTQUEUE_MAX_CONSUMER_COUNT = "ttqueue.max.consumer.count";

  // For testing
  AtomicLong dequeueReturns = new AtomicLong(0);

  // Row prefix names
  // Row prefix for column GLOBAL_ENTRYID_COUNTER
  static final byte [] GLOBAL_ENTRY_ID_PREFIX = {10, 'I'};  //row <queueName>10I
  // Row prefix for columns containing queue entry
  static final byte [] GLOBAL_DATA_PREFIX = {20, 'D'};   //row <queueName>20D
  // Row prefix for columns containing global eviction data for consumer groups
  static final byte [] GLOBAL_EVICT_META_ROW = {30, 'M'};   //row <queueName>30M
  // Row prefix for columns containing consumer specific information
  static final byte [] CONSUMER_META_PREFIX = {40, 'C'}; //row <queueName>40C
  // Row prefix for columns that don't affect the operation of queue, i.e. groupId generation
  static final byte [] GLOBAL_GENERIC_PREFIX = {50, 'G'}; //row <queueName>50G

  // Columns for row = GLOBAL_ENTRY_ID_PREFIX
  // GLOBAL_ENTRYID_COUNTER contains the counter to generate entryIds during enqueue operation. There is only one such counter for a queue.
  // GLOBAL_ENTRYID_COUNTER contains the highest valid entryId for the queue
  static final byte [] GLOBAL_ENTRYID_COUNTER = {10, 'I'};  //row <queueName>10I, column 10I

  // GROUP_READ_POINTER is a group counter used by consumers of a FifoDequeueStrategy group to claim queue entries.
  // GROUP_READ_POINTER contains the higest entryId claimed by consumers of a FifoDequeueStrategy group
  static final byte [] GROUP_READ_POINTER = {10, 'I'}; //row <queueName>10I<groupId>, column 10I

  // Columns for row = GLOBAL_DATA_PREFIX (Global data, shared by all consumer groups)
  // ENTRY_META contains the meta data for a queue entry, whether the entry is invalid or not.
  static final byte [] ENTRY_META = {10, 'M'}; //row  <queueName>20D<entryId>, column 10M
  // ENTRY_DATA contains the queue entry.
  static final byte [] ENTRY_DATA = {20, 'D'}; //row  <queueName>20D<entryId>, column 20D
  // ENTRY_HEADER contains the partitioning keys of a queue entry.
  static final byte [] ENTRY_HEADER = {30, 'H'};  //row  <queueName>20D<entryId>, column 30H

  // GLOBAL_LAST_EVICT_ENTRY contains the entryId of the max evicted entry of the queue.
  // if GLOBAL_LAST_EVICT_ENTRY is not invalid, GLOBAL_LAST_EVICT_ENTRY + 1 points to the first queue entry that can be dequeued.
  static final byte [] GLOBAL_LAST_EVICT_ENTRY = {10, 'L'};   //row  <queueName>30M<groupId>, column 10L

  // Columns for row = GLOBAL_EVICT_META_ROW (Global data, shared by all consumers)
  // GROUP_EVICT_ENTRY contains the entryId upto which the queue entries can be evicted for a group.
  // It means all consumers in the group have acked until GROUP_EVICT_ENTRY
  static final byte [] GROUP_EVICT_ENTRY = {20, 'E'};     //row  <queueName>30M<groupId>, column 20E

  // Columns for row = CONSUMER_META_PREFIX (consumer specific information)
  // DEQUEUE_ENTRY_SET contains a list of entries dequeued by a consumer, but not yet acked.
  static final byte [] DEQUEUE_ENTRY_SET = {10, 'A'};              //row <queueName>40C<groupId><consumerId>, column 10A
  // If CONSUMER_READ_POINTER is valid, CONSUMER_READ_POINTER contains the highest entryId that the consumer has read
  // (the consumer may not have completed processing the entry).
  // CONSUMER_READ_POINTER + 1 points to the next entry that the consumer can dequeue.
  static final byte [] CONSUMER_READ_POINTER = {20, 'R'};     //row <queueName>40C<groupId><consumerId>, column 20R
  // CLAIMED_ENTRY_BEGIN is used by a consumer of FifoDequeueStrategy to specify the start entryId of the batch of entries claimed by it.
  static final byte [] CLAIMED_ENTRY_LIST = {30, 'C'};       //row <queueName>40C<groupId><consumerId>, column 30C
  // LAST_EVICT_TIME_IN_SECS is the time when the last eviction was run by the consumer
  static final byte [] LAST_EVICT_TIME_IN_SECS = {40, 'T'};           //row <queueName>40C<groupId><consumerId>, column 40T
  // RECONFIG_PARTITIONER stores the partition information for prior configurations
  static final byte [] RECONFIG_PARTITIONER = {50, 'P'};     //row <queueName>40C<groupId><consumerId>, column 50P

  static final long INVALID_ENTRY_ID = -1;
  static final long FIRST_QUEUE_ENTRY_ID = 1;

  private final long DEFAULT_BATCH_SIZE;
  private final long EVICT_INTERVAL_IN_SECS;
  private final int MAX_CRASH_DEQUEUE_TRIES;
  private final int MAX_CONSUMER_COUNT;

  protected TTQueueNewOnVCTable(VersionedColumnarTable table, byte[] queueName, TransactionOracle oracle,
                                final CConfiguration conf) {
    this.table = table;
    this.queueName = queueName;
    this.oracle = oracle;

    final long defaultBatchSize = conf.getLong(TTQUEUE_BATCH_SIZE_DEFAULT, 100);
    this.DEFAULT_BATCH_SIZE = defaultBatchSize > 0 ? defaultBatchSize : 100;

    final long evictIntervalInSecs = conf.getLong(TTQUEUE_EVICT_INTERVAL_SECS, 60);
    // Removing check for evictIntervalInSecs >= 0, since having it less than 0 does not cause errors in queue
    // behaviour. -ve evictIntervalInSecs is needed for unit tests.
    this.EVICT_INTERVAL_IN_SECS = evictIntervalInSecs;

    final int maxCrashDequeueTries = conf.getInt(TTQUEUE_MAX_CRASH_DEQUEUE_TRIES, 15);
    this.MAX_CRASH_DEQUEUE_TRIES = maxCrashDequeueTries > 0 ? maxCrashDequeueTries : 15;

    final int maxConsumerCount = conf.getInt(TTQUEUE_MAX_CONSUMER_COUNT, 1000);
    this.MAX_CONSUMER_COUNT = maxConsumerCount > 0 ? maxConsumerCount : 1000;
  }

  private long getBatchSize(QueueConfig queueConfig) {
    if(queueConfig.getBatchSize() > 0) {
      return queueConfig.getBatchSize();
    }
    return DEFAULT_BATCH_SIZE;
  }

  @Override
  public EnqueueResult enqueue(QueueEntry entry, long cleanWriteVersion) throws OperationException {
    byte[] data = entry.getData();
    if (LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage("Enqueueing (data.len=" + data.length + ", writeVersion=" + cleanWriteVersion + ")"));
    }

    // Get our unique entry id
    long entryId;
    try {
      // Make sure the increment below uses increment operation of the underlying implementation directly
      // so that it is atomic (Eg. HBase increment operation)
      entryId = this.table.incrementAtomicDirtily(makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 1);
    } catch (OperationException e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR,
         getLogMessage(String.format("Increment of global entry id failed with status code %d : %s",
                                     e.getStatus(), e.getMessage())), e);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage("New enqueue got entry id " + entryId));
    }

    /*
    Insert entry with version=<cleanWriteVersion> and
    row-key = <queueName>20D<entryId> , column/value 20D/<data>, 10M/EntryState.VALID, 30H<partitionKey>/<hashValue>
    */

    final int size = entry.getPartitioningMap().size() + 2;
    byte[][] colKeys = new byte[size][];
    byte[][] colValues = new byte[size][];

    int colKeyIndex = 0;
    int colValueIndex = 0;
    colKeys[colKeyIndex++] = ENTRY_DATA;
    colKeys[colKeyIndex++] = ENTRY_META;
    colValues[colValueIndex++] = data;
    colValues[colValueIndex++] = new EntryMeta(EntryMeta.EntryState.VALID).getBytes();
    for(Map.Entry<String, Integer> e : entry.getPartitioningMap().entrySet()) {
      colKeys[colKeyIndex++] = makeColumnName(ENTRY_HEADER, e.getKey());
      colValues[colValueIndex++] = Bytes.toBytes(e.getValue());
    }

    this.table.put(makeRowKey(GLOBAL_DATA_PREFIX, entryId),
                   colKeys,
                   cleanWriteVersion,
                   colValues);

    // Return success with pointer to entry
    return new EnqueueResult(EnqueueResult.EnqueueStatus.SUCCESS, new QueueEntryPointer(this.queueName, entryId));
  }

  @Override
  public void invalidate(QueueEntryPointer entryPointer, long cleanWriteVersion) throws OperationException {
    if(LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage(String.format("Invalidating entry %d", entryPointer.getEntryId())));
    }
    final byte [] rowName = makeRowKey(GLOBAL_DATA_PREFIX, entryPointer.getEntryId());
    // Change meta data to INVALID
    // TODO: check if it okay to use cleanWriteVersion during invalidate,
    // TODO: what happens to values written using transaction pointer when transaction gets rolled back
    this.table.put(rowName, ENTRY_META,
                   cleanWriteVersion, new EntryMeta(EntryMeta.EntryState.INVALID).getBytes());
    // No need to delete data/headers since they will be cleaned up during eviction later
    if (LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage("Invalidated " + entryPointer));
    }
  }

  @Override
  public DequeueResult dequeue(QueueConsumer consumer, ReadPointer readPointer) throws OperationException {
    final QueueConfig config = consumer.getQueueConfig();
    if (LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage("Attempting dequeue [curNumDequeues=" + this.dequeueReturns.get() +
                  "] (" + consumer + ", " + config + ", " + readPointer + ")"));
    }

    // Determine what dequeue strategy to use based on the partitioner
    final DequeueStrategy dequeueStrategy = getDequeueStrategy(config.getPartitionerType().getPartitioner());
    final QueueStateImpl queueState = getQueueState(consumer, readPointer);

    // If single entry mode return the previously dequeued entry that was not acked, otherwise dequeue the next entry
    if(config.isSingleEntry()) {
      final DequeuedEntrySet dequeueEntrySet = queueState.getDequeueEntrySet();
      final TransientWorkingSet transientWorkingSet = queueState.getTransientWorkingSet();
      final Map<Long, byte[]> cachedEntries = queueState.getTransientWorkingSet().getCachedEntries();

      Preconditions.checkState(dequeueEntrySet.size() <= 1,
                               "More than 1 entry dequeued in single entry mode - %s", dequeueEntrySet);
      if(!dequeueEntrySet.isEmpty()) {
        DequeueEntry returnEntry = dequeueEntrySet.min();
        long returnEntryId = returnEntry.getEntryId();
        if(transientWorkingSet.hasNext() && transientWorkingSet.peekNext().getEntryId() == returnEntryId) {
          // Crash recovery case.
          // The cached entry list would not have been incremented for the first time in single entry mode
          transientWorkingSet.next();
        }

        byte[] entryBytes = cachedEntries.get(returnEntryId);
        if(entryBytes == null) {
          throw new OperationException(StatusCode.INTERNAL_ERROR,
                  getLogMessage(String.format("Cannot fetch dequeue entry id %d from cached entries", returnEntryId)));
        }
        QueueEntry entry = new QueueEntry(entryBytes);
        dequeueStrategy.saveDequeueState(consumer, config, queueState, readPointer);
        DequeueResult dequeueResult = new DequeueResult(DequeueResult.DequeueStatus.SUCCESS,
                                  new QueueEntryPointer(this.queueName, returnEntryId, returnEntry.getTries()), entry);
        return dequeueResult;
      }
    }

    // If no more cached entries, read entries from storage
    if(!queueState.getTransientWorkingSet().hasNext()) {
      // TODO: return a list of DequeueEntry instead of list of Long
      List<Long> entryIds = dequeueStrategy.fetchNextEntries(consumer, config, queueState, readPointer);
      readEntries(consumer, config, queueState, readPointer, entryIds);
    }

    if(queueState.getTransientWorkingSet().hasNext()) {
      DequeueEntry dequeueEntry = queueState.getTransientWorkingSet().next();
      this.dequeueReturns.incrementAndGet();
      queueState.getDequeueEntrySet().add(dequeueEntry);
      QueueEntry entry = new QueueEntry(queueState.getTransientWorkingSet()
                                          .getCachedEntries().get(dequeueEntry.getEntryId()));
      dequeueStrategy.saveDequeueState(consumer, config, queueState, readPointer);
      DequeueResult dequeueResult = new DequeueResult(DequeueResult.DequeueStatus.SUCCESS,
                     new QueueEntryPointer(this.queueName, dequeueEntry.getEntryId(), dequeueEntry.getTries()), entry);
      return dequeueResult;
    } else {
      // No queue entries available to dequue, return queue empty
      if (LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage("End of queue reached using " + "read pointer " + readPointer));
      }
      dequeueStrategy.saveDequeueState(consumer, config, queueState, readPointer);
      DequeueResult dequeueResult = new DequeueResult(DequeueResult.DequeueStatus.EMPTY);
      return dequeueResult;
    }
  }

  private DequeueStrategy getDequeueStrategy(QueuePartitioner queuePartitioner) throws OperationException {
    DequeueStrategy dequeueStrategy;
    if(queuePartitioner instanceof QueuePartitioner.HashPartitioner) {
      dequeueStrategy = new HashDequeueStrategy();
    } else if(queuePartitioner instanceof QueuePartitioner.RoundRobinPartitioner) {
      dequeueStrategy = new RoundRobinDequeueStrategy();
    } else if(queuePartitioner instanceof QueuePartitioner.FifoPartitioner) {
      dequeueStrategy = new FifoDequeueStrategy();
    } else {
      throw new OperationException(StatusCode.INTERNAL_ERROR,
         getLogMessage(String.format("Cannot figure out the dequeue strategy to use for partitioner %s",
                                     queuePartitioner.getClass())));
    }
    return dequeueStrategy;
  }

  private void readEntries(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                           ReadPointer readPointer, List<Long> entryIds) throws OperationException{
    if(LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage(String.format("Reading entries from storage - %s", entryIds)));
    }

    // Copy over the entries that are dequeued, but not yet acked
    Map<Long, byte[]> currentCachedEntries = queueState.getTransientWorkingSet().getCachedEntries();
    Map<Long, byte[]> newCachedEntries = Maps.newHashMap();
    for(long entryId : queueState.getDequeueEntrySet().getEntryIds()) {
      byte[] entry = currentCachedEntries.get(entryId);
      if(entry != null) {
        newCachedEntries.put(entryId, entry);
      }
    }

    List<Long> readEntryIds = Lists.newArrayListWithCapacity(entryIds.size());

    try {
      if(entryIds.isEmpty()) {
        return;
      }

      final byte[][] entryRowKeys = new byte[entryIds.size()][];
      for(int i = 0; i < entryIds.size(); ++i) {
        entryRowKeys[i] = makeRowKey(GLOBAL_DATA_PREFIX, entryIds.get(i));
      }

      final byte[][] entryColKeys = new byte[][]{ ENTRY_META, ENTRY_DATA };
      OperationResult<Map<byte[], Map<byte[], byte[]>>> entriesResult =
                                                    this.table.getAllColumns(entryRowKeys, entryColKeys, readPointer);
      if(entriesResult.isEmpty()) {
      } else {
        for(int i = 0; i < entryIds.size(); ++i) {
          Map<byte[], byte[]> entryMap = entriesResult.getValue().get(entryRowKeys[i]);
          if(entryMap == null) {
            if (LOG.isTraceEnabled()) {
              LOG.trace(getLogMessage(
                String.format("Not able to read entry with entryId %d. Returning empty cached list.",
                              entryIds.get(i))));
            }
            return;
          }
          byte[] entryMetaBytes = entryMap.get(ENTRY_META);
          if(entryMetaBytes == null) {
            if (LOG.isTraceEnabled()) {
              LOG.trace(getLogMessage(
                String.format("Not able to decode entry with entryId %d. Returning empty cached list.",
                              entryIds.get(i))));
            }
            return;
          }
          EntryMeta entryMeta = EntryMeta.fromBytes(entryMetaBytes);
          if (LOG.isTraceEnabled()) {
            LOG.trace(getLogMessage("entryId:" + entryIds.get(i) + ". entryMeta : " + entryMeta.toString()));
          }

          // Check if entry has been invalidated or evicted
          if (entryMeta.isInvalid() || entryMeta.isEvicted()) {
            if (LOG.isTraceEnabled()) {
              LOG.trace(getLogMessage("Found invalidated or evicted entry at " + entryIds.get(i) +
                          " (" + entryMeta.toString() + ")"));
            }
          } else {
            // Entry is visible and valid!
            assert(entryMeta.isValid());
            long entryId = entryIds.get(i);
            byte [] entryData = entryMap.get(ENTRY_DATA);
            newCachedEntries.put(entryId, entryData);
            readEntryIds.add(entryId);
          }
        }
      }
    } finally {
      // Update queue state
      queueState.setTransientWorkingSet(new TransientWorkingSet(readEntryIds, newCachedEntries));
    }
  }

  @Override
  public void ack(QueueEntryPointer entryPointer, QueueConsumer consumer, ReadPointer readPointer)
    throws OperationException {
    QueuePartitioner partitioner = consumer.getQueueConfig().getPartitionerType().getPartitioner();
    final DequeueStrategy dequeueStrategy = getDequeueStrategy(partitioner);

    // Get queue state
    QueueStateImpl queueState = getQueueState(consumer, readPointer);

    // Only the entry that has been dequeued by this consumer can be acked
    if(!queueState.getDequeueEntrySet().contains(entryPointer.getEntryId())) {
      throw new OperationException(StatusCode.ILLEGAL_ACK,
                 getLogMessage(String.format("Entry %d is not dequeued by this consumer. Current active entries are %s",
                                            entryPointer.getEntryId(), queueState.getDequeueEntrySet())));
    }

    // Set ack state
    // TODO: what happens when you ack and crash?
    queueState.getDequeueEntrySet().remove(entryPointer.getEntryId());

    // Write ack state
    dequeueStrategy.saveDequeueState(consumer, consumer.getQueueConfig(), queueState, readPointer);
  }

  @Override
  public void unack(QueueEntryPointer entryPointer, QueueConsumer consumer, ReadPointer readPointer)
    throws OperationException {
    // TODO: add tests for unack
    QueuePartitioner partitioner = consumer.getQueueConfig().getPartitionerType().getPartitioner();
    final DequeueStrategy dequeueStrategy = getDequeueStrategy(partitioner);

    // Get queue state
    QueueStateImpl queueState = getQueueState(consumer, readPointer);

    // Set unack state
    // TODO: 1. Check if entry was really acked
    // TODO: 2. If this is the first call after a consumer crashes, then this entry will not be present in the
    // TODO: 2. queue cache.
    queueState.getDequeueEntrySet().add(new DequeueEntry(entryPointer.getEntryId(), entryPointer.getTries()));

    // Write unack state
    dequeueStrategy.saveDequeueState(consumer, consumer.getQueueConfig(), queueState, readPointer);
  }

  @Override
  public int configure(QueueConsumer newConsumer) throws OperationException {
    // Delete state information of consumer, since it will be no longer valid after the configuration is done.
    newConsumer.setQueueState(null);

    // Simple leader election
    if(newConsumer.getInstanceId() != 0) {
      if(LOG.isTraceEnabled()) {
        LOG.trace("Only consumer with instanceID 0 can run configure. Current consumer's instance id = %d",
                  newConsumer.getInstanceId());
      }
      return -1;
    }

    final ReadPointer readPointer = TransactionOracle.DIRTY_READ_POINTER;
    final QueueConfig config = newConsumer.getQueueConfig();
    final int newConsumerCount = newConsumer.getGroupSize();
    final long groupId = newConsumer.getGroupId();

    if(LOG.isDebugEnabled()) {
      LOG.trace(getLogMessage(String.format(
        "Running configure with consumer=%s, readPointer= %s", newConsumer, readPointer)));
    }

    if(newConsumerCount < 1) {
      throw new OperationException(StatusCode.ILLEGAL_GROUP_CONFIG_CHANGE,
                        getLogMessage(String.format("New consumer count (%d) should atleast be 1", newConsumerCount)));
    }

    // Determine what dequeue strategy to use based on the partitioner
    final DequeueStrategy dequeueStrategy = getDequeueStrategy(config.getPartitionerType().getPartitioner());

    // Read queue state for all consumers of the group to find the number of previous consumers
    int oldConsumerCount = 0;
    List<QueueConsumer> consumers = Lists.newArrayList();
    List<QueueStateImpl> queueStates = Lists.newArrayList();
    for(int i = 0; i < MAX_CONSUMER_COUNT; ++i) {
      // Note: the consumers created here do not contain QueueConsumer.partitioningKey or QueueConsumer.groupSize
      StatefulQueueConsumer consumer = new StatefulQueueConsumer(i, groupId, MAX_CONSUMER_COUNT, config);
      QueueStateImpl queueState = null;
      try {
        // TODO: read queue state in one call
        queueState = dequeueStrategy.readQueueState(consumer, config, readPointer);
      } catch(OperationException e) {
        if(e.getStatus() != StatusCode.NOT_CONFIGURED) {
          throw e;
        }
      }
      if(queueState == null) {
        break;
      }

      ++oldConsumerCount;
      // Verify there are no inflight entries
      if(!queueState.getDequeueEntrySet().isEmpty()) {
        throw new OperationException(StatusCode.ILLEGAL_GROUP_CONFIG_CHANGE,
                     getLogMessage(String.format("Consumer %d still has inflight entries", consumer.getInstanceId())));
      }
      consumer.setQueueState(queueState);
      consumers.add(consumer);
      queueStates.add(queueState);
    }

    // Nothing to do if newConsumerCount == oldConsumerCount
    if(oldConsumerCount == newConsumerCount) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(String.format(
          "Nothing to configure since oldConsumerCount is equal to newConsumerCount (%d)", oldConsumerCount)));
      }
      return oldConsumerCount;
    }

    dequeueStrategy.configure(consumers, queueStates, config, groupId, oldConsumerCount, newConsumerCount, readPointer);

    // Delete eviction information for all groups
    // We get the list of groups to evict from the group eviction information. Whenever there is a configuration change
    // we'll need to delete the eviction information for all groups so that we always maintain eviction information for
    // active groups only
    EvictionState evictionState = new EvictionState(table);
    evictionState.deleteGroupEvictionState(readPointer, TransactionOracle.DIRTY_WRITE_VERSION);

    return oldConsumerCount;
  }

  @Override
  public void finalize(QueueEntryPointer entryPointer, QueueConsumer consumer, int totalNumGroups, long writePoint)
    throws OperationException {
    // Figure out queue entries that can be evicted, and evict them.
    // We are assuming here that for a given consumer all entries up to
    // min(min(DEQUEUE_ENTRY_SET)-1, CONSUMER_READ_POINTER-1) can be evicted.
    // The min of such evict entry is determined across all consumers across all groups,
    // and entries till the min evict entry are removed.

    // One consumer per consumer group will do the determination of min group evict entry for each group.
    // Finally, one consumer across all groups will get the least of min group evict entries for all groups
    // and does the eviction.

    // NOTE: Using min(min(DEQUEUE_ENTRY_SET)-1, CONSUMER_READ_POINTER-1) to determine evict entry removes the need of
    // storing/reading the finalized entry for each consumer.
    // However in this approach the last entry of each consumer may not get evicted.
    // This limitation should be okay since the number of such entries will be small
    // (less than or equal to the number of consumers).

    // A simple leader election for selecting consumer to run eviction for group
    // Only consumers with id 0 (one per group)
    if(consumer.getInstanceId() != 0) {
      return;
    }

    if(totalNumGroups <= 0) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(String.format("totalNumGroups=%d, nothing to be evicted", totalNumGroups)));
      }
      return;
    }

    // TODO: can finalize throw exceptions?

    ReadPointer readPointer = TransactionOracle.DIRTY_READ_POINTER;

    // Run eviction only if EVICT_INTERVAL_IN_SECS secs have passed since the last eviction run
    final long evictStartTimeInSecs = System.currentTimeMillis() / 1000;
    QueueStateImpl queueState = getQueueState(consumer, readPointer);
    if(evictStartTimeInSecs - queueState.getLastEvictTimeInSecs() < EVICT_INTERVAL_IN_SECS) {
      return;
    }

    // Record things that need to be written to storage
    List<byte[]> writeKeys = new ArrayList<byte[]>();
    List<byte[]> writeCols = new ArrayList<byte[]>();
    List<byte[]> writeValues = new ArrayList<byte[]>();

    // Save evictStartTimeInSecs as LAST_EVICT_TIME_IN_SECS
    writeKeys.add(makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()));
    writeCols.add(LAST_EVICT_TIME_IN_SECS);
    writeValues.add(Bytes.toBytes(evictStartTimeInSecs));
    queueState.setLastEvictTimeInSecs(evictStartTimeInSecs);

    if(LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage(String.format("Running eviction for group %d", consumer.getGroupId())));
    }

    // Find the min entry that can be evicted for the consumer's group
    final long minGroupEvictEntry = getMinGroupEvictEntry(consumer, entryPointer.getEntryId(), readPointer);
    // Save the minGroupEvictEntry for the consumer's group
    if(minGroupEvictEntry != INVALID_ENTRY_ID) {
      writeKeys.add(GLOBAL_EVICT_META_ROW);
      writeCols.add(makeColumnName(GROUP_EVICT_ENTRY, consumer.getGroupId()));
      writeValues.add(Bytes.toBytes(minGroupEvictEntry));
    }

    if(LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage(String.format(
        "minGroupEvictEntry=%d, groupId=%d", minGroupEvictEntry, consumer.getGroupId())));
    }

    // Only one consumer per queue will run the below eviction algorithm for the queue,
    // all others will save minGroupEvictEntry and return
    // If runEviction returns INVALID_ENTRY_ID, then this consumer did not run eviction
    final long currentMaxEvictedEntry = runEviction(consumer, minGroupEvictEntry, totalNumGroups, readPointer);
    // Save the max of the entries that were evicted now
    if(currentMaxEvictedEntry != INVALID_ENTRY_ID) {
      writeKeys.add(GLOBAL_LAST_EVICT_ENTRY);
      writeCols.add(GLOBAL_LAST_EVICT_ENTRY);
      writeValues.add(Bytes.toBytes(currentMaxEvictedEntry));
    }

    // Save the state
    byte[][] keyArray = new byte[writeKeys.size()][];
    byte[][] colArray = new byte[writeCols.size()][];
    byte[][] valArray = new byte[writeValues.size()][];

    // TODO: move all queue state manipulation into a single class
    table.put(writeKeys.toArray(keyArray), writeCols.toArray(colArray),
              TransactionOracle.DIRTY_WRITE_VERSION, writeValues.toArray(valArray));
  }

  private long getMinGroupEvictEntry(QueueConsumer consumer, long currentConsumerFinalizeEntry,
                                     ReadPointer readPointer) throws OperationException {
    // Find out the min entry that can be evicted across all consumers in the consumer's group

    // Read CONSUMER_READ_POINTER and DEQUEUE_ENTRY_SET for all consumers in the group to determine evict entry
    final byte[][] rowKeys = new byte[consumer.getGroupSize()][];
    for(int consumerId = 0; consumerId < consumer.getGroupSize(); ++consumerId) {
      rowKeys[consumerId] = makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumerId);
    }
    // TODO: move all queue state manipulation methods into single class
    OperationResult<Map<byte[], Map<byte[], byte[]>>> operationResult =
      table.getAllColumns(rowKeys,
                          new byte[][]{CONSUMER_READ_POINTER, DEQUEUE_ENTRY_SET}, TransactionOracle.DIRTY_READ_POINTER);
    if(operationResult.isEmpty()) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(String.format(
          "Not able to fetch state of group %d for eviction", consumer.getGroupId())));
      }
      return INVALID_ENTRY_ID;
    }

    long minGroupEvictEntry = Long.MAX_VALUE;
    for(int consumerId = 0; consumerId < consumer.getGroupSize(); ++consumerId) {
      // As far as consumer consumerId is concerned, all queue entries before
      // min(min(DEQUEUE_ENTRY_SET), CONSUMER_READ_POINTER) can be evicted
      // The least of such entry is the minGroupEvictEntry to which all queue entries can be evicted for the group
      long evictEntry = getEvictEntryForConsumer(operationResult, consumerId, consumer, currentConsumerFinalizeEntry);
      if(evictEntry == INVALID_ENTRY_ID) {
        minGroupEvictEntry = INVALID_ENTRY_ID;
        break;
      }
      // Save the min entry
      if(minGroupEvictEntry > evictEntry) {
        minGroupEvictEntry = evictEntry;
      }
    }
    return minGroupEvictEntry == Long.MAX_VALUE ? INVALID_ENTRY_ID : minGroupEvictEntry;
  }

  private long getEvictEntryForConsumer(OperationResult<Map<byte[], Map<byte[], byte[]>>> operationResult,
               int consumerId, QueueConsumer consumer, long currentConsumerFinalizeEntry) throws OperationException {
    // evictEntry determination logic:
    // evictEntry = (min(DEQUEUE_ENTRY_SET) != INVALID_ENTRY ? min(DEQUEUE_ENTRY_SET) - 1 : consumerReadPointer - 1)

    // Read the min(DEQUEUE_ENTRY_SET) and CONSUMER_READ_POINTER for the consumer consumerId
    Map<byte[], byte[]> readPointerMap = operationResult.getValue().get(
                                              makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumerId));
    if(readPointerMap == null) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(String.format("Not able to fetch readPointer/activeEntry for consumerId %d, groupId %d",
                                              consumerId, consumer.getGroupId())));
      }
      return INVALID_ENTRY_ID;
    }
    final byte[] dequeueEntrySetBytes = readPointerMap.get(DEQUEUE_ENTRY_SET);
    if(dequeueEntrySetBytes == null) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(String.format(
          "Not able to decode dequeue entry set for consumerId %d, groupId %d", consumerId, consumer.getGroupId())));
      }
      return INVALID_ENTRY_ID;
    }
    long evictEntry;
    DequeuedEntrySet dequeueEntrySet;
    try {
       dequeueEntrySet = DequeuedEntrySet.decode(new BinaryDecoder(new ByteArrayInputStream(dequeueEntrySetBytes)));
    } catch (IOException e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, getLogMessage(
        String.format("Exception while deserializing dequeue entry list during finalize for consumerId %d, groupId %d",
                      consumerId, consumer.getGroupId())), e);
    }
    if(!dequeueEntrySet.isEmpty()) {
      evictEntry = dequeueEntrySet.min().getEntryId() - 1;
    } else {
      // For consumer running the eviction, currentConsumerFinalizeEntry is a better evict entry
      // than CONSUMER_READ_POINTER - 1, since currentConsumerFinalizeEntry > CONSUMER_READ_POINTER - 1
      if(consumerId == consumer.getInstanceId()) {
        evictEntry = currentConsumerFinalizeEntry;
      } else {
        byte[] consumerReadPointerBytes = readPointerMap.get(CONSUMER_READ_POINTER);
        if(consumerReadPointerBytes == null) {
          if(LOG.isTraceEnabled()) {
            LOG.trace(getLogMessage(String.format(
              "Not able to decode readPointer for consumerId %d, groupId %d", consumerId, consumer.getGroupId())));
          }
          return INVALID_ENTRY_ID;
        }
        evictEntry = Bytes.toLong(consumerReadPointerBytes) - 1;
      }
    }
    return evictEntry;
  }

  static class EvictionState {
    private long globalLastEvictEntry = FIRST_QUEUE_ENTRY_ID - 1;
    private Map<Long, Long> groupEvictEntries = Maps.newHashMap();

    private final VersionedColumnarTable table;

    public EvictionState(VersionedColumnarTable table) {
      this.table = table;
    }

    public void readEvictionState(ReadPointer readPointer) throws OperationException {
      // Read GLOBAL_LAST_EVICT_ENTRY
      OperationResult<byte[]> lastEvictEntryBytes = table.get(GLOBAL_LAST_EVICT_ENTRY, GLOBAL_LAST_EVICT_ENTRY,
                                                              readPointer);
      if(!lastEvictEntryBytes.isEmpty() && lastEvictEntryBytes.getValue() != null) {
        globalLastEvictEntry = Bytes.toLong(lastEvictEntryBytes.getValue());
      }

      readGroupEvictInformationInternal(readPointer);
    }

    public void deleteGroupEvictionState(ReadPointer readPointer, long writeVersion) throws OperationException {
      if(groupEvictEntries.isEmpty()) {
        readGroupEvictInformationInternal(readPointer);
      }

      // If still no entries, noting to do
      if(groupEvictEntries.isEmpty()) {
        return;
      }

      // TODO: need to fix writing byte[0] to delete entries
      // Write byte[0] to delete entries
      byte[][] columnKeys = new byte[groupEvictEntries.size()][];
      byte[][] values = new byte[groupEvictEntries.size()][];
      int i = 0;
      for(Map.Entry<Long, Long> entry : groupEvictEntries.entrySet()) {
        columnKeys[i] = makeColumnName(GROUP_EVICT_ENTRY, entry.getKey());
        values[i] = Bytes.EMPTY_BYTE_ARRAY;
        ++i;
      }
      table.put(GLOBAL_EVICT_META_ROW, columnKeys, writeVersion, values);
    }

    public long getGlobalLastEvictEntry() {
      return globalLastEvictEntry;
    }

    public Set<Long> getGroupIds() {
      return groupEvictEntries.keySet();
    }

    public Long getGroupEvictEntry(long groupId) {
      return groupEvictEntries.get(groupId);
    }

    private void readGroupEvictInformationInternal(ReadPointer readPointer) throws OperationException {
      // Read evict information for all groups
      OperationResult<Map<byte[], byte[]>> groupEvictBytes = table.get(GLOBAL_EVICT_META_ROW, readPointer);
      if(!groupEvictBytes.isEmpty()) {
        for(Map.Entry<byte[], byte[]> entry : groupEvictBytes.getValue().entrySet()) {
          if(entry.getKey().length > 0 && entry.getValue().length > 0) {
            Long groupId = removePrefixFromLongId(entry.getKey(), GROUP_EVICT_ENTRY);
            if(groupId != null) {
              long groupEvictEntry = Bytes.toLong(entry.getValue());
              groupEvictEntries.put(groupId, groupEvictEntry);
            }
          }
        }
      }
    }
  }

  private long runEviction(QueueConsumer consumer, long currentGroupMinEvictEntry,
                           int totalNumGroups, ReadPointer readPointer) throws OperationException {
    // Read eviction state from table
    // TODO: move all queue state reads to a single class
    EvictionState evictionState = new EvictionState(table);
    evictionState.readEvictionState(readPointer);

    final long lastEvictedEntry = evictionState.getGlobalLastEvictEntry();

    // Continue eviction only if eviction information for all totalNumGroups groups can be read
    final int numGroupsRead = evictionState.getGroupIds().size();
    if(numGroupsRead < totalNumGroups) {
      if(LOG.isDebugEnabled()) {
        LOG.trace(getLogMessage(
          String.format("Cannot get eviction information for all %d groups, got only for %d groups. Aborting eviction.",
                        totalNumGroups, numGroupsRead)));
      }
      return INVALID_ENTRY_ID;
    } else if(numGroupsRead > totalNumGroups) {
      LOG.warn(getLogMessage(
        String.format("Getting eviction information for more groups (%d) than required (%d). %s",
                      numGroupsRead,
                      totalNumGroups,
                      "Looks like eviction state is corrupted. Aborting eviction.")));
      return INVALID_ENTRY_ID;
    }

    // Only one consumer across all groups can run eviction
    // Simple leader election: consumer with lowest groupId will run eviction.
    List<Long> groupIds = Lists.newArrayList(evictionState.getGroupIds());
    Collections.sort(groupIds);
    if(groupIds.get(0) != consumer.getGroupId()) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(String.format("Min groupId=%d, current consumer groupId=%d. Aborting eviction",
                                              groupIds.get(0), consumer.getGroupId())));
      }
      return INVALID_ENTRY_ID;
    }

    if(LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage("Running global eviction..."));
    }

    // Determine the max entry that can be evicted across all groups
    long maxEntryToEvict = Long.MAX_VALUE;
    for(long groupId : groupIds) {
      long entry;
      if(groupId == consumer.getGroupId()) {
        // Min evict entry for the consumer's group was just evaluated earlier but not written to storage, use that
        entry = currentGroupMinEvictEntry;
      } else {
        // Get the evict info for group with groupId
        entry = evictionState.getGroupEvictEntry(groupId);
      }
      // Save the least entry
      if(maxEntryToEvict > entry) {
        maxEntryToEvict = entry;
      }
    }

    if(maxEntryToEvict < FIRST_QUEUE_ENTRY_ID || maxEntryToEvict <= lastEvictedEntry ||
      maxEntryToEvict == Long.MAX_VALUE) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(String.format(
          "Nothing to evict. Entry to be evicted = %d, lastEvictedEntry = %d", maxEntryToEvict, lastEvictedEntry)));
      }
      return INVALID_ENTRY_ID;
    }

      final long startEvictEntry = lastEvictedEntry + 1;

      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(String.format("Evicting entries from %d to %d", startEvictEntry, maxEntryToEvict)));
      }

      // Evict entries
      int i = 0;
      byte[][] deleteKeys = new byte[(int) (maxEntryToEvict - startEvictEntry) + 1][];
      for(long id = startEvictEntry; id <= maxEntryToEvict; ++id) {
        deleteKeys[i++] = makeRowKey(GLOBAL_DATA_PREFIX, id);
      }
      this.table.deleteDirty(deleteKeys);

    return maxEntryToEvict;
  }

  @Override
  public long getGroupID() throws OperationException {
    // TODO: implement this :)
    return this.table.incrementAtomicDirtily(makeRowName(GLOBAL_GENERIC_PREFIX), Bytes.toBytes("GROUP_ID"), 1);
  }

  @Override
  public QueueAdmin.QueueInfo getQueueInfo() throws OperationException {
    // TODO: implement this :)
    return new QueueAdmin.QueueInfo(new QueueAdmin.QueueMeta(1, 1, new GroupState[0]));
  }

  private QueueStateImpl getQueueState(QueueConsumer consumer, ReadPointer readPointer) throws OperationException {
    // Determine what dequeue strategy to use based on the partitioner
    final DequeueStrategy dequeueStrategy =
      getDequeueStrategy(consumer.getQueueConfig().getPartitionerType().getPartitioner());

    QueueStateImpl queueState;
    // If QueueState is null, read the queue state from underlying storage.
    if(consumer.getQueueState() == null) {
      queueState = dequeueStrategy.readQueueState(consumer, consumer.getQueueConfig(), readPointer);
    } else {
      if(! (consumer.getQueueState() instanceof QueueStateImpl)) {
        throw new OperationException(StatusCode.INTERNAL_ERROR,
              getLogMessage(String.format(
                "Don't know how to use QueueState class %s", consumer.getQueueState().getClass())));
      }
      queueState = (QueueStateImpl) consumer.getQueueState();
    }
    consumer.setQueueState(queueState);
    return queueState;
  }

  protected byte[] makeRowName(byte[] bytesToAppendToQueueName) {
    return Bytes.add(this.queueName, bytesToAppendToQueueName);
  }

  protected byte[] makeRowKey(byte[] bytesToAppendToQueueName, long id1) {
    return Bytes.add(this.queueName, bytesToAppendToQueueName, Bytes.toBytes(id1));
  }

  protected byte[] makeRowKey(byte[] bytesToAppendToQueueName, long id1, int id2) {
    return Bytes.add(
      Bytes.add(this.queueName, bytesToAppendToQueueName, Bytes.toBytes(id1)), Bytes.toBytes(id2));
  }

  protected static byte[] makeColumnName(byte[] bytesToPrependToId, long id) {
    return Bytes.add(bytesToPrependToId, Bytes.toBytes(id));
  }

  protected static byte[] makeColumnName(byte[] bytesToPrependToId, String id) {
    return Bytes.add(bytesToPrependToId, Bytes.toBytes(id));
  }

  protected static Long removePrefixFromLongId(byte[] bytes, byte[] prefix) {
    if(Bytes.startsWith(bytes, prefix) && (bytes.length >= Bytes.SIZEOF_LONG + prefix.length)) {
      return Bytes.toLong(bytes, prefix.length);
    }
    return null;
  }

  protected String getLogMessage(String message) {
    return String.format("Queue-%s: %s", Bytes.toString(queueName), message);
  }

  static class DequeueEntry implements Comparable<DequeueEntry> {
    private final long entryId;
    // tries is used to keep track of the number of times the consumer crashed when processing an entry.
    // A consumer is considered to have crashed every time it loses its cached state information,
    // and the state has to be read from underlying storage.
    private int tries;

    DequeueEntry(long entryId) {
      this.entryId = entryId;
      this.tries = 1;
    }

    DequeueEntry(long entryId, int tries) {
      this.entryId = entryId;
      this.tries = tries;
    }

    public long getEntryId() {
      return entryId;
    }

    public int getTries() {
      return tries;
    }

    public void incrementTries() {
      ++tries;
    }

    @Override
    public int compareTo(DequeueEntry dequeueEntry) {
      if(this.entryId > dequeueEntry.entryId) {
        return 1;
      }
      if(this.entryId < dequeueEntry.entryId) {
        return -1;
      }
      return 0;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DequeueEntry that = (DequeueEntry) o;

      if (entryId != that.entryId) return false;

      return true;
    }

    @Override
    public int hashCode() {
      return (int) (entryId ^ (entryId >>> 32));
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("entryId", entryId)
        .add("tries", tries)
        .toString();
    }

    public void encode(Encoder encoder) throws IOException {
      encoder.writeLong(entryId)
        .writeInt(tries);
    }

    public static DequeueEntry decode(Decoder decoder) throws IOException {
      long entryId = decoder.readLong();
      int tries = decoder.readInt();
      return new DequeueEntry(entryId, tries);
    }
  }

  static class DequeuedEntrySet {
    private final SortedSet<DequeueEntry> entrySet;

    DequeuedEntrySet() {
      this.entrySet = new TreeSet<DequeueEntry>();
    }

    DequeuedEntrySet(SortedSet<DequeueEntry> entrySet) {
      this.entrySet = entrySet;
    }

    public DequeueEntry min() {
      return entrySet.first();
    }

    public DequeueEntry max() {
      return entrySet.last();
    }

    public boolean isEmpty() {
      return entrySet.isEmpty();
    }

    public int size() {
      return entrySet.size();
    }

    public boolean add(DequeueEntry dequeueEntry) {
      return entrySet.add(dequeueEntry);
    }

    public boolean remove(long entryId) {
      return entrySet.remove(new DequeueEntry(entryId));
    }

    public boolean contains(long entryId) {
      return entrySet.contains(new DequeueEntry(entryId));
    }

    public List<Long> getEntryIds() {
      List<Long> entryIds = Lists.newArrayListWithCapacity(entrySet.size());
      for(DequeueEntry entry : entrySet) {
        entryIds.add(entry.getEntryId());
      }
      return entryIds;
    }

    public List<DequeueEntry> getEntryList() {
      return Lists.newArrayList(entrySet);
    }

    public SortedSet<Long> startNewTry(final long maxCrashDequeueTries) {
      SortedSet<Long> droppedEntries = new TreeSet<Long>();
      // TODO: Right now we increment tries and remove all dequeueed entries no matter which entry caused the crash.
      for(Iterator<DequeueEntry> it = entrySet.iterator(); it.hasNext();) {
        DequeueEntry entry = it.next();
        entry.incrementTries();
        // If MAX_CRASH_DEQUEUE_TRIES reached for entry then drop it
        if(entry.getTries() > maxCrashDequeueTries) {
          droppedEntries.add(entry.getEntryId());
          it.remove();
        }
      }
      return droppedEntries;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("entrySet", entrySet)
        .toString();
    }

    public void encode(Encoder encoder) throws IOException {
      if(!entrySet.isEmpty()) {
        encoder.writeInt(entrySet.size());
        for(DequeueEntry entry : entrySet) {
          entry.encode(encoder);
        }
      }
      encoder.writeInt(0); // zero denotes end of list as per AVRO spec
    }

    public static DequeuedEntrySet decode(Decoder decoder) throws IOException {
      int size = decoder.readInt();
      // TODO: return empty set if size == 0
      DequeuedEntrySet dequeuedEntrySet = new DequeuedEntrySet();
      while(size > 0) {
        for(int i = 0; i < size; ++i) {
          dequeuedEntrySet.add(DequeueEntry.decode(decoder));
        }
        size = decoder.readInt();
      }
      return dequeuedEntrySet;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DequeuedEntrySet that = (DequeuedEntrySet) o;

      if (entrySet != null ? !entrySet.equals(that.entrySet) : that.entrySet != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      return entrySet != null ? entrySet.hashCode() : 0;
    }
  }

  public static class TransientWorkingSet {
    private final List<DequeueEntry> entryList;
    private int curPtr = -1;
    private final Map<Long, byte[]> cachedEntries;

    private static final TransientWorkingSet EMPTY_SET =
      new TransientWorkingSet(Collections.<Long>emptyList(), Collections.<Long, byte[]>emptyMap());

    public static TransientWorkingSet emptySet() {
      return EMPTY_SET;
    }

    public TransientWorkingSet(List<Long> entryIds, Map<Long, byte[]> cachedEntries) {
      List<DequeueEntry> entries = Lists.newArrayListWithCapacity(entryIds.size());
      for(long id : entryIds) {
        if(!cachedEntries.containsKey(id)) {
          // TODO: catch Runtime exception and translate it into OperationException
          throw new IllegalArgumentException(String.format("Cached entries does not contain entry %d", id));
        }
        entries.add(new DequeueEntry(id, 0));
      }
      this.entryList = entries;
      this.cachedEntries = cachedEntries;
    }

    public TransientWorkingSet(List<DequeueEntry> entryList, int curPtr, Map<Long, byte[]> cachedEntries) {
      this.entryList = entryList;
      this.curPtr = curPtr;
      this.cachedEntries = cachedEntries;
    }

    public boolean hasNext() {
      return curPtr + 1 < entryList.size();
    }

    public DequeueEntry next() {
      return fetch(++curPtr);
    }

    public DequeueEntry peekNext() {
      return fetch(curPtr + 1);
    }

    private DequeueEntry fetch(int ptr) {
      if(ptr >= entryList.size()) {
        throw new IllegalArgumentException(String.format(
          "%d exceeds bounds of claimed entries %d", ptr, entryList.size()));
      }
      return entryList.get(ptr);
    }

    public Map<Long, byte[]> getCachedEntries() {
      return cachedEntries;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("entryList", entryList)
        .add("curPtr", curPtr)
        .add("cachedEntries", cachedEntries)
        .toString();
    }

    public void encode(Encoder encoder) throws IOException {
      if(!entryList.isEmpty()) {
        encoder.writeInt(entryList.size());
        for(DequeueEntry entry : entryList) {
          entry.encode(encoder);
        }
      }
      encoder.writeInt(0); // zero denotes end of list as per AVRO spec

      encoder.writeLong(curPtr);

      if(!cachedEntries.isEmpty()) {
        encoder.writeInt(cachedEntries.size());
        for(Map.Entry<Long, byte[]> entry : cachedEntries.entrySet()) {
          encoder.writeLong(entry.getKey());
          encoder.writeBytes(entry.getValue());
        }
      }
      encoder.writeInt(0); // zero denotes end of map as per AVRO spec
    }

    public static TransientWorkingSet decode(Decoder decoder) throws IOException {
      int size = decoder.readInt();
      // TODO: return empty set if size == 0
      List<DequeueEntry> entries = Lists.newArrayListWithExpectedSize(size);
      while(size > 0) {
        for(int i = 0; i < size; ++i) {
          entries.add(DequeueEntry.decode(decoder));
        }
        size = decoder.readInt();
      }

      int curPtr = decoder.readInt();

      int mapSize = decoder.readInt();
      Map<Long, byte[]> cachedEntries = Maps.newHashMapWithExpectedSize(mapSize);
      while(mapSize > 0) {
        for(int i= 0; i < mapSize; ++i) {
          cachedEntries.put(decoder.readLong(), decoder.readBytes().array());
        }
        mapSize = decoder.readInt();
      }
      return new TransientWorkingSet(entries, curPtr, cachedEntries);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TransientWorkingSet that = (TransientWorkingSet) o;

      if (curPtr != that.curPtr) {
        return false;
      }
      if (cachedEntries != null ?
        !cachedEntriesEquals(cachedEntries, that.cachedEntries) :
        that.cachedEntries != null) {
        return false;
      }
      if (entryList != null ? !entryList.equals(that.entryList) : that.entryList != null) {
        return false;
      }

      return true;
    }

    private static boolean cachedEntriesEquals(Map<Long, byte[]> cachedEntries1, Map<Long, byte[]> cachedEntries2) {
      if(cachedEntries1.size() != cachedEntries2.size()) {
        return false;
      }
      for(Map.Entry<Long, byte[]> entry : cachedEntries1.entrySet()) {
        byte[] bytes2 = cachedEntries2.get(entry.getKey());
        if(entry.getValue() == null || bytes2 == null) {
          if(entry.getValue() != bytes2) {
            return false;
          } else {
            if(!Bytes.equals(entry.getValue(), bytes2)) {
              return false;
            }
          }
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      int result = entryList != null ? entryList.hashCode() : 0;
      result = 31 * result + curPtr;
      result = 31 * result + (cachedEntries != null ? cachedEntries.hashCode() : 0);
      return result;
    }
  }

  static class ReconfigPartitionInstance {
    private final int instanceId;
    private final long maxAckEntryId;

    ReconfigPartitionInstance(int instanceId, long maxAckEntryId) {
      this.instanceId = instanceId;
      this.maxAckEntryId = maxAckEntryId;
    }

    public long getMaxAckEntryId() {
      return maxAckEntryId;
    }

    public int getInstanceId() {
      return instanceId;
    }

    public boolean isRedundant(long minAckEntryId) {
      return minAckEntryId > maxAckEntryId ? true : false;
    }

    public void encode(Encoder encoder) throws IOException {
      encoder.writeInt(instanceId)
        .writeLong(maxAckEntryId);
    }

    public static ReconfigPartitionInstance decode(Decoder decoder) throws IOException {
      int instanceId = decoder.readInt();
      long maxAckEntryId = decoder.readLong();
      return new ReconfigPartitionInstance(instanceId, maxAckEntryId);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ReconfigPartitionInstance that = (ReconfigPartitionInstance) o;

      if (instanceId != that.instanceId) return false;
      if (maxAckEntryId != that.maxAckEntryId) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = instanceId;
      result = 31 * result + (int) (maxAckEntryId ^ (maxAckEntryId >>> 32));
      return result;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("instanceId", instanceId)
        .add("maxAckEntryId", maxAckEntryId)
        .toString();
    }
  }

  static class ReconfigPartitioner implements QueuePartitioner {
    private final int groupSize;
    private final QueuePartitioner.PartitionerType partitionerType;
    private final List<ReconfigPartitionInstance> reconfigPartitionInstances;

    private static final ReconfigPartitioner EMPTY_RECONFIG_PARTITIONER = new ReconfigPartitioner();
    public static ReconfigPartitioner getEmptyReconfigPartitioner() {
      return EMPTY_RECONFIG_PARTITIONER;
    }

    // TODO: convert this into a Builder
    private ReconfigPartitioner() {
      groupSize = 0;
      partitionerType = PartitionerType.FIFO; // Doesn't matter what partition type
      reconfigPartitionInstances = Collections.emptyList();
    }

    public ReconfigPartitioner(int groupSize, PartitionerType partitionerType) {
      this.groupSize = groupSize;
      this.partitionerType = partitionerType;
      this.reconfigPartitionInstances = Lists.newArrayListWithCapacity(groupSize);
    }

    public void add(int consumerId, long maxAckEntryId) {
      add(new ReconfigPartitionInstance(consumerId, maxAckEntryId));
    }

    private void add(ReconfigPartitionInstance info) {
      if(reconfigPartitionInstances.size() >= groupSize) {
        throw new IllegalArgumentException(String.format("GroupSize %d exceeded", reconfigPartitionInstances.size()));
      }
      reconfigPartitionInstances.add(info);
    }

    public int getGroupSize() {
      return groupSize;
    }

    public PartitionerType getPartitionerType() {
      return partitionerType;
    }

    public List<ReconfigPartitionInstance> getReconfigPartitionInstances() {
      return reconfigPartitionInstances;
    }

    // TODO: remove isDisjoint and usesHeaderData methods from QueuePartitioner interface
    @Override
    public boolean isDisjoint() {
      return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean usesHeaderData() {
      return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean shouldEmit(int groupSize, int instanceId, long entryId, byte[] value) {
      QueuePartitioner partitioner = partitionerType.getPartitioner();
      for(ReconfigPartitionInstance reconfigInfo : reconfigPartitionInstances) {
        // Ignore passed in groupSize and instanceId since we are partitioning using old information
        // No consumer with reconfigPartitioner.getInstanceId() should have already acked entryId
        if(entryId <= reconfigInfo.getMaxAckEntryId() &&
          partitioner.shouldEmit(this.groupSize, reconfigInfo.getInstanceId(), entryId, value)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean shouldEmit(int groupSize, int instanceId, long entryId, int hash) {
      QueuePartitioner partitioner = partitionerType.getPartitioner();
      for(ReconfigPartitionInstance reconfigInfo : reconfigPartitionInstances) {
        // Ignore passed in groupSize and instanceId since we are partitioning using old information
        // No consumer with reconfigPartitioner.getInstanceId() should have already acked entryId
        if(entryId <= reconfigInfo.getMaxAckEntryId() &&
          partitioner.shouldEmit(this.groupSize, reconfigInfo.getInstanceId(), entryId, hash)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean shouldEmit(int groupSize, int instanceId, long entryId) {
      QueuePartitioner partitioner = partitionerType.getPartitioner();
      for(ReconfigPartitionInstance reconfigInfo : reconfigPartitionInstances) {
        // Ignore passed in groupSize and instanceId since we are partitioning using old information
        // No consumer with reconfigInfo.getInstanceId() should have already acked entryId
        if(entryId <= reconfigInfo.getMaxAckEntryId() &&
          partitioner.shouldEmit(this.groupSize, reconfigInfo.getInstanceId(), entryId)) {
          return false;
        }
      }
      return true;
    }

    public boolean isRedundant(long minAckEntryId) {
      boolean allRedundant = true;
      for(ReconfigPartitionInstance instance : reconfigPartitionInstances) {
        if(!instance.isRedundant(minAckEntryId)) {
          allRedundant = false;
        }
      }
      return allRedundant;
    }

    public void encode(Encoder encoder) throws IOException {
      if(groupSize != reconfigPartitionInstances.size()) {
        throw new IllegalStateException(String.format(
          "Groupsize: %d is not equal to partition information objects %d", groupSize, reconfigPartitionInstances.size()));
      }

      // TODO: use common code to decode/encode lists
      if(!reconfigPartitionInstances.isEmpty()) {
        // Note: we are not writing reconfigPartitionInstances.size() again, we use groupSize as the size of the list
        encoder.writeInt(groupSize)
          .writeString(partitionerType.name());
        for(ReconfigPartitionInstance info : reconfigPartitionInstances) {
          info.encode(encoder);
        }
      }
      encoder.writeInt(0); // zero denotes end of list as per AVRO spec
    }

    public static ReconfigPartitioner decode(Decoder decoder) throws IOException {
      // TODO: use common code to decode/encode lists
      // Note: we are not reading reconfigPartitionInstances.size() again, we use groupSize as the size of the list
      int groupSize = decoder.readInt();
      if(groupSize == 0) {
        return ReconfigPartitioner.getEmptyReconfigPartitioner();
      }

      PartitionerType partitionerType = PartitionerType.valueOf(decoder.readString());
      ReconfigPartitioner partitioner = new ReconfigPartitioner(groupSize, partitionerType);
      int size = groupSize;
      while(size > 0) {
        for(int i = 0; i < size; ++i) {
          partitioner.add(ReconfigPartitionInstance.decode(decoder));
        }
        size = decoder.readInt();
      }

      if(groupSize != partitioner.reconfigPartitionInstances.size()) {
        throw new IllegalStateException(String.format(
          "Groupsize: %d is not equal to partition information objects %d", groupSize,
          partitioner.reconfigPartitionInstances.size()));
      }
      return partitioner;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ReconfigPartitioner that = (ReconfigPartitioner) o;

      if (groupSize != that.groupSize) return false;
      if (partitionerType != that.partitionerType) return false;
      if (!reconfigPartitionInstances.equals(that.reconfigPartitionInstances)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = groupSize;
      result = 31 * result + partitionerType.hashCode();
      result = 31 * result + reconfigPartitionInstances.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("groupSize", groupSize)
        .add("partitionerType", partitionerType)
        .add("reconfigPartitionInstances", reconfigPartitionInstances)
        .toString();
    }
  }

  public static class ReconfigPartitionersList implements QueuePartitioner {
    private final List<ReconfigPartitioner> reconfigPartitioners;

    private static final ReconfigPartitionersList EMPTY_RECONFIGURATION_PARTITIONERS_LIST =
      new ReconfigPartitionersList(Collections.EMPTY_LIST);

    public static ReconfigPartitionersList getEmptyList() {
      return EMPTY_RECONFIGURATION_PARTITIONERS_LIST;
    }

    public ReconfigPartitionersList(List<ReconfigPartitioner> reconfigPartitioners) {
      this.reconfigPartitioners = Lists.newLinkedList(reconfigPartitioners);
    }

    public List<ReconfigPartitioner> getReconfigPartitioners() {
      return reconfigPartitioners;
    }

    // TODO: remove isDisjoint and usesHeaderData methods from QueuePartitioner interface
    @Override
    public boolean isDisjoint() {
      return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean usesHeaderData() {
      return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean shouldEmit(int groupSize, int instanceId, long entryId, byte[] value) {
      // Return false if the entry has been acknowledged by any of the previous partitions
      for(ReconfigPartitioner partitioner : reconfigPartitioners) {
        if(!partitioner.shouldEmit(groupSize, instanceId, entryId, value)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean shouldEmit(int groupSize, int instanceId, long entryId, int hash) {
      // Return false if the entry has been acknowledged by any of the previous partitions
      for(ReconfigPartitioner partitioner : reconfigPartitioners) {
        if(!partitioner.shouldEmit(groupSize, instanceId, entryId, hash)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean shouldEmit(int groupSize, int instanceId, long entryId) {
      // Return false if the entry has been acknowledged by any of the previous partitions
      for(ReconfigPartitioner partitioner : reconfigPartitioners) {
        if(!partitioner.shouldEmit(groupSize, instanceId, entryId)) {
          return false;
        }
      }
      return true;
    }

    public void compact(long minAckEntry) {
      List<ReconfigPartitioner> redundantPartitioners = Lists.newLinkedList();
      for(ReconfigPartitioner reconfigPartitioner : reconfigPartitioners) {
        if(reconfigPartitioner.isRedundant(minAckEntry)) {
          redundantPartitioners.add(reconfigPartitioner);
        }
      }
      reconfigPartitioners.removeAll(redundantPartitioners);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("reconfigPartitioners", reconfigPartitioners)
        .toString();
    }

    public void encode(Encoder encoder) throws IOException {
      // TODO: use common code to decode/encode lists
      if(!reconfigPartitioners.isEmpty()) {
        encoder.writeInt(reconfigPartitioners.size());
        for(ReconfigPartitioner partitioner : reconfigPartitioners) {
          partitioner.encode(encoder);
        }
      }
      encoder.writeInt(0); // zero denotes end of list as per AVRO spec
    }

    public static ReconfigPartitionersList decode(Decoder decoder) throws IOException {
      int size = decoder.readInt();
      if(size == 0) {
        return ReconfigPartitionersList.getEmptyList();
      }

      List<ReconfigPartitioner> reconfigPartitioners = Lists.newArrayList();
      while(size > 0) {
        for(int i = 0; i < size; ++i) {
          reconfigPartitioners.add(ReconfigPartitioner.decode(decoder));
        }
        size = decoder.readInt();
      }
      return new ReconfigPartitionersList(reconfigPartitioners);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ReconfigPartitionersList that = (ReconfigPartitionersList) o;

      if (reconfigPartitioners != null ? !reconfigPartitioners.equals(that.reconfigPartitioners) : that.reconfigPartitioners != null)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      return reconfigPartitioners != null ? reconfigPartitioners.hashCode() : 0;
    }
  }

  static class ClaimedEntryList implements Comparable<ClaimedEntryList> {
    private ClaimedEntry current;
    private List<ClaimedEntry> otherClaimedEntries;

    public ClaimedEntryList() {
      this.current = ClaimedEntry.getInvalidClaimedEntry();
      this.otherClaimedEntries = Lists.newLinkedList();
    }

    public ClaimedEntryList(ClaimedEntry claimedEntry, List<ClaimedEntry> otherClaimedEntries) {
      otherClaimedEntries.remove(ClaimedEntry.INVALID_CLAIMED_ENTRY);
      if(claimedEntry.isValid()) {
        this.current = claimedEntry;
      } else if(!otherClaimedEntries.isEmpty()) {
        this.current = otherClaimedEntries.get(0);
      } else {
        this.current = ClaimedEntry.INVALID_CLAIMED_ENTRY;
      }
      this.otherClaimedEntries = otherClaimedEntries;
    }

    public void add(long begin, long end) {
      ClaimedEntry newClaimedEntry = new ClaimedEntry(begin, end);
      if(!newClaimedEntry.isValid()) {
        return;
      }
      makeCurrentValid();
      if(!current.isValid()) {
        current = newClaimedEntry;
      } else {
        otherClaimedEntries.add(newClaimedEntry);
      }
    }

    public void addAll(ClaimedEntryList claimedEntryList) {
      ClaimedEntry otherCurrent = claimedEntryList.getClaimedEntry();
      add(otherCurrent.getBegin(), otherCurrent.getEnd());

      claimedEntryList.otherClaimedEntries.remove(ClaimedEntry.INVALID_CLAIMED_ENTRY);
      otherClaimedEntries.addAll(claimedEntryList.otherClaimedEntries);
    }

    public void moveForwardTo(long entryId) {
      if(entryId < current.getBegin()) {
        throw new IllegalArgumentException(String.format
          ("entryId (%d) shoudl not be less than begin (%d)", entryId, current.getBegin()));
      }
      current = current.move(entryId);
      makeCurrentValid();
    }

    private void makeCurrentValid() {
      while(!current.isValid() && !otherClaimedEntries.isEmpty()) {
        current = otherClaimedEntries.remove(0);
      }
    }

    public ClaimedEntry getClaimedEntry() {
      makeCurrentValid();
      return current;
    }

    public int size() {
      // TODO: use the claimed entry range to determine size
      return (current.isValid() ? 1 : 0) + otherClaimedEntries.size();
    }

    @Override
    public int compareTo(ClaimedEntryList claimedEntryList) {
      if(this.size() > claimedEntryList.size()) {
        return 1;
      }
      if(this.size() < claimedEntryList.size()) {
        return -1;
      }
      return 0;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("current", current)
        .add("otherClaimedEntries", otherClaimedEntries)
        .toString();
    }

    public void encode(Encoder encoder) throws IOException {
      // TODO: use common code to decode/encode lists
      current.encode(encoder);
      if(!otherClaimedEntries.isEmpty()) {
        encoder.writeInt(otherClaimedEntries.size());
        for(ClaimedEntry claimedEntry : otherClaimedEntries) {
          claimedEntry.encode(encoder);
        }
      }
      encoder.writeInt(0); // zero denotes end of list as per AVRO spec
    }

    public static ClaimedEntryList decode(Decoder decoder) throws IOException {
      ClaimedEntry current = ClaimedEntry.decode(decoder);

      int size = decoder.readInt();
      List<ClaimedEntry> otherClaimedEntries = Lists.newLinkedList();

      while(size > 0) {
        for(int i = 0; i < size; ++i) {
          otherClaimedEntries.add(ClaimedEntry.decode(decoder));
        }
        size = decoder.readInt();
      }
      return new ClaimedEntryList(current, otherClaimedEntries);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ClaimedEntryList that = (ClaimedEntryList) o;

      if (!current.equals(that.current)) return false;
      if (!otherClaimedEntries.equals(that.otherClaimedEntries)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = current.hashCode();
      result = 31 * result + otherClaimedEntries.hashCode();
      return result;
    }
  }

  static class ClaimedEntry {
    private final long begin;
    private final long end;

    static final ClaimedEntry INVALID_CLAIMED_ENTRY = new ClaimedEntry(INVALID_ENTRY_ID, INVALID_ENTRY_ID);
    public static ClaimedEntry getInvalidClaimedEntry() {
      return INVALID_CLAIMED_ENTRY;
    }

    public ClaimedEntry(long begin, long end) {
      if(begin > end) {
        throw new IllegalArgumentException(String.format("begin (%d) is greater than end (%d)", begin, end));
      } else if((begin == INVALID_ENTRY_ID || end == INVALID_ENTRY_ID) && begin != end) {
        // Both begin and end can be INVALID_ENTRY_ID
        throw new IllegalArgumentException(String.format("Either begin (%d) or end (%d) is invalid", begin, end));
      }
      this.begin = begin;
      this.end = end;
    }

    public long getBegin() {
      return begin;
    }

    public long getEnd() {
      return end;
    }

    public ClaimedEntry move(long entryId) {
      if(!isValid()) {
        return this;
      }
      if(entryId > end) {
        return getInvalidClaimedEntry();
      }
      return new ClaimedEntry(entryId, end);
    }

    public boolean isValid() {
      return begin != INVALID_ENTRY_ID;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("begin", begin)
        .add("end", end)
        .toString();
    }

    public void encode(Encoder encoder) throws IOException {
      encoder.writeLong(begin);
      encoder.writeLong(end);
    }

    public static ClaimedEntry decode(Decoder decoder) throws IOException {
      return new ClaimedEntry(decoder.readLong(), decoder.readLong());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ClaimedEntry that = (ClaimedEntry) o;

      if (begin != that.begin) return false;
      if (end != that.end) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = (int) (begin ^ (begin >>> 32));
      result = 31 * result + (int) (end ^ (end >>> 32));
      return result;
    }
  }

  public static class QueueStateImpl implements QueueState {
    // Note: QueueStateImpl does not override equals and hashcode,
    // since in some cases we would want to include transient state in comparison and in some other cases not.

    private TransientWorkingSet transientWorkingSet = TransientWorkingSet.emptySet();
    private DequeuedEntrySet dequeueEntrySet;
    private long consumerReadPointer = INVALID_ENTRY_ID;
    private long queueWritePointer = FIRST_QUEUE_ENTRY_ID - 1;
    private ClaimedEntryList claimedEntryList;
    private long lastEvictTimeInSecs = 0;

    private ReconfigPartitionersList reconfigPartitionersList = ReconfigPartitionersList.getEmptyList();

    public QueueStateImpl() {
      dequeueEntrySet = new DequeuedEntrySet();
      claimedEntryList = new ClaimedEntryList();
    }

    public TransientWorkingSet getTransientWorkingSet() {
      return transientWorkingSet;
    }

    public void setTransientWorkingSet(TransientWorkingSet transientWorkingSet) {
      this.transientWorkingSet = transientWorkingSet;
    }

    public DequeuedEntrySet getDequeueEntrySet() {
      return dequeueEntrySet;
    }

    public void setDequeueEntrySet(DequeuedEntrySet dequeueEntrySet) {
      this.dequeueEntrySet = dequeueEntrySet;
    }

    public long getConsumerReadPointer() {
      return consumerReadPointer;
    }

    public void setConsumerReadPointer(long consumerReadPointer) {
      this.consumerReadPointer = consumerReadPointer;
    }

    public ClaimedEntryList getClaimedEntryList() {
      return claimedEntryList;
    }

    public void setClaimedEntryList(ClaimedEntryList claimedEntryList) {
      this.claimedEntryList = claimedEntryList;
    }

    public long getQueueWritePointer() {
      return queueWritePointer;
    }

    public long getLastEvictTimeInSecs() {
      return lastEvictTimeInSecs;
    }

    public void setLastEvictTimeInSecs(long lastEvictTimeInSecs) {
      this.lastEvictTimeInSecs = lastEvictTimeInSecs;
    }

    public void setQueueWritePointer(long queueWritePointer) {
      this.queueWritePointer = queueWritePointer;
    }

    public ReconfigPartitionersList getReconfigPartitionersList() {
      return reconfigPartitionersList;
    }

    public void setReconfigPartitionersList(ReconfigPartitionersList reconfigPartitionersList) {
      this.reconfigPartitionersList = reconfigPartitionersList;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("transientWorkingSet", transientWorkingSet)
        .add("dequeueEntrySet", dequeueEntrySet)
        .add("consumerReadPointer", consumerReadPointer)
        .add("claimedEntryList", claimedEntryList)
        .add("queueWritePointer", queueWritePointer)
        .add("lastEvictTimeInSecs", lastEvictTimeInSecs)
        .add("reconfigPartitionersList", reconfigPartitionersList)
        .toString();
    }

    public void encode(Encoder encoder) throws IOException {
      // TODO: write and read schema
      dequeueEntrySet.encode(encoder);
      encoder.writeLong(consumerReadPointer);
      claimedEntryList.encode(encoder);
      encoder.writeLong(queueWritePointer);
      encoder.writeLong(lastEvictTimeInSecs);
      reconfigPartitionersList.encode(encoder);
    }

    public void encodeTransient(Encoder encoder) throws IOException {
      encode(encoder);
      transientWorkingSet.encode(encoder);
    }

    public static QueueStateImpl decode(Decoder decoder) throws IOException {
      QueueStateImpl queueState = new QueueStateImpl();
      queueState.setDequeueEntrySet(DequeuedEntrySet.decode(decoder));
      queueState.setConsumerReadPointer(decoder.readLong());
      queueState.setClaimedEntryList(ClaimedEntryList.decode(decoder));
      queueState.setQueueWritePointer(decoder.readLong());
      queueState.setLastEvictTimeInSecs(decoder.readLong());
      queueState.setReconfigPartitionersList(ReconfigPartitionersList.decode(decoder));
      return queueState;
    }

    public static QueueStateImpl decodeTransient(Decoder decoder) throws IOException {
      QueueStateImpl queueState = QueueStateImpl.decode(decoder);
      queueState.setTransientWorkingSet(TransientWorkingSet.decode(decoder));
      return queueState;
    }
  }

  private static class QueueStateStore {
    private final VersionedColumnarTable table;
    private final TransactionOracle oracle;
    private byte[] rowKey;
    private final List<byte[]> columnNames = Lists.newArrayList();
    private final List<byte[]> columnValues = Lists.newArrayList();

    private OperationResult<Map<byte[], byte[]>> readResult;

    private QueueStateStore(VersionedColumnarTable table, TransactionOracle oracle) {
      this.table = table;
      this.oracle = oracle;
    }

    public byte[] getRowKey() {
      return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
      this.rowKey = rowKey;
    }

    public void addColumnName(byte[] columnName) {
      columnNames.add(columnName);
    }

    public void addColumnValue(byte[] columnValue) {
      columnValues.add(columnValue);
    }

    public void read()
      throws OperationException{
      final byte[][] colNamesByteArray = new byte[columnNames.size()][];
      readResult = table.get(rowKey, columnNames.toArray(colNamesByteArray), TransactionOracle.DIRTY_READ_POINTER);
      clearParameters();
    }

    public OperationResult<Map<byte[], byte[]>> getReadResult() {
      return this.readResult;
    }

    public void write()
      throws OperationException {
      final byte[][] colNamesByteArray = new byte[columnNames.size()][];
      final byte[][] colValuesByteArray = new byte[columnValues.size()][];
      table.put(rowKey, columnNames.toArray(colNamesByteArray),
                TransactionOracle.DIRTY_WRITE_VERSION, columnValues.toArray(colValuesByteArray));
      clearParameters();
    }

    public void clearParameters() {
      rowKey = null;
      columnNames.clear();
      columnValues.clear();
    }
  }

  interface DequeueStrategy {
    QueueStateImpl readQueueState(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
      throws OperationException;
    QueueStateImpl constructQueueState(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
      throws OperationException;
    List<Long> fetchNextEntries(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                          ReadPointer readPointer) throws OperationException;
    void saveDequeueState(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                          ReadPointer readPointer) throws OperationException;

    /**
     *
     * @param consumers List of current consumers. Needs to be sorted on instanceId
     * @param queueStates
     * @param config
     * @param groupId
     * @param currentConsumerCount
     * @param newConsumerCount
     * @param readPointer
     * @throws OperationException
     */
    void configure(List<QueueConsumer> consumers, List<QueueStateImpl> queueStates, QueueConfig config,
                   long groupId, int currentConsumerCount, int newConsumerCount, ReadPointer readPointer)
      throws OperationException;

    void deleteDequeueState(QueueConsumer consumer) throws OperationException;

  }

  abstract class AbstractDequeueStrategy implements DequeueStrategy {
    protected final QueueStateStore readQueueStateStore = new QueueStateStore(table, oracle);
    protected final QueueStateStore writeQueueStateStore = new QueueStateStore(table, oracle);
    protected SortedSet<Long> droppedEntries = ImmutableSortedSet.of();

    /**
     * This function is used to initialize the read pointer when a consumer first runs.
     * Initial value for the read pointer is max(lastEvictEntry, FIRST_QUEUE_ENTRY_ID - 1)
     * @return read pointer initial value
     * @throws OperationException
     */
    protected long getReadPointerIntialValue() throws OperationException {
      final long defaultInitialValue = FIRST_QUEUE_ENTRY_ID - 1;
        long lastEvictEntry = getLastEvictEntry();
        if(lastEvictEntry != INVALID_ENTRY_ID && lastEvictEntry > defaultInitialValue) {
          return lastEvictEntry;
        }
      return defaultInitialValue;
    }

    protected long getLastEvictEntry() throws OperationException {
      QueueStateStore readEvictState = new QueueStateStore(table, oracle);
      readEvictState.setRowKey(GLOBAL_LAST_EVICT_ENTRY);
      readEvictState.addColumnName(GLOBAL_LAST_EVICT_ENTRY);
      readEvictState.read();
      OperationResult<Map<byte[], byte[]>> evictStateBytes = readEvictState.getReadResult();

      if(!evictStateBytes.isEmpty()) {
        byte[] lastEvictEntryBytes = evictStateBytes.getValue().get(GLOBAL_LAST_EVICT_ENTRY);
        if(!isNullOrEmpty(lastEvictEntryBytes)) {
          long lastEvictEntry = Bytes.toLong(lastEvictEntryBytes);
          return lastEvictEntry;
        }
      }
      return INVALID_ENTRY_ID;
    }

    @Override
    public QueueStateImpl readQueueState(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
      throws OperationException {
      // Note: QueueConfig.groupSize and QueueConfig.partitioningKey will not be set when calling from configure
      return constructQueueStateInternal(consumer, config, readPointer, false); // TODO: change to false
    }

    @Override
    public QueueStateImpl constructQueueState(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
      throws OperationException {
      // Note: QueueConfig.groupSize and QueueConfig.partitioningKey will not be set when calling from configure
      return constructQueueStateInternal(consumer, config, readPointer, true);
    }

    protected boolean isNullOrEmpty(byte[] bytes) {
      return bytes == null || bytes.length == 0;
    }

    protected QueueStateImpl constructQueueStateInternal(QueueConsumer consumer, QueueConfig config,
                                             ReadPointer readPointer, boolean construct) throws OperationException {
      // TODO: encode/decode the whole QueueStateImpl object using QueueStateImpl.encode/decode
      // Note: 1. QueueConfig.groupSize and QueueConfig.partitioningKey will not be set when calling from configure

      // Note: 2. We define a deleted cell as no bytes or no zero length bytes.
      //          This is to do with limitation of deleteDiry on HBase

      // Note: 3. We define deleted queue state as empty bytes or zero length consumerReadPointerBytes

      readQueueStateStore.setRowKey(makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()));
      readQueueStateStore.addColumnName(DEQUEUE_ENTRY_SET);
      readQueueStateStore.addColumnName(CONSUMER_READ_POINTER);
      readQueueStateStore.addColumnName(LAST_EVICT_TIME_IN_SECS);
      readQueueStateStore.read();

      OperationResult<Map<byte[], byte[]>> stateBytes = readQueueStateStore.getReadResult();
      byte[] consumerReadPointerBytes = null;

      QueueStateImpl queueState = new QueueStateImpl();
      if(!stateBytes.isEmpty()) {
        // Read dequeued entries
        byte[] dequeueEntrySetBytes = stateBytes.getValue().get(DEQUEUE_ENTRY_SET);
        if(!isNullOrEmpty(dequeueEntrySetBytes)) {
          ByteArrayInputStream bin = new ByteArrayInputStream(dequeueEntrySetBytes);
          BinaryDecoder decoder = new BinaryDecoder(bin);
          // TODO: Read and check schema
          try {
            queueState.setDequeueEntrySet(DequeuedEntrySet.decode(decoder));
          } catch (IOException e) {
            throw new OperationException(StatusCode.INTERNAL_ERROR, getLogMessage(
              "Exception while deserializing dequeue entry list"), e);
          }
        }

        // Read consumer read pointer
        consumerReadPointerBytes = stateBytes.getValue().get(CONSUMER_READ_POINTER);
        if(!isNullOrEmpty(consumerReadPointerBytes)) {
          queueState.setConsumerReadPointer(Bytes.toLong(consumerReadPointerBytes));
        }

        // Note: last evict time is read while constructing state, but it is only saved after finalize
        byte[] lastEvictTimeInSecsBytes = stateBytes.getValue().get(LAST_EVICT_TIME_IN_SECS);
        if(!isNullOrEmpty(lastEvictTimeInSecsBytes)) {
          queueState.setLastEvictTimeInSecs(Bytes.toLong(lastEvictTimeInSecsBytes));
        }
      }

      if(!construct && (stateBytes.isEmpty() || isNullOrEmpty(consumerReadPointerBytes))) {
        throw new OperationException(StatusCode.NOT_CONFIGURED, getLogMessage(String.format(
          "Cannot find configuration for consumer %d. Is configure method called?", consumer.getInstanceId())));
      }

      // If read pointer is invalid then this the first time the consumer is running, initialize the read pointer
      if(queueState.getConsumerReadPointer() == INVALID_ENTRY_ID) {
        queueState.setConsumerReadPointer(getReadPointerIntialValue());
      }

      // Read queue write pointer
      // TODO: use raw Get instead of the workaround of incrementing zero
      long queueWritePointer = table.incrementAtomicDirtily(
        makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 0);
      queueState.setQueueWritePointer(queueWritePointer);

      // If dequeue entries present, read them from storage
      // This is the crash recovery case, the consumer has stopped processing before acking the previous dequeues
      if(!queueState.getDequeueEntrySet().isEmpty()) {
        droppedEntries = queueState.getDequeueEntrySet().startNewTry(MAX_CRASH_DEQUEUE_TRIES);
        // TODO: what do we do with the dropped entries?
        if(!droppedEntries.isEmpty() && LOG.isWarnEnabled()) {
          LOG.warn(getLogMessage(String.format("Dropping entries %s after %d tries",
                                               droppedEntries, MAX_CRASH_DEQUEUE_TRIES)));
        }

        // TODO: Do we still need the entries to be in dequeue list?
        // TODO: If not, what happens if the consumer crashes again before the claimed entries are processed?
        // Any previously dequeued entries will now need to be dequeued again
        readEntries(consumer, config, queueState, readPointer, queueState.getDequeueEntrySet().getEntryIds());
      }
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(String.format("Constructed new QueueState - %s", queueState)));
      }
      return queueState;
    }

    @Override
    public void saveDequeueState(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                                 ReadPointer readPointer) throws OperationException {
      // Persist the queue state of this consumer
      writeQueueStateStore.setRowKey(makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()));

      writeQueueStateStore.addColumnName(CONSUMER_READ_POINTER);
      long consumerReadPointer = queueState.getConsumerReadPointer();
      if(!queueState.getDequeueEntrySet().isEmpty()) {
        long maxEntryId = queueState.getDequeueEntrySet().max().getEntryId();
        if(consumerReadPointer < maxEntryId) {
          consumerReadPointer = maxEntryId;
        }
      }
      queueState.setConsumerReadPointer(consumerReadPointer);
      writeQueueStateStore.addColumnValue(Bytes.toBytes(queueState.getConsumerReadPointer()));

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Encoder encoder = new BinaryEncoder(bos);
      // TODO: add schema
      try {
        queueState.getDequeueEntrySet().encode(encoder);
      } catch(IOException e) {
        throw new OperationException(StatusCode.INTERNAL_ERROR,
                                     getLogMessage("Exception while serializing dequeue entry list"), e);
      }
      writeQueueStateStore.addColumnName(DEQUEUE_ENTRY_SET);
      writeQueueStateStore.addColumnValue(bos.toByteArray());


      // Note: last evict time is read while constructing state, but it is only saved after finalize

      writeQueueStateStore.write();
    }

    @Override
    public void deleteDequeueState(QueueConsumer consumer) throws OperationException {
      // Delete queue state for the consumer, see notes in constructQueueStateInternal

      // TODO: make delete automatically detect the columns that needs to be empty
      writeQueueStateStore.setRowKey(makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()));

      writeQueueStateStore.addColumnName(DEQUEUE_ENTRY_SET);
      writeQueueStateStore.addColumnName(CONSUMER_READ_POINTER);
      writeQueueStateStore.addColumnName(LAST_EVICT_TIME_IN_SECS);

      writeQueueStateStore.addColumnValue(Bytes.EMPTY_BYTE_ARRAY);
      writeQueueStateStore.addColumnValue(Bytes.EMPTY_BYTE_ARRAY);
      writeQueueStateStore.addColumnValue(Bytes.EMPTY_BYTE_ARRAY);
      writeQueueStateStore.write();
      // TODO: delete evict information for the consumer
    }
  }

  abstract class AbstractDisjointDequeueStrategy extends AbstractDequeueStrategy implements DequeueStrategy {
    @Override
    protected QueueStateImpl constructQueueStateInternal(QueueConsumer consumer, QueueConfig config,
                                              ReadPointer readPointer, boolean construct) throws OperationException {
      // Note: QueueConfig.groupSize and QueueConfig.partitioningKey will not be set when calling from configure

      // Read reconfig partition information
      readQueueStateStore.addColumnName(RECONFIG_PARTITIONER);

      QueueStateImpl queueState = super.constructQueueStateInternal(consumer, config, readPointer, construct);
      OperationResult<Map<byte[], byte[]>> stateBytes = readQueueStateStore.getReadResult();
      if(!stateBytes.isEmpty()) {
        byte[] configPartitionerBytes = stateBytes.getValue().get(RECONFIG_PARTITIONER);
        if(!isNullOrEmpty(configPartitionerBytes)) {
          try {
            // TODO: Read and check schema
            ReconfigPartitionersList partitioners = ReconfigPartitionersList.decode(new BinaryDecoder(
              new ByteArrayInputStream(configPartitionerBytes)));
            queueState.setReconfigPartitionersList(partitioners);
          } catch (IOException e) {
            throw new OperationException(StatusCode.INTERNAL_ERROR,
                                         getLogMessage("Exception while deserializing reconfig partitioners"), e);
          }
        }
      }
      return queueState;
    }

    @Override
    public void saveDequeueState(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState, ReadPointer readPointer) throws OperationException {
      // Write reconfig partition information
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Encoder encoder = new BinaryEncoder(bos);
      try {
        queueState.getReconfigPartitionersList().encode(encoder);
      } catch(IOException e) {
        throw new OperationException(StatusCode.INTERNAL_ERROR,
                                     getLogMessage("Exception while serializing reconfig partitioners"), e);
      }
      writeQueueStateStore.addColumnName(RECONFIG_PARTITIONER);
      writeQueueStateStore.addColumnValue(bos.toByteArray());
      super.saveDequeueState(consumer, config, queueState, readPointer);
    }

    @Override
    public void deleteDequeueState(QueueConsumer consumer) throws OperationException {
      writeQueueStateStore.addColumnName(RECONFIG_PARTITIONER);
      writeQueueStateStore.addColumnValue(Bytes.EMPTY_BYTE_ARRAY);
      super.deleteDequeueState(consumer);
    }

    @Override
    public void configure(List<QueueConsumer> currentConsumers, List<QueueStateImpl> queueStates, QueueConfig config,
                          final long groupId, final int currentConsumerCount, final int newConsumerCount,
                          ReadPointer readPointer) throws OperationException {
      // Note: the consumers list passed here does not contain QueueConsumer.partitioningKey

      if(currentConsumers.size() != currentConsumerCount) {
        throw new OperationException(
          StatusCode.INTERNAL_ERROR,
          getLogMessage(String.format("Size of passed in consumer list (%d) is not equal to currentConsumerCount (%d)",
                                      currentConsumers.size(), currentConsumerCount)));
      }

      long minAckedEntryId = Long.MAX_VALUE;
      ReconfigPartitioner reconfigPartitioner =
        new ReconfigPartitioner(currentConsumerCount, config.getPartitionerType());
      for(QueueConsumer consumer : currentConsumers) {
        QueueStateImpl queueState = (QueueStateImpl) consumer.getQueueState();
        // Since there are no inflight entries, all entries till and including consumer read pointer are acked
        long ackedEntryId = queueState.getConsumerReadPointer();
        if(ackedEntryId < minAckedEntryId) {
          minAckedEntryId = ackedEntryId;
        }
        reconfigPartitioner.add(consumer.getInstanceId(), ackedEntryId);
      }

      DequeueStrategy dequeueStrategy = getDequeueStrategy(config.getPartitionerType().getPartitioner());

      // Consumer zero is never deleted, so read partitioning information from consumer zero
      ReconfigPartitionersList oldReconfigPartitionerList = queueStates.isEmpty() ?
        ReconfigPartitionersList.getEmptyList() : queueStates.get(0).getReconfigPartitionersList();
      // Run compaction
      if(minAckedEntryId != Long.MAX_VALUE) {
        oldReconfigPartitionerList.compact(minAckedEntryId);
      }

      for(int j = 0; j < newConsumerCount; ++j) {
        QueueConsumer consumer;
        QueueStateImpl queueState;
        if(j < currentConsumerCount) {
          consumer = currentConsumers.get(j);
          queueState = (QueueStateImpl) consumer.getQueueState();
          queueState.setTransientWorkingSet(TransientWorkingSet.emptySet());
        } else {
          // Note: the consumer created here does not contain QueueConsumer.partitioningKey
          consumer = new StatefulQueueConsumer(j, groupId, newConsumerCount, config);
          queueState = dequeueStrategy.constructQueueState(consumer, config, readPointer);
          consumer.setQueueState(queueState);
        }

        if(!currentConsumers.isEmpty()) {
          // Modify queue state for active consumers
          // Add reconfigPartitioner
          queueState.setReconfigPartitionersList(
            new ReconfigPartitionersList(
              Lists.newArrayList(Iterables.concat(
                oldReconfigPartitionerList.getReconfigPartitioners(),
                Collections.singleton(reconfigPartitioner))
              )
            ));

          // Move consumer read pointer to the min acked entry for all consumers
          if(minAckedEntryId != Long.MAX_VALUE) {
            queueState.setConsumerReadPointer(minAckedEntryId);
          }
        }
        // TODO: save queue states for all consumers in a single call
        saveDequeueState(consumer, config, queueState, readPointer);
      }

      // Delete queue state for removed consumers, if any
      for(int j = newConsumerCount; j < currentConsumerCount; ++j) {
        QueueConsumer consumer = currentConsumers.get(j);
        // TODO: save and delete queue states for all consumers in a single call
        deleteDequeueState(consumer);
      }
    }
  }

  class HashDequeueStrategy extends AbstractDisjointDequeueStrategy implements DequeueStrategy {
    @Override
    public List<Long> fetchNextEntries(
      QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState, ReadPointer readPointer)
      throws OperationException {
      long entryId = queueState.getConsumerReadPointer();
      QueuePartitioner partitioner=config.getPartitionerType().getPartitioner();
      List<Long> newEntryIds = new ArrayList<Long>();

      outerLoop:
      while (newEntryIds.isEmpty()) {
        if(entryId >= queueState.getQueueWritePointer()) {
          // Reached the end of queue as per cached QueueWritePointer,
          // read it again to see if there is any progress made by producers
          // TODO: use raw Get instead of the workaround of incrementing zero
          long queueWritePointer = table.incrementAtomicDirtily(
            makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 0);
          queueState.setQueueWritePointer(queueWritePointer);
          if(LOG.isTraceEnabled()) {
            LOG.trace(getLogMessage(String.format("New queueWritePointer = %d", queueWritePointer)));
          }
          // If still no progress, return empty queue
          if(entryId >= queueState.getQueueWritePointer()) {
            return Collections.EMPTY_LIST;
          }
        }

        final long batchSize = getBatchSize(config);
        long startEntryId = entryId + 1;
        long endEntryId =
                startEntryId + (batchSize * consumer.getGroupSize()) < queueState.getQueueWritePointer() ?
                    startEntryId + (batchSize * consumer.getGroupSize()) : queueState.getQueueWritePointer();

        // Read  header data from underlying storage, if any
        final int cacheSize = (int)(endEntryId - startEntryId + 1);
        final String partitioningKey = consumer.getPartitioningKey();
        if(partitioningKey == null || partitioningKey.isEmpty()) {
          throw new OperationException(StatusCode.INTERNAL_ERROR,
            getLogMessage("Using Hash Partitioning with null/empty partitioningKey."));
        }
        final byte [][] rowKeys = new byte[cacheSize][];
        for(int id = 0; id < cacheSize; ++id) {
          rowKeys[id] = makeRowKey(GLOBAL_DATA_PREFIX, startEntryId + id);
        }
        final byte[][] columnKeys = new byte[1][];
        columnKeys[0] = makeColumnName(ENTRY_HEADER, partitioningKey);
        OperationResult<Map<byte[], Map<byte[], byte[]>>> headerResult =
          table.getAllColumns(rowKeys, columnKeys, readPointer);

        // Determine which entries  need to be read from storage
        for(int id = 0; id < cacheSize; ++id) {
          final long currentEntryId = startEntryId + id;
          if (!headerResult.isEmpty()) {
            Map<byte[], Map<byte[], byte[]>> headerValue = headerResult.getValue();
            Map<byte[], byte[]> headerMap = headerValue.get(rowKeys[id]);
            if(headerMap == null) {
              break outerLoop;
            }
            byte[] hashBytes = headerMap.get(columnKeys[0]);
            if(hashBytes == null) {
              break outerLoop;
            }
            int hashValue = Bytes.toInt(hashBytes);
            if(partitioner.shouldEmit(consumer.getGroupSize(), consumer.getInstanceId(), currentEntryId, hashValue) &&
              queueState.getReconfigPartitionersList().shouldEmit(
                consumer.getGroupSize(), consumer.getInstanceId(), currentEntryId, hashValue)
              ) {
              newEntryIds.add(currentEntryId);
            }
          } else {
            // Not able to read header
            break outerLoop;
          }
        }
        entryId = endEntryId;
      }
      return newEntryIds;
    }
  }

  class RoundRobinDequeueStrategy extends AbstractDisjointDequeueStrategy implements DequeueStrategy {
    @Override
    public List<Long> fetchNextEntries(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                                       ReadPointer readPointer) throws OperationException {
      long entryId = queueState.getConsumerReadPointer();
      QueuePartitioner partitioner=config.getPartitionerType().getPartitioner();
      List<Long> newEntryIds = new ArrayList<Long>();

      while (newEntryIds.isEmpty()) {
        if(entryId >= queueState.getQueueWritePointer()) {
          // Reached the end of queue as per cached QueueWritePointer,
          // read it again to see if there is any progress made by producers
          // TODO: use raw Get instead of the workaround of incrementing zero
          long queueWritePointer = table.incrementAtomicDirtily(
            makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 0);
          queueState.setQueueWritePointer(queueWritePointer);
          // If still no progress, return empty queue
          if(entryId >= queueState.getQueueWritePointer()) {
            return Collections.EMPTY_LIST;
          }
        }

        final long batchSize = getBatchSize(config);
        long startEntryId = entryId + 1;
        long endEntryId =
                  startEntryId + (batchSize * consumer.getGroupSize()) < queueState.getQueueWritePointer() ?
                  startEntryId + (batchSize * consumer.getGroupSize()) : queueState.getQueueWritePointer();

        final int cacheSize = (int)(endEntryId - startEntryId + 1);

        // Determine which entries  need to be read from storage
        for(int id = 0; id < cacheSize; ++id) {
          final long currentEntryId = startEntryId + id;
          if(partitioner.shouldEmit(consumer.getGroupSize(), consumer.getInstanceId(), currentEntryId) &&
            queueState.getReconfigPartitionersList()
              .shouldEmit(consumer.getGroupSize(), consumer.getInstanceId(), currentEntryId)
            ) {
            newEntryIds.add(currentEntryId);
          }
        }
        entryId = endEntryId;
      }
      return newEntryIds;
    }
  }

  /**
   *  In FifoDequeueStrategy, the next entries are claimed by a consumer by incrementing a
   *  group-shared counter (group read pointer) atomically.
   *  The entries are claimed in batches, the claimed range is recorded in
   *  CLAIMED_ENTRY_BEGIN and CLAIMED_ENTRY_END columns
   */
  class FifoDequeueStrategy extends AbstractDequeueStrategy implements DequeueStrategy {
    @Override
    protected QueueStateImpl constructQueueStateInternal(QueueConsumer consumer, QueueConfig config,
                                              ReadPointer readPointer, boolean construct) throws OperationException {
      // Note: QueueConfig.groupSize and QueueConfig.partitioningKey will not be set when calling from configure

      // Read CLAIMED_ENTRY_LIST and store it in queueState
      readQueueStateStore.addColumnName(CLAIMED_ENTRY_LIST);

      QueueStateImpl queueState = super.constructQueueStateInternal(consumer, config, readPointer, construct);
      OperationResult<Map<byte[], byte[]>> stateBytes = readQueueStateStore.getReadResult();
      if(!stateBytes.isEmpty()) {
        byte[] claimedEntryListBytes = stateBytes.getValue().get(CLAIMED_ENTRY_LIST);
        if(!isNullOrEmpty(claimedEntryListBytes)) {
          ClaimedEntryList claimedEntryList;
          try {
            // TODO: Read and check schema
            claimedEntryList = ClaimedEntryList.decode(
              new BinaryDecoder(new ByteArrayInputStream(claimedEntryListBytes)));
          } catch (IOException e) {
            throw new OperationException(StatusCode.INTERNAL_ERROR,
                                         getLogMessage("Exception while deserializing CLAIMED_ENTRY_LIST"), e);
          }
          queueState.setClaimedEntryList(claimedEntryList);
        }
      }
      return queueState;
    }

    @Override
    public void saveDequeueState(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                                 ReadPointer readPointer) throws OperationException {
      // We can now move the claimed entry begin pointer to dequeueEntrySet.max() + 1
      if(!queueState.getDequeueEntrySet().isEmpty()) {
        long maxDequeuedEntry = queueState.getDequeueEntrySet().max().getEntryId();
        if(maxDequeuedEntry >= queueState.getClaimedEntryList().getClaimedEntry().getBegin()) {
          queueState.getClaimedEntryList().moveForwardTo(maxDequeuedEntry + 1);
        }
      }

      // Add CLAIMED_ENTRY_LIST writeQueueStateStore so that they can be written
      // to underlying storage by base class saveDequeueState
      writeQueueStateStore.addColumnName(CLAIMED_ENTRY_LIST);
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try {
        queueState.getClaimedEntryList().encode(new BinaryEncoder(bos));
      } catch (IOException e) {
        throw new OperationException(StatusCode.INTERNAL_ERROR,
                                     getLogMessage("Exception while serializing CLAIMED_ENTRY_LIST"), e);
      }
      writeQueueStateStore.addColumnValue(bos.toByteArray());

      super.saveDequeueState(consumer, config, queueState, readPointer);
    }

    @Override
    public void deleteDequeueState(QueueConsumer consumer) throws OperationException {
      writeQueueStateStore.addColumnName(CLAIMED_ENTRY_LIST);
      writeQueueStateStore.addColumnValue(Bytes.EMPTY_BYTE_ARRAY);
      super.deleteDequeueState(consumer);
    }

    /**
     * Returns the group read pointer for the consumer. This also initializes the group read pointer
     * when the consumer group is starting for the first time.
     * @param consumer
     * @return group read pointer
     * @throws OperationException
     */
    private long getGroupReadPointer(QueueConsumer consumer) throws OperationException {
      // Fetch the group read pointer
      final byte[] rowKey = makeRowKey(GROUP_READ_POINTER, consumer.getGroupId());
      // TODO: use raw Get instead of the workaround of incrementing zero
      // TODO: move counters into oracle
      long groupReadPointer = table.incrementAtomicDirtily(rowKey, GROUP_READ_POINTER, 0);

      // If read pointer is zero then this the first time the consumer group is running, initialize the read pointer
      if(groupReadPointer == 0) {
        long lastEvictEntry = getLastEvictEntry();
        if(lastEvictEntry != INVALID_ENTRY_ID) {
          table.compareAndSwapDirty(rowKey, GROUP_READ_POINTER, Bytes.toBytes(groupReadPointer),
                                    Bytes.toBytes(lastEvictEntry));
          // No need to read the group read pointer again, since we are looking for an approximate value anyway.
          return lastEvictEntry;
        }
      }
      return groupReadPointer;
    }

    @Override
    public List<Long> fetchNextEntries(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                                       ReadPointer readPointer) throws OperationException {
      // Determine the next batch of entries that can be dequeued by this consumer
      List<Long> newEntryIds = new ArrayList<Long>();

      // If claimed entries exist, return them. This can happen when the queue cache is lost due to consumer
      // crash or other reasons
      ClaimedEntry claimedEntry = queueState.getClaimedEntryList().getClaimedEntry();
      if(claimedEntry.isValid()) {
        for(long i = claimedEntry.getBegin(); i <= claimedEntry.getEnd(); ++i) {
          newEntryIds.add(i);
        }
        return newEntryIds;
      }

      // Else claim new queue entries
      final long batchSize = getBatchSize(config);
      QueuePartitioner partitioner=config.getPartitionerType().getPartitioner();
      while (newEntryIds.isEmpty()) {
        // Fetch the group read pointer
        long groupReadPointer = getGroupReadPointer(consumer);
        if(groupReadPointer + batchSize >= queueState.getQueueWritePointer()) {
          // Reached the end of queue as per cached QueueWritePointer,
          // read it again to see if there is any progress made by producers
          // TODO: use raw Get instead of the workaround of incrementing zero
          // TODO: move counters into oracle
          long queueWritePointer = table.incrementAtomicDirtily(
            makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 0);
          queueState.setQueueWritePointer(queueWritePointer);
        }

        // End of queue reached
        if(groupReadPointer >= queueState.getQueueWritePointer()) {
          return Collections.EMPTY_LIST;
        }

        // If there are enough entries for all consumers to claim, then claim batchSize entries
        // Otherwise divide the entries equally among all consumers
        long curBatchSize =
          groupReadPointer + (batchSize * consumer.getGroupSize()) < queueState.getQueueWritePointer() ?
          batchSize : (queueState.getQueueWritePointer() - groupReadPointer) / consumer.getGroupSize();
        // Make sure there is progress
        if(curBatchSize < 1) {
          curBatchSize = 1;
        }

        // Claim the entries by incrementing GROUP_READ_POINTER
        long endEntryId = table.incrementAtomicDirtily(makeRowKey(GROUP_READ_POINTER, consumer.getGroupId()),
                                                       GROUP_READ_POINTER, curBatchSize);
        long startEntryId = endEntryId - curBatchSize + 1;
        // Note: incrementing GROUP_READ_POINTER, and storing the claimed entryIds in HBase ideally need to
        // happen atomically. HBase doesn't support atomic increment and put.
        // Also, for performance reasons we have moved the write to method saveDequeueEntryState where
        // all writes for a dequeue happen
        queueState.getClaimedEntryList().add(startEntryId, endEntryId);

        final int cacheSize = (int)(endEntryId - startEntryId + 1);

        // Determine which entries  need to be read from storage based on partition type
        for(int id = 0; id < cacheSize; ++id) {
          final long currentEntryId = startEntryId + id;
          // TODO: No need for partitioner in FIFO
          if(partitioner.shouldEmit(consumer.getGroupSize(), consumer.getInstanceId(), currentEntryId) &&
            queueState.getReconfigPartitionersList().
              shouldEmit(consumer.getGroupSize(), consumer.getInstanceId(), currentEntryId)
            ) {
            newEntryIds.add(currentEntryId);
          }
        }
      }
      return newEntryIds;
    }

    @Override
    public void configure(List<QueueConsumer> currentConsumers, List<QueueStateImpl> queueStates, QueueConfig config, final long groupId, final int currentConsumerCount, final int newConsumerCount, ReadPointer readPointer) throws OperationException {
      if(newConsumerCount >= currentConsumerCount) {
        DequeueStrategy dequeueStrategy = getDequeueStrategy(config.getPartitionerType().getPartitioner());
        for(int i = currentConsumerCount; i < newConsumerCount; ++i) {
          StatefulQueueConsumer consumer = new StatefulQueueConsumer(i, groupId, newConsumerCount, config);
          QueueStateImpl queueState = dequeueStrategy.constructQueueState(consumer, config, readPointer);
          consumer.setQueueState(queueState);
          // TODO: save queue states for all consumers in a single call
          saveDequeueState(consumer, config, queueState, readPointer);
        }
        return;
      }

      if(currentConsumers.size() != currentConsumerCount) {
        throw new OperationException(
          StatusCode.INTERNAL_ERROR,
          getLogMessage(String.format("Size of passed in consumer list (%d) is not equal to currentConsumerCount (%d)", currentConsumers.size(), currentConsumerCount)));
      }

      if(currentConsumers.isEmpty()) {
        // Nothing to do
        return;
      }

      PriorityQueue<ClaimedEntryList> priorityQueue = new PriorityQueue<ClaimedEntryList>(currentConsumerCount);
      for(int i = 0; i < newConsumerCount; ++i) {
        ClaimedEntryList claimedEntryList = queueStates.get(i).getClaimedEntryList();
        priorityQueue.add(claimedEntryList);
      }

      // Transfer the claimed entries of to be removed consumers to other consumers
      for(int i = newConsumerCount; i < currentConsumerCount; ++i) {
        ClaimedEntryList claimedEntryList = queueStates.get(i).getClaimedEntryList();
        ClaimedEntryList transferEntryList = priorityQueue.poll();
        transferEntryList.addAll(claimedEntryList);
        priorityQueue.add(transferEntryList);
      }

      // Save dequeue state of consumers that won't be removed
      for(int i = 0; i < newConsumerCount; ++i) {
        // TODO: save queue states for all consumers in a single call
        saveDequeueState(currentConsumers.get(i), config, queueStates.get(i), readPointer);
      }

      // Delete the state of removed consumers
      for(int i = newConsumerCount; i < currentConsumerCount; ++i) {
        // TODO: save and delete queue states for all consumers in a single call
        deleteDequeueState(currentConsumers.get(i));
      }
      return;
    }
  }
}
