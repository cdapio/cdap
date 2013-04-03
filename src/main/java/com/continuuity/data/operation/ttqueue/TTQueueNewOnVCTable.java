package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.ttqueue.internal.EntryMeta;
import com.continuuity.data.table.VersionedColumnarTable;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

  private final int MAX_CRASH_DEQUEUE_TRIES;

  // For testing
  AtomicLong dequeueReturns = new AtomicLong(0);

  // Row prefix names
  // Row prefix for column GLOBAL_ENTRYID_COUNTER
  static final byte [] GLOBAL_ENTRY_ID_PREFIX = {10, 'I'};  //row <queueName>10I
  // Row prefix for columns containing queue entry
  static final byte [] GLOBAL_DATA_PREFIX = {20, 'D'};   //row <queueName>20D
  // Row prefix for columns containing global eviction data
  static final byte [] GLOBAL_EVICT_META_PREFIX = {30, 'M'};   //row <queueName>30M
  // Row prefix for columns containing consumer specific information
  static final byte [] CONSUMER_META_PREFIX = {40, 'C'}; //row <queueName>40C

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

  // Columns for row = GLOBAL_EVICT_META_PREFIX (Global data, shared by all consumers)
  // GLOBAL_LAST_EVICT_ENTRY contains the entryId of the max evicted entry of the queue.
  // if GLOBAL_LAST_EVICT_ENTRY is not invalid, GLOBAL_LAST_EVICT_ENTRY + 1 points to the first queue entry that can be dequeued.
  static final byte [] GLOBAL_LAST_EVICT_ENTRY = {10, 'L'};   //row  <queueName>30M<groupId>, column 10L
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
  static final byte [] CLAIMED_ENTRY_BEGIN = {30, 'B'};       //row <queueName>40C<groupId><consumerId>, column 30B
  // CLAIMED_ENTRY_END is used by a consumer of FifoDequeueStrategy to specify the end entryId of the batch of entries claimed by it.
  static final byte [] CLAIMED_ENTRY_END = {40, 'E'};         //row <queueName>40C<groupId><consumerId>, column 40E
  // LAST_EVICT_TIME_IN_SECS is the time when the last eviction was run by the consumer
  static final byte [] LAST_EVICT_TIME_IN_SECS = {50, 'T'};           //row <queueName>40C<groupId><consumerId>, column 50T

  static final long INVALID_ENTRY_ID = -1;
  static final long FIRST_QUEUE_ENTRY_ID = 1;

  final long DEFAULT_BATCH_SIZE;
  final long EVICT_INTERVAL_IN_SECS;

  protected TTQueueNewOnVCTable(VersionedColumnarTable table, byte[] queueName, TransactionOracle oracle,
                                final CConfiguration conf) {
    this.table = table;
    this.queueName = queueName;
    this.oracle = oracle;

    final long defaultBatchSize = conf.getLong(TTQUEUE_BATCH_SIZE_DEFAULT, 100);
    this.DEFAULT_BATCH_SIZE = defaultBatchSize > 0 ? defaultBatchSize : 100;

    final long evictIntervalInSecs = conf.getLong(TTQUEUE_EVICT_INTERVAL_SECS, 10 * 60 * 60);
    this.EVICT_INTERVAL_IN_SECS = evictIntervalInSecs >= 0 ? evictIntervalInSecs : 10 * 60 * 60;

    final int maxCrashDequeueTries = conf.getInt(TTQUEUE_MAX_CRASH_DEQUEUE_TRIES, 15);
    this.MAX_CRASH_DEQUEUE_TRIES = maxCrashDequeueTries > 0 ? maxCrashDequeueTries : 15;
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
      LOG.trace(getLogMessage(String.format("Invalidating entry ", entryPointer.getEntryId())));
    }
    final byte [] rowName = makeRowKey(GLOBAL_DATA_PREFIX, entryPointer.getEntryId());
    // Change meta data to INVALID
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
      final QueueEntrySet dequeueEntrySet = queueState.getDequeueEntrySet();
      final ClaimedEntryList claimedEntryList = queueState.getClaimedEntryList();
      final Map<Long, byte[]> cachedEntries = queueState.getCachedEntries();

      Preconditions.checkState(dequeueEntrySet.size() <= 1,
                               "More than 1 entry dequeued in single entry mode - %s", dequeueEntrySet);
      if(!dequeueEntrySet.isEmpty()) {
        long returnEntryId = dequeueEntrySet.min().getEntryId();
        if(claimedEntryList.hasNext() && claimedEntryList.peekNext().getEntryId() == returnEntryId) {
          // Crash recovery case.
          // The claimed entry list would not have been incremented for the first time in single entry mode
          claimedEntryList.next();
        }

        byte[] entryBytes = cachedEntries.get(returnEntryId);
        if(entryBytes == null) {
          throw new OperationException(StatusCode.INTERNAL_ERROR,
                  getLogMessage(String.format("Cannot fetch dequeue entry id %d from cached entries", returnEntryId)));
        }
        QueueEntry entry = new QueueEntry(entryBytes);
        dequeueStrategy.saveDequeueState(consumer, config, queueState, readPointer);
        DequeueResult dequeueResult = new DequeueResult(DequeueResult.DequeueStatus.SUCCESS,
                                                        new QueueEntryPointer(this.queueName, returnEntryId), entry);
        return dequeueResult;
      }
    }

    // If no more cached entries, read entries from storage
    if(!queueState.getClaimedEntryList().hasNext()) {
      // TODO: return a list of DequeueEntry instead of list of Long
      List<Long> entryIds = dequeueStrategy.fetchNextEntries(consumer, config, queueState, readPointer);
      readEntries(consumer, config, queueState, readPointer, entryIds);
    }

    if(queueState.getClaimedEntryList().hasNext()) {
      DequeueEntry dequeueEntry = queueState.getClaimedEntryList().next();
      this.dequeueReturns.incrementAndGet();
      queueState.getDequeueEntrySet().add(dequeueEntry);
      QueueEntry entry = new QueueEntry(queueState.getCachedEntries().get(dequeueEntry.getEntryId()));
      dequeueStrategy.saveDequeueState(consumer, config, queueState, readPointer);
      DequeueResult dequeueResult = new DequeueResult(DequeueResult.DequeueStatus.SUCCESS,
                               new QueueEntryPointer(this.queueName, dequeueEntry.getEntryId()), entry);
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
         getLogMessage(String.format("Cannot figure out the dequeue strategy to use for partitioner %s", queuePartitioner.getClass())));
    }
    return dequeueStrategy;
  }

  private void readEntries(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState, ReadPointer readPointer,
                            List<Long> entryIds) throws OperationException{
    if(LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage(String.format("Reading entries from storage - ", Arrays.toString(entryIds.toArray()))));
    }

    // Copy over the entries that are dequeued, but not yet acked
    Map<Long, byte[]> currentCachedEntries = queueState.getCachedEntries();
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
              LOG.trace(getLogMessage(String.format("Not able to read entry with entryId %d. Returning empty cached list.")));
            }
            return;
          }
          byte[] entryMetaBytes = entryMap.get(ENTRY_META);
          if(entryMetaBytes == null) {
            if (LOG.isTraceEnabled()) {
              LOG.trace(getLogMessage(String.format("Not able to decode entry with entryId %d. Returning empty cached list.")));
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
      queueState.setCachedEntries(newCachedEntries);
      queueState.setClaimedEntriesById(readEntryIds);
    }
  }

  @Override
  public void ack(QueueEntryPointer entryPointer, QueueConsumer consumer, ReadPointer readPointer)
    throws OperationException {
    // TODO: 1. Later when active entry can saved in memory, there is no need to write it into HBase
    // TODO: 2. Need to treat Ack as a simple write operation so that it can use a simple write rollback for unack
    // TODO: 3. Use Transaction.getWriteVersion instead ReadPointer

    QueuePartitioner partitioner = consumer.getQueueConfig().getPartitionerType().getPartitioner();
    final DequeueStrategy dequeueStrategy = getDequeueStrategy(partitioner);

    // Get queue state
    QueueStateImpl queueState = getQueueState(consumer, readPointer);

    // Only the entry that has been dequeued by this consumer can be acked
    if(!queueState.getDequeueEntrySet().contains(entryPointer.getEntryId())) {
      throw new OperationException(StatusCode.ILLEGAL_ACK,
                 getLogMessage(String.format("Entry %d is not dequeued by this consumer. Current active entries are %s",
                                            entryPointer.getEntryId(), queueState.getDequeueEntrySet().toString())));
    }

    // Set ack state
    queueState.getDequeueEntrySet().remove(entryPointer.getEntryId());

    // Write ack state
    dequeueStrategy.saveDequeueState(consumer, consumer.getQueueConfig(), queueState, readPointer);
  }

  @Override
  public void finalize(QueueEntryPointer entryPointer, QueueConsumer consumer, int totalNumGroups, long writePoint)
    throws OperationException {
    // Figure out queue entries that can be evicted, and evict them.
    // We are assuming here that for a given consumer all entries up to min(ACTIVE_ENTRY-1, CONSUMER_READ_POINTER-1) can be evicted.
    // The min of such evict entry is determined across all consumers across all groups, and entries till the min evict entry are removed.

    // One consumer per consumer group will do the determination of min group evict entry for each group.
    // Finally, one consumer across all groups will get the least of min group evict entries for all groups and does eviction.

    // NOTE: Using min(ACTIVE_ENTRY-1, CONSUMER_READ_POINTER-1) to determine evict entry removes the need of
    // storing/reading the finalized entry for each consumer.
    // However in this approach the last entry of each consumer may not get evicted.
    // This limitation should be okay since the number of such entries will be small (less than or equal to the number of consumers).

    // A simple leader election for selecting consumer to run eviction for group - only consumers with id 0 (one per group)
    if(consumer.getInstanceId() != 0) {
      return;
    }

    // TODO: get transaction read pointer?
    ReadPointer readPointer = oracle.dirtyReadPointer();

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
      writeKeys.add(GLOBAL_EVICT_META_PREFIX);
      writeCols.add(makeColumnName(GROUP_EVICT_ENTRY, consumer.getGroupId()));
      writeValues.add(Bytes.toBytes(minGroupEvictEntry));
    }

    if(LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage(String.format("minGroupEvictEntry=%d, groupId=%d", minGroupEvictEntry, consumer.getGroupId())));
    }

    // Only one consumer per queue will run the below eviction algorithm for the queue, all others will save minGroupEvictEntry and return
    // Again simple leader election
    if(consumer.getGroupId() == 0) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage("Running global eviction..."));
      }

      final long currentMaxEvictedEntry = runEviction(consumer, minGroupEvictEntry, totalNumGroups, readPointer);
      // Save the max of the entries that were evicted now
      if(currentMaxEvictedEntry != INVALID_ENTRY_ID) {
        writeKeys.add(GLOBAL_EVICT_META_PREFIX);
        writeCols.add(GLOBAL_LAST_EVICT_ENTRY);
        writeValues.add(Bytes.toBytes(currentMaxEvictedEntry));
      }
    }

    // Save the state
    byte[][] keyArray = new byte[writeKeys.size()][];
    byte[][] colArray = new byte[writeCols.size()][];
    byte[][] valArray = new byte[writeValues.size()][];
    table.put(writeKeys.toArray(keyArray), writeCols.toArray(colArray), writePoint, writeValues.toArray(valArray));
  }

  private long getMinGroupEvictEntry(QueueConsumer consumer, long currentConsumerFinalizeEntry, ReadPointer readPointer)
                                                                                        throws OperationException {
    // Find out the min entry that can be evicted across all consumers in the consumer's group

    // Read CONSUMER_READ_POINTER and ACTIVE_ENTRY for all consumers in the group to determine evict entry
    final byte[][] rowKeys = new byte[consumer.getGroupSize()][];
    for(int consumerId = 0; consumerId < consumer.getGroupSize(); ++consumerId) {
      rowKeys[consumerId] = makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumerId);
    }
    OperationResult<Map<byte[], Map<byte[], byte[]>>> operationResult =
      table.getAllColumns(rowKeys, new byte[][]{CONSUMER_READ_POINTER, DEQUEUE_ENTRY_SET}, readPointer);
    if(operationResult.isEmpty()) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(String.format("Not able to fetch state of group %d for eviction", consumer.getGroupId())));
      }
      return INVALID_ENTRY_ID;
    }

    long minGroupEvictEntry = Long.MAX_VALUE;
    for(int consumerId = 0; consumerId < consumer.getGroupSize(); ++consumerId) {
      // As far as consumer consumerId is concerned, all queue entries before min(ACTIVE_ENTRY, CONSUMER_READ_POINTER) can be evicted
      // The least of such entry is the minGroupEvictEntry to which all queue entries can be evicted for the group
      long evictEntry;
      if(consumerId == consumer.getInstanceId()) {
        // currentConsumerFinalizeEntry is a better entry to be evicted than the one determined by getEvictEntryForConsumer for current consumer
        // since currentConsumerFinalizeEntry > min(CONSUMER_READ_POINTER - 1, ACTIVE_ENTRY - 1)
        evictEntry = currentConsumerFinalizeEntry;
      } else {
        // For other consumers, determine evict entry based on CONSUMER_READ_POINTER or ACTIVE_ENTRY
        evictEntry = getEvictEntryForConsumer(operationResult, consumerId, consumer.getGroupId());
      }
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

  private long getEvictEntryForConsumer(OperationResult<Map<byte[], Map<byte[], byte[]>>> operationResult, int consumerId, long groupId)
                                                                                        throws OperationException {
    // evictEntry determination logic: evictEntry = (activeEntry != INVALID_ENTRY ? activeEntry - 1 : consumerReadPointer - 1)

    // Read the ACTIVE_ENTRY and CONSUMER_READ_POINTER for the consumer consumerId
    Map<byte[], byte[]> readPointerMap = operationResult.getValue().get(makeRowKey(CONSUMER_META_PREFIX, groupId, consumerId));
    if(readPointerMap == null) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(String.format("Not able to fetch readPointer/activeEntry for consumerId %d, groupId %d", consumerId, groupId)));
      }
      return INVALID_ENTRY_ID;
    }
    final byte[] activeEntryBytes = readPointerMap.get(DEQUEUE_ENTRY_SET);
    if(activeEntryBytes == null) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(String.format("Not able to decode activeEntry for consumerId %d, groupId %d", consumerId, groupId)));
      }
      return INVALID_ENTRY_ID;
    }
    long evictEntry;
    QueueEntrySet dequeueEntrySet;
    try {
       dequeueEntrySet = QueueEntrySet.decode(new BinaryDecoder(new ByteArrayInputStream(activeEntryBytes)));
    } catch (IOException e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, getLogMessage("Exception while deserializing dequeue entry list during finalize"), e);
    }
    if(!dequeueEntrySet.isEmpty()) {
      evictEntry = dequeueEntrySet.min().getEntryId() - 1;
    } else {
      byte[] consumerReadPointerBytes = readPointerMap.get(CONSUMER_READ_POINTER);
      if(consumerReadPointerBytes == null) {
        if(LOG.isTraceEnabled()) {
          LOG.trace(getLogMessage(String.format("Not able to decode readPointer for consumerId %d, groupId %d", consumerId, groupId)));
        }
        return INVALID_ENTRY_ID;
      }
      evictEntry = Bytes.toLong(consumerReadPointerBytes) - 1;
    }
    return evictEntry;
  }

  private long runEviction(QueueConsumer consumer, long currentGroupMinEvictEntry,
                           int totalNumGroups, ReadPointer readPointer) throws OperationException {
    // Get all the columns for row GLOBAL_EVICT_META_PREFIX, which contains the entry that can be evicted for each group
    // and the last evicted entry.
    OperationResult<Map<byte [], byte []>> evictBytes = table.get(GLOBAL_EVICT_META_PREFIX, readPointer);
    if(evictBytes.isEmpty() || evictBytes.getValue() == null) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage("Not able to fetch eviction information"));
      }
      return INVALID_ENTRY_ID;
    }

    // Get the last evicted entry
    Map<byte[], byte[]> evictInfoMap = evictBytes.getValue();
    byte[] lastEvictedEntryBytes = evictInfoMap.get(GLOBAL_LAST_EVICT_ENTRY);
    final long lastEvictedEntry = lastEvictedEntryBytes == null ? FIRST_QUEUE_ENTRY_ID - 1 : Bytes.toLong(lastEvictedEntryBytes);

    // Determine the max entry that can be evicted across all groups
    long maxEntryToEvict = Long.MAX_VALUE;
    for(int groupId = 0; groupId < totalNumGroups; ++groupId) {
      long entry;
      if(groupId == consumer.getGroupId()) {
        // Min evict entry for the consumer's group was just evaluated earlier but not written to storage, use that
        entry = currentGroupMinEvictEntry;
      } else {
        // Get the evict info for group with groupId
        byte[] entryBytes = evictInfoMap.get(makeColumnName(GROUP_EVICT_ENTRY, groupId));
        if(entryBytes == null) {
          if(LOG.isTraceEnabled()) {
            LOG.trace(getLogMessage(String.format("Not able to fetch maxEvictEntry for group %d", groupId)));
          }
          return INVALID_ENTRY_ID;
        }
        entry = Bytes.toLong(entryBytes);
      }
      // Save the least entry
      if(maxEntryToEvict > entry) {
        maxEntryToEvict = entry;
      }
    }

    if(maxEntryToEvict < FIRST_QUEUE_ENTRY_ID || maxEntryToEvict <= lastEvictedEntry || maxEntryToEvict == Long.MAX_VALUE) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(String.format("Nothing to evict. Entry to be evicted = %d, lastEvictedEntry = %d", maxEntryToEvict, lastEvictedEntry)));
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
  public void unack(QueueEntryPointer entryPointer, QueueConsumer consumer, ReadPointer readPointer)
    throws OperationException {
    // TODO: 1. Later when active entry can saved in memory, there is no need to write it into HBase
    // TODO: 2. Need to treat Ack as a simple write operation so that it can use a simple write rollback for unack
    // TODO: 3. Ack gets rolled back with tries=0. Need to fix this by fixing point 2 above.

    QueuePartitioner partitioner = consumer.getQueueConfig().getPartitionerType().getPartitioner();
    final DequeueStrategy dequeueStrategy = getDequeueStrategy(partitioner);

    // Get queue state
    QueueStateImpl queueState = getQueueState(consumer, readPointer);

    // Set unack state
    queueState.getClaimedEntryList().add(new DequeueEntry(entryPointer.getEntryId(), 0)); // TODO: add tries

    // Write unack state
    dequeueStrategy.saveDequeueState(consumer, consumer.getQueueConfig(), queueState, readPointer);
  }
  static long groupId = 0;
  @Override
  public long getGroupID() throws OperationException {
    // TODO: implement this :)
    return groupId++;
  }

  @Override
  public QueueAdmin.QueueInfo getQueueInfo() throws OperationException {
    // TODO: implement this :)
    return null;
  }

  private QueueStateImpl getQueueState(QueueConsumer consumer, ReadPointer readPointer) throws OperationException {
    // Determine what dequeue strategy to use based on the partitioner
    final DequeueStrategy dequeueStrategy = getDequeueStrategy(consumer.getQueueConfig().getPartitionerType().getPartitioner());

    QueueStateImpl queueState;
    // If QueueState is null, read the queue state from underlying storage.
    if(consumer.getQueueState() == null) {
      queueState = dequeueStrategy.constructQueueState(consumer, consumer.getQueueConfig(), readPointer);
    } else {
      if(! (consumer.getQueueState() instanceof QueueStateImpl)) {
        throw new OperationException(StatusCode.INTERNAL_ERROR,
              getLogMessage(String.format("Don't know how to use QueueState class %s", consumer.getQueueState().getClass())));
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

  protected byte[] makeColumnName(byte[] bytesToPrependToId, long id) {
    return Bytes.add(bytesToPrependToId, Bytes.toBytes(id));
  }

  protected byte[] makeColumnName(byte[] bytesToPrependToId, String id) {
    return Bytes.add(bytesToPrependToId, Bytes.toBytes(id));
  }

  protected String getLogMessage(String message) {
    return String.format("Queue-%s: %s", Bytes.toString(queueName), message);
  }

  static class DequeueEntry implements Comparable<DequeueEntry> {
    private final long entryId;
    // tries is used to keep track of the number of times the consumer crashed when processing the ACTIVE_ENTRY.
    // A consumer is considered to have crashed every time it loses its cached state information, and the state has to be read from underlying storage.
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

  static class QueueEntrySet {
    private final SortedSet<DequeueEntry> entrySet;

    QueueEntrySet() {
      this.entrySet = new TreeSet<DequeueEntry>();
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

    public static QueueEntrySet decode(Decoder decoder) throws IOException {
      QueueEntrySet queueEntrySet = new QueueEntrySet();
      int size = decoder.readInt();
      while(size > 0) {
        for(int i = 0; i < size; ++i) {
          queueEntrySet.add(DequeueEntry.decode(decoder));
        }
        size = decoder.readInt();
      }
      return queueEntrySet;
    }
  }

  public static class ClaimedEntryList {
    private final List<DequeueEntry> entryList;
    private int curPtr = -1;

    private static ClaimedEntryList EMPTY_LIST = new ClaimedEntryList(Collections.<DequeueEntry>emptyList());

    public static ClaimedEntryList emptyList() {
      return EMPTY_LIST;
    }

    public ClaimedEntryList(List<DequeueEntry> entryList) {
      this.entryList = entryList;
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
        throw new IllegalArgumentException(String.format("%d exceeds bounds of claimed entries %d", ptr, entryList.size()));
      }
      return entryList.get(ptr);
    }

    public void add(DequeueEntry entry) {
      entryList.add(entry);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("entryList", entryList)
        .add("curPtr", curPtr)
        .toString();
    }
  }

  public static class QueueStateImpl implements QueueState {
    private ClaimedEntryList claimedEntryList = ClaimedEntryList.EMPTY_LIST;
    private QueueEntrySet dequeueEntrySet;
    private long consumerReadPointer = FIRST_QUEUE_ENTRY_ID - 1;
    private long queueWrtiePointer = FIRST_QUEUE_ENTRY_ID - 1;
    private long claimedEntryBegin = INVALID_ENTRY_ID;
    private long claimedEntryEnd = INVALID_ENTRY_ID;
    private long lastEvictTimeInSecs = 0;

    private Map<Long, byte[]> cachedEntries;

    public QueueStateImpl() {
      dequeueEntrySet = new QueueEntrySet();
      cachedEntries = Collections.emptyMap();
    }

    public ClaimedEntryList getClaimedEntryList() {
      return claimedEntryList;
    }

    public void setClaimedEntriesById(List<Long> entryIds) {
      List<DequeueEntry> entries = Lists.newArrayList();
      for(long id : entryIds) {
        entries.add(new DequeueEntry(id, 0));
      }
      this.claimedEntryList = new ClaimedEntryList(entries);
    }

    public void setClaimedEntryList(List<DequeueEntry> entries) {
      this.claimedEntryList = new ClaimedEntryList(entries);
    }

    public QueueEntrySet getDequeueEntrySet() {
      return dequeueEntrySet;
    }

    public void setDequeueEntrySet(QueueEntrySet dequeueEntrySet) {
      this.dequeueEntrySet = dequeueEntrySet;
    }

    public long getConsumerReadPointer() {
      return consumerReadPointer;
    }

    public void setConsumerReadPointer(long consumerReadPointer) {
      this.consumerReadPointer = consumerReadPointer;
    }

    public long getClaimedEntryBegin() {
      return claimedEntryBegin;
    }

    public void setClaimedEntryBegin(long claimedEntryBegin) {
      this.claimedEntryBegin = claimedEntryBegin;
    }

    public long getClaimedEntryEnd() {
      return claimedEntryEnd;
    }

    public void setClaimedEntryEnd(long claimedEntryEnd) {
      this.claimedEntryEnd = claimedEntryEnd;
    }

    public long getQueueWritePointer() {
      return queueWrtiePointer;
    }

    public long getLastEvictTimeInSecs() {
      return lastEvictTimeInSecs;
    }

    public void setLastEvictTimeInSecs(long lastEvictTimeInSecs) {
      this.lastEvictTimeInSecs = lastEvictTimeInSecs;
    }

    public void setQueueWritePointer(long queueWritePointer) {
      this.queueWrtiePointer = queueWritePointer;
    }

    public Map<Long, byte[]> getCachedEntries() {
      return cachedEntries;
    }

    public void setCachedEntries(Map<Long, byte[]> cachedEntries) {
      this.cachedEntries = cachedEntries;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("dequeueEntrySet", dequeueEntrySet)
        .add("consumerReadPointer", consumerReadPointer)
        .add("claimedEntryBegin", claimedEntryBegin)
        .add("claimedEntryEnd", claimedEntryEnd)
        .add("queueWritePointer", queueWrtiePointer)
        .add("lastEvictTimeInSecs", lastEvictTimeInSecs)
        .add("cachedEntries", cachedEntries)
        .toString();
    }
  }

  private static class QueueStateStore {
    private final VersionedColumnarTable table;
    private byte[] rowKey;
    private final List<byte[]> columnNames = Lists.newArrayList();
    private final List<byte[]> columnValues = Lists.newArrayList();

    private OperationResult<Map<byte[], byte[]>> readResult;

    private QueueStateStore(VersionedColumnarTable table) {
      this.table = table;
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

    public void read(ReadPointer readPointer)
      throws OperationException{
      final byte[][] colNamesByteArray = new byte[columnNames.size()][];
      readResult = table.get(rowKey, columnNames.toArray(colNamesByteArray), readPointer);
    }

    public OperationResult<Map<byte[], byte[]>> getReadResult() {
      return this.readResult;
    }

    public void write(ReadPointer readPointer)
      throws OperationException {
      final byte[][] colNamesByteArray = new byte[columnNames.size()][];
      final byte[][] colValuesByteArray = new byte[columnValues.size()][];
      table.put(rowKey, columnNames.toArray(colNamesByteArray), readPointer.getMaximum(), columnValues.toArray(colValuesByteArray));
    }
  }

  interface DequeueStrategy {
    QueueStateImpl constructQueueState(QueueConsumer consumer, QueueConfig config,
                               ReadPointer readPointer) throws OperationException;
    List<Long> fetchNextEntries(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                          ReadPointer readPointer) throws OperationException;
    void saveDequeueState(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                          ReadPointer readPointer) throws OperationException;
  }

  abstract class AbstractDequeueStrategy implements DequeueStrategy {
    protected final QueueStateStore readQueueStateStore = new QueueStateStore(table);
    protected final QueueStateStore writeQueueStateStore = new QueueStateStore(table);
    protected SortedSet<Long> droppedEntries = ImmutableSortedSet.of();

    @Override
    public QueueStateImpl constructQueueState(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
                                                       throws OperationException {
      readQueueStateStore.setRowKey(makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()));
      readQueueStateStore.addColumnName(DEQUEUE_ENTRY_SET);
      readQueueStateStore.addColumnName(CONSUMER_READ_POINTER);
      readQueueStateStore.addColumnName(LAST_EVICT_TIME_IN_SECS);
      readQueueStateStore.read(readPointer);

      OperationResult<Map<byte[], byte[]>> stateBytes = readQueueStateStore.getReadResult();
      QueueStateImpl queueState = new QueueStateImpl();
      if(!stateBytes.isEmpty()) {
        // Read active entry
        ByteArrayInputStream bin = new ByteArrayInputStream(stateBytes.getValue().get(DEQUEUE_ENTRY_SET));
        BinaryDecoder decoder = new BinaryDecoder(bin);
        // TODO: Read and check schema
        try {
          queueState.setDequeueEntrySet(QueueEntrySet.decode(decoder));
        } catch (IOException e) {
          throw new OperationException(StatusCode.INTERNAL_ERROR, getLogMessage("Exception while deserializing dequeue entry list"), e);
        }

        // Read consumer read pointer
        byte[] consumerReadPointerBytes = stateBytes.getValue().get(CONSUMER_READ_POINTER);
        if(consumerReadPointerBytes != null) {
          queueState.setConsumerReadPointer(Bytes.toLong(consumerReadPointerBytes));
        }

        // Note: last evict time is read while constructing state, but it is only saved after finalize
        byte[] lastEvictTimeInSecsBytes = stateBytes.getValue().get(LAST_EVICT_TIME_IN_SECS);
        if(lastEvictTimeInSecsBytes != null) {
          queueState.setLastEvictTimeInSecs(Bytes.toLong(lastEvictTimeInSecsBytes));
        }
      }

      // Read queue write pointer
      // TODO: use raw Get instead of the workaround of incrementing zero
      long queueWritePointer = table.incrementAtomicDirtily(makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 0);
      queueState.setQueueWritePointer(queueWritePointer);

      // If dequeue entries present, read them from storage
      // This is the crash recovery case, the consumer has stopped processing before acking the previous dequeues
      if(!queueState.getDequeueEntrySet().isEmpty()) {
        droppedEntries = queueState.getDequeueEntrySet().startNewTry(MAX_CRASH_DEQUEUE_TRIES);
        // TODO: what do we do with the dropped entries?
        if(!droppedEntries.isEmpty() && LOG.isTraceEnabled()) {
          LOG.trace(getLogMessage(String.format("Dropping entries %s after %d tries", droppedEntries.toString(), MAX_CRASH_DEQUEUE_TRIES)));
        }

        // TODO: Do we still need the entries to be in dequeue list? If not, what happens if the consumer crashes again before the claimed entries are processed?
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
        throw new OperationException(StatusCode.INTERNAL_ERROR, getLogMessage("Exception while serializing dequeue entry list"), e);
      }
      writeQueueStateStore.addColumnName(DEQUEUE_ENTRY_SET);
      writeQueueStateStore.addColumnValue(bos.toByteArray());


      // Note: last evict time is read while constructing state, but it is only saved after finalize

      writeQueueStateStore.write(readPointer);
    }
  }

  class HashDequeueStrategy extends AbstractDequeueStrategy implements DequeueStrategy {
    @Override
    public List<Long> fetchNextEntries(
      QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState, ReadPointer readPointer) throws OperationException {
      long entryId = queueState.getConsumerReadPointer();
      QueuePartitioner partitioner=config.getPartitionerType().getPartitioner();
      List<Long> newEntryIds = new ArrayList<Long>();

      outerLoop:
      while (newEntryIds.isEmpty()) {
        if(entryId >= queueState.getQueueWritePointer()) {
          // Reached the end of queue as per cached QueueWritePointer,
          // read it again to see if there is any progress made by producers
          // TODO: use raw Get instead of the workaround of incrementing zero
          long queueWritePointer = table.incrementAtomicDirtily(makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 0);
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
        OperationResult<Map<byte[], Map<byte[], byte[]>>> headerResult = table.getAllColumns(rowKeys, columnKeys, readPointer);

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
            if(partitioner.shouldEmit(consumer, currentEntryId, hashValue)) {
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

  class RoundRobinDequeueStrategy extends AbstractDequeueStrategy implements DequeueStrategy {
    @Override
    public List<Long> fetchNextEntries(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState, ReadPointer readPointer) throws OperationException {
      long entryId = queueState.getConsumerReadPointer();
      QueuePartitioner partitioner=config.getPartitionerType().getPartitioner();
      List<Long> newEntryIds = new ArrayList<Long>();

      while (newEntryIds.isEmpty()) {
        if(entryId >= queueState.getQueueWritePointer()) {
          // Reached the end of queue as per cached QueueWritePointer,
          // read it again to see if there is any progress made by producers
          // TODO: use raw Get instead of the workaround of incrementing zero
          long queueWritePointer = table.incrementAtomicDirtily(makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 0);
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
          if(partitioner.shouldEmit(consumer, currentEntryId)) {
            newEntryIds.add(currentEntryId);
          }
        }
        entryId = endEntryId;
      }
      return newEntryIds;
    }
  }

  /**
   *  In FifoDequeueStrategy, the next entries are claimed by a consumer by incrementing a group-shared counter (group read pointer) atomically.
   *  The entries are claimed in batches, the claimed range is recorded in CLAIMED_ENTRY_BEGIN and CLAIMED_ENTRY_END columns
   */
  class FifoDequeueStrategy extends AbstractDequeueStrategy implements DequeueStrategy {
    @Override
    public QueueStateImpl constructQueueState(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer) throws OperationException {
      // Read CLAIMED_ENTRY_BEGIN and CLAIMED_ENTRY_END, and store them in queueState
      readQueueStateStore.addColumnName(CLAIMED_ENTRY_BEGIN);
      readQueueStateStore.addColumnName(CLAIMED_ENTRY_END);

      QueueStateImpl queueState = super.constructQueueState(consumer, config, readPointer);
      OperationResult<Map<byte[], byte[]>> stateBytes = readQueueStateStore.getReadResult();
      if(!stateBytes.isEmpty()) {
        long claimedEntryIdBegin = Bytes.toLong(stateBytes.getValue().get(CLAIMED_ENTRY_BEGIN));
        long claimedEntryIdEnd = Bytes.toLong(stateBytes.getValue().get(CLAIMED_ENTRY_END));
        if(droppedEntries.contains(claimedEntryIdBegin)) {
          // Some entries were dropped, move claimed entry begin to reflect that
          if(claimedEntryIdEnd <= droppedEntries.last()) {
            // All claimed entries are dropped
            claimedEntryIdBegin = claimedEntryIdEnd = INVALID_ENTRY_ID;
          } else {
            final long newClaimedEntryIdBegin = droppedEntries.last() + 1;
            if(newClaimedEntryIdBegin > claimedEntryIdEnd) {
              claimedEntryIdBegin = claimedEntryIdEnd = INVALID_ENTRY_ID;
            } else {
              claimedEntryIdBegin = newClaimedEntryIdBegin;
            }
          }
        }
        if(claimedEntryIdBegin != INVALID_ENTRY_ID && claimedEntryIdEnd != INVALID_ENTRY_ID) {
          queueState.setClaimedEntryBegin(claimedEntryIdBegin);
          queueState.setClaimedEntryEnd(claimedEntryIdEnd);
        }
      }
      return queueState;
    }

    @Override
    public void saveDequeueState(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                                 ReadPointer readPointer) throws OperationException {
      // If a claimed entry is now being dequeued then update CLAIMED_ENTRY_BEGIN
      if(queueState.getDequeueEntrySet().contains(queueState.getClaimedEntryBegin())) {
        long claimedEntryIdBegin = queueState.getClaimedEntryBegin();
        final long newClaimedEntryBegin = claimedEntryIdBegin + 1;
        // If reached end of claimed entries, then reset the claimed ids
        if(newClaimedEntryBegin > queueState.getClaimedEntryEnd()) {
          queueState.setClaimedEntryBegin(INVALID_ENTRY_ID);
          queueState.setClaimedEntryEnd(INVALID_ENTRY_ID);
        } else {
          queueState.setClaimedEntryBegin(newClaimedEntryBegin);
        }
      }

      // Add CLAIMED_ENTRY_BEGIN and CLAIMED_ENTRY_END to writeQueueStateStore so that they can be written to underlying storage by base class saveDequeueState
      writeQueueStateStore.addColumnName(CLAIMED_ENTRY_BEGIN);
      writeQueueStateStore.addColumnValue(Bytes.toBytes(queueState.getClaimedEntryBegin()));

      writeQueueStateStore.addColumnName(CLAIMED_ENTRY_END);
      writeQueueStateStore.addColumnValue(Bytes.toBytes(queueState.getClaimedEntryEnd()));

      super.saveDequeueState(consumer, config, queueState, readPointer);
    }

    @Override
    public List<Long> fetchNextEntries(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                                       ReadPointer readPointer) throws OperationException {
      // Determine the next batch of entries that can be dequeued by this consumer
      List<Long> newEntryIds = new ArrayList<Long>();

      // If claimed entries exist, return them. This can happen when the queue cache is lost due to consumer crash or other reasons
      long claimedEntryIdBegin = queueState.getClaimedEntryBegin();
      long claimedEntryIdEnd = queueState.getClaimedEntryEnd();
      if(claimedEntryIdBegin != INVALID_ENTRY_ID && claimedEntryIdEnd != INVALID_ENTRY_ID &&
        claimedEntryIdEnd >= claimedEntryIdBegin) {
        for(long i = claimedEntryIdBegin; i <= claimedEntryIdEnd; ++i) {
          newEntryIds.add(i);
        }
        return newEntryIds;
      }

      // Else claim new queue entries
      final long batchSize = getBatchSize(config);
      QueuePartitioner partitioner=config.getPartitionerType().getPartitioner();
      while (newEntryIds.isEmpty()) {
        // TODO: use raw Get instead of the workaround of incrementing zero
        // TODO: move counters into oracle
        // Fetch the group read pointer
        long groupReadPointetr = table.incrementAtomicDirtily(makeRowKey(GROUP_READ_POINTER, consumer.getGroupId()), GROUP_READ_POINTER, 0);
        if(groupReadPointetr + batchSize >= queueState.getQueueWritePointer()) {
          // Reached the end of queue as per cached QueueWritePointer,
          // read it again to see if there is any progress made by producers
          // TODO: use raw Get instead of the workaround of incrementing zero
          // TODO: move counters into oracle
          long queueWritePointer = table.incrementAtomicDirtily(makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 0);
          queueState.setQueueWritePointer(queueWritePointer);
        }

        // End of queue reached
        if(groupReadPointetr >= queueState.getQueueWritePointer()) {
          return Collections.EMPTY_LIST;
        }

        // If there are enough entries for all consumers to claim, then claim batchSize entries
        // Otherwise divide the entries equally among all consumers
        long curBatchSize = groupReadPointetr + (batchSize * consumer.getGroupSize()) < queueState.getQueueWritePointer() ?
          batchSize : (queueState.getQueueWritePointer() - groupReadPointetr) / consumer.getGroupSize();
        // Make sure there is progress
        if(curBatchSize < 1) {
          curBatchSize = 1;
        }

        // Claim the entries by incrementing GROUP_READ_POINTER
        long endEntryId = table.incrementAtomicDirtily(makeRowKey(GROUP_READ_POINTER, consumer.getGroupId()),
                                                       GROUP_READ_POINTER, curBatchSize);
        long startEntryId = endEntryId - curBatchSize + 1;
        // Note: incrementing GROUP_READ_POINTER, and storing the claimed entryIds in HBase ideally need to happen atomically.
        //       HBase doesn't support atomic increment and put.
        //       Also, for performance reasons we have moved the write to method saveDequeueEntryState where all writes for a dequeue happen
        queueState.setClaimedEntryBegin(startEntryId);
        queueState.setClaimedEntryEnd(endEntryId);

        final int cacheSize = (int)(endEntryId - startEntryId + 1);

        // Determine which entries  need to be read from storage based on partition type
        for(int id = 0; id < cacheSize; ++id) {
          final long currentEntryId = startEntryId + id;
          if(partitioner.shouldEmit(consumer, currentEntryId)) {
            newEntryIds.add(currentEntryId);
          }
        }
      }
      return newEntryIds;
    }
  }
}
