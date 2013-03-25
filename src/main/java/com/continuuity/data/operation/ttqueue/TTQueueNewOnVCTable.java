package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.ttqueue.internal.CachedList;
import com.continuuity.data.operation.ttqueue.internal.EntryMeta;
import com.continuuity.data.table.VersionedColumnarTable;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class TTQueueNewOnVCTable implements TTQueue {

  private static final Logger LOG = LoggerFactory.getLogger(TTQueueNewOnVCTable.class);
  protected final VersionedColumnarTable table;
  private final byte [] queueName;
  final TransactionOracle oracle;
  static final int MAX_CRASH_DEQUEUE_TRIES = 15;

  // For testing
  AtomicLong dequeueReturns = new AtomicLong(0);

  /*
  for each queue (global):
    global entry id counter for newest (highest) entry id, incremented during enqueue
    row-key        | column  | value
    <queueName>10I | 10I     | <entryId>

    data and meta data (=entryState) for each entry (together in one row per entry)
    (GLOBAL_DATA_PREFIX)
    row-key                 | column  | value
    <queueName>20D<entryId> | 20D     | <data>
                            | 10M     | <entryState>
                            | 30H     | <header data>

  for each group of consumers (= each group of flowlet instances):
    group read pointer for highest entry id processed by group of consumers
    row-key                 | column  | value
    <queueName>10I<groupId> | 10I     | <entryId>

  for each consumer(=flowlet instance)
    state of entry ids processed by consumer (one column per entry id), current active entry and consumer read pointer
    (CONSUMER_META_PREFIX)
    row-key                             | column           | value
    <queueName>40C<groupId><consumerId> | 10A              | <entryId>
                                        | 20C              | <crash retries for active entry>
                                        | 30I              | <entryId>
   */

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
  // GLOBAL_ENTRYID_COUNTER contains the counter to generate entryIds during enqueue operation. This is a unique for a queue.
  static final byte [] GLOBAL_ENTRYID_COUNTER = {10, 'I'};  //row <queueName>10I, column 10I

  // GROUP_READ_POINTER is a group counter used by consumers of a FifoDequeueStrategy group to claim queue entries.
  static final byte [] GROUP_READ_POINTER = {10, 'I'}; //row <queueName>10I<groupId>, column 10I

  // Columns for row = GLOBAL_DATA_PREFIX
  // ENTRY_META contains the meta data for a queue entry, whether the entry is invalid or not.
  static final byte [] ENTRY_META = {10, 'M'}; //row  <queueName>20D<entryId>, column 10M
  // ENTRY_DATA contains the queue entry.
  static final byte [] ENTRY_DATA = {20, 'D'}; //row  <queueName>20D<entryId>, column 20D
  // ENTRY_HEADER contains the partitioning keys of a queue entry.
  static final byte [] ENTRY_HEADER = {30, 'H'};  //row  <queueName>20D<entryId>, column 30H

  // Columns for row = GLOBAL_EVICT_META_PREFIX
  // GLOBAL_LAST_EVICT_ENTRY contains the entryId of the max evicted entry of the queue.
  // if GLOBAL_LAST_EVICT_ENTRY is not invalid, GLOBAL_LAST_EVICT_ENTRY + 1 points to the first queue entry that can be dequeued.
  static final byte [] GLOBAL_LAST_EVICT_ENTRY = {10, 'L'};   //row  <queueName>30M<groupId>, column 10L
  // GROUP_EVICT_ENTRY contains the entryId upto which the queue entries can be evicted for a group.
  // It means all consumers in the group have acked until GROUP_EVICT_ENTRY
  static final byte [] GROUP_EVICT_ENTRY = {20, 'E'};     //row  <queueName>30M<groupId>, column 20E

  // Columns for row = CONSUMER_META_PREFIX
  // ACTIVE_ENTRY points to the entryId that is dequeued by a consumer, but not yet acked.
  // Once the consumer acks the entry, ACTIVE_ENTRY is set to INVALID_ENTRY_ID until the consumer dequeues another entry.
  static final byte [] ACTIVE_ENTRY = {10, 'A'};              //row <queueName>40C<groupId><consumerId>, column 10A
  // ACTIVE_ENTRY_CRASH_TRIES is used to keep track of the number of times the consumer crashed when processing the ACTIVE_ENTRY.
  // A consumer is considered to have crashed every time it loses its cached state information, and the state has to be read from underlying storage.
  static final byte [] ACTIVE_ENTRY_CRASH_TRIES = {20, 'C'};  //row <queueName>40C<groupId><consumerId>, column 20C
  // If CONSUMER_READ_POINTER is valid, CONSUMER_READ_POINTER contains the entry that the consumer is processing, or has processed.
  static final byte [] CONSUMER_READ_POINTER = {30, 'R'};     //row <queueName>40C<groupId><consumerId>, column 30R
  // CLAIMED_ENTRY_BEGIN is used by a consumer of FifoDequeueStrategy to specify the start entryId of the batch of entries claimed by it.
  static final byte [] CLAIMED_ENTRY_BEGIN = {40, 'B'};       //row <queueName>40C<groupId><consumerId>, column 40B
  // CLAIMED_ENTRY_END is used by a consumer of FifoDequeueStrategy to specify the end entryId of the batch of entries claimed by it.
  static final byte [] CLAIMED_ENTRY_END = {50, 'E'};         //row <queueName>40C<groupId><consumerId>, column 50E
  // LAST_EVICT_TIME_IN_SECS is the time when the last eviction was run by the consumer
  static final byte [] LAST_EVICT_TIME_IN_SECS = {60, 'T'};           //row <queueName>40C<groupId><consumerId>, column 60T

  static final long INVALID_ENTRY_ID = -1;
  static final long FIRST_QUEUE_ENTRY_ID = 1;

  final long DEFAULT_BATCH_SIZE;
  final long EVICT_INTERVAL_IN_SECS;

  protected TTQueueNewOnVCTable(VersionedColumnarTable table, byte[] queueName, TransactionOracle oracle,
                                final CConfiguration conf) {
    this.table = table;
    this.queueName = queueName;
    this.oracle = oracle;

    final long defaultBatchSize = conf.getLong("ttqueue.batch.size.default", 100);
    this.DEFAULT_BATCH_SIZE = defaultBatchSize > 0 ? defaultBatchSize : 100;

    final long evictIntervalInSecs = conf.getLong("ttqueue.evict.interval.secs", 10 * 60 * 60);
    this.EVICT_INTERVAL_IN_SECS = evictIntervalInSecs >= 0 ? evictIntervalInSecs : 10 * 60 * 60;
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
         getLogMessage(String.format("Increment of global entry id failed with status code %d : %s", e.getStatus(), e.getMessage())), e);
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

    QueueStateImpl queueState = getQueueState(consumer, readPointer);

    // If the previous entry was not acked, return the same one (Note: will need to change for async mode)
    if(queueState.getActiveEntryId() != INVALID_ENTRY_ID) {
      if(!queueState.getCachedEntries().hasCurrent()) {
        throw new OperationException(StatusCode.INTERNAL_ERROR,
          getLogMessage("Cannot fetch active entry id from cached entries"));
      }
      QueueStateEntry cachedEntry = queueState.getCachedEntries().getCurrent();
      QueueEntry entry = new QueueEntry(cachedEntry.getData());
      dequeueStrategy.saveDequeueState(consumer, config, queueState, readPointer);
      DequeueResult dequeueResult = new DequeueResult(DequeueResult.DequeueStatus.SUCCESS,
                                                      new QueueEntryPointer(this.queueName, cachedEntry.getEntryId()), entry);
      return dequeueResult;
    }

    // If no more cached entries, read entries from storage
    if(!queueState.getCachedEntries().hasNext()) {
      List<Long> entryIds = dequeueStrategy.fetchNextEntries(consumer, config, queueState, readPointer);
      readEntries(consumer, config, queueState, readPointer, entryIds);
    }

    if(queueState.getCachedEntries().hasNext()) {
      QueueStateEntry cachedEntry = queueState.getCachedEntries().getNext();
      this.dequeueReturns.incrementAndGet();
      queueState.setActiveEntryId(cachedEntry.getEntryId());
      queueState.setConsumerReadPointer(cachedEntry.getEntryId());
      QueueEntry entry = new QueueEntry(cachedEntry.getData());
      dequeueStrategy.saveDequeueState(consumer, config, queueState, readPointer);
      DequeueResult dequeueResult = new DequeueResult(DequeueResult.DequeueStatus.SUCCESS,
                               new QueueEntryPointer(this.queueName, cachedEntry.getEntryId()), entry);
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
    if(entryIds.isEmpty()) {
      queueState.setCachedEntries(CachedList.EMPTY_LIST);
      return;
    }

    final byte[][] entryRowKeys = new byte[entryIds.size()][];
    for(int i = 0; i < entryIds.size(); ++i) {
      entryRowKeys[i] = makeRowKey(GLOBAL_DATA_PREFIX, entryIds.get(i));
    }

    final byte[][] entryColKeys = new byte[][]{ ENTRY_META, ENTRY_DATA };
    OperationResult<Map<byte[], Map<byte[], byte[]>>> entriesResult =
                                                          this.table.get(entryRowKeys, entryColKeys, readPointer);
    if(entriesResult.isEmpty()) {
      queueState.setCachedEntries(CachedList.EMPTY_LIST);
    } else {
      List<QueueStateEntry> entries = new ArrayList<QueueStateEntry>(entryIds.size());
      for(int i = 0; i < entryIds.size(); ++i) {
        Map<byte[], byte[]> entryMap = entriesResult.getValue().get(entryRowKeys[i]);
        if(entryMap == null) {
          queueState.setCachedEntries(CachedList.EMPTY_LIST);
          return;
        }
        byte[] entryMetaBytes = entryMap.get(ENTRY_META);
        if(entryMetaBytes == null) {
          queueState.setCachedEntries(CachedList.EMPTY_LIST);
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
          byte [] entryData = entryMap.get(ENTRY_DATA);
          entries.add(new QueueStateEntry(entryData, entryIds.get(i)));
        }
      }
      queueState.setCachedEntries(new CachedList<QueueStateEntry>(entries));
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

    // Only the entry that has been dequeued (active entry) can be acked
    if(queueState.getActiveEntryId() != entryPointer.getEntryId()) {
      throw new OperationException(StatusCode.ILLEGAL_ACK,
        getLogMessage(String.format("Entry %d is not the active entry. Current active entry is %d",
                            entryPointer.getEntryId(), queueState.getActiveEntryId())));
    }

    // Set ack state
    queueState.setActiveEntryId(INVALID_ENTRY_ID);
    queueState.setActiveEntryTries(0);

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

    // TODO: get transaction read pointer
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
      table.get(rowKeys, new byte[][]{CONSUMER_READ_POINTER, ACTIVE_ENTRY}, readPointer);
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

  private long getEvictEntryForConsumer(OperationResult<Map<byte[], Map<byte[], byte[]>>> operationResult, int consumerId, long groupId) {
    // evictEntry determination logic: evictEntry = (activeEntry != INVALID_ENTRY ? activeEntry - 1 : consumerReadPointer - 1)

    // Read the ACTIVE_ENTRY and CONSUMER_READ_POINTER for the consumer consumerId
    Map<byte[], byte[]> readPointerMap = operationResult.getValue().get(makeRowKey(CONSUMER_META_PREFIX, groupId, consumerId));
    if(readPointerMap == null) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(String.format("Not able to fetch readPointer/activeEntry for consumerId %d, groupId %d", consumerId, groupId)));
      }
      return INVALID_ENTRY_ID;
    }
    final byte[] activeEntryBytes = readPointerMap.get(ACTIVE_ENTRY);
    if(activeEntryBytes == null) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(String.format("Not able to decode activeEntry for consumerId %d, groupId %d", consumerId, groupId)));
      }
      return INVALID_ENTRY_ID;
    }
    long evictEntry;
    final long activeEntry = Bytes.toLong(activeEntryBytes);
    if(activeEntry != INVALID_ENTRY_ID) {
      evictEntry = activeEntry - 1;
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
    queueState.setActiveEntryId(entryPointer.getEntryId());
    queueState.setActiveEntryTries(0);

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

  public static class QueueStateImpl implements QueueState {
    private long activeEntryId = INVALID_ENTRY_ID;
    private int activeEntryTries = 0;
    private long consumerReadPointer = FIRST_QUEUE_ENTRY_ID - 1;
    private long queueWrtiePointer = FIRST_QUEUE_ENTRY_ID - 1;
    private long claimedEntryBegin = INVALID_ENTRY_ID;
    private long claimedEntryEnd = INVALID_ENTRY_ID;
    private long lastEvictTimeInSecs = 0;

    private CachedList<QueueStateEntry> cachedEntries;

    public QueueStateImpl() {
      cachedEntries = CachedList.emptyList();
    }

    public long getActiveEntryId() {
      return activeEntryId;
    }

    public void setActiveEntryId(long activeEntryId) {
      this.activeEntryId = activeEntryId;
    }

    public int getActiveEntryTries() {
      return activeEntryTries;
    }

    public void setActiveEntryTries(int activeEntryTries) {
      this.activeEntryTries = activeEntryTries;
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

    public CachedList<QueueStateEntry> getCachedEntries() {
      return cachedEntries;
    }

    public void setCachedEntries(CachedList<QueueStateEntry> cachedEntries) {
      this.cachedEntries = cachedEntries;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("activeEntryId", activeEntryId)
        .add("activeEntryTries", activeEntryTries)
        .add("consumerReadPointer", consumerReadPointer)
        .add("claimedEntryBegin", claimedEntryBegin)
        .add("claimedEntryEnd", claimedEntryEnd)
        .add("queueWritePointer", queueWrtiePointer)
        .add("lastEvictTimeInSecs", lastEvictTimeInSecs)
        .add("cachedEntries", cachedEntries.toString())
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

    @Override
    public QueueStateImpl constructQueueState(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
                                                       throws OperationException {
      // ACTIVE_ENTRY contains the entry if any that is dequeued, but not acked
      // CONSUMER_READ_POINTER + 1 points to the next entry that can be read by this queue consuemr
      readQueueStateStore.setRowKey(makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()));
      readQueueStateStore.addColumnName(ACTIVE_ENTRY);
      readQueueStateStore.addColumnName(ACTIVE_ENTRY_CRASH_TRIES);
      readQueueStateStore.addColumnName(CONSUMER_READ_POINTER);
      readQueueStateStore.addColumnName(LAST_EVICT_TIME_IN_SECS);
      readQueueStateStore.read(readPointer);

      OperationResult<Map<byte[], byte[]>> stateBytes = readQueueStateStore.getReadResult();
      QueueStateImpl queueState = new QueueStateImpl();
      if(!stateBytes.isEmpty()) {
        // Read active entry
        queueState.setActiveEntryId(Bytes.toLong(stateBytes.getValue().get(ACTIVE_ENTRY)));
        queueState.setActiveEntryTries(Bytes.toInt(stateBytes.getValue().get(ACTIVE_ENTRY_CRASH_TRIES)));

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

      // If active entry is present, read that from storage
      // This is the crash recovery case, the consumer has stopped processing before acking the previous dequeue
      if(queueState.getActiveEntryId() != INVALID_ENTRY_ID) {
        if(queueState.getActiveEntryTries() < MAX_CRASH_DEQUEUE_TRIES) {
          queueState.setActiveEntryTries(queueState.getActiveEntryTries() + 1);
          readEntries(consumer, config, queueState, readPointer, Collections.singletonList(queueState.getActiveEntryId()));
          // Set the active entry as the current entry
          if(queueState.getCachedEntries().hasNext()) {
            queueState.getCachedEntries().getNext();
          }
        } else {
          // TODO: what do we do with the active entry?
          if(LOG.isTraceEnabled()) {
            LOG.trace(getLogMessage(String.format("Ignoring dequeue of entry %d after %d tries", queueState.getActiveEntryId(), MAX_CRASH_DEQUEUE_TRIES)));
          }
          queueState.setActiveEntryId(INVALID_ENTRY_ID);
          queueState.setActiveEntryTries(0);
        }
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
      writeQueueStateStore.addColumnValue(Bytes.toBytes(queueState.getConsumerReadPointer()));

      writeQueueStateStore.addColumnName(ACTIVE_ENTRY);
      writeQueueStateStore.addColumnValue(Bytes.toBytes(queueState.getActiveEntryId()));

      writeQueueStateStore.addColumnName(ACTIVE_ENTRY_CRASH_TRIES);
      writeQueueStateStore.addColumnValue(Bytes.toBytes(queueState.getActiveEntryTries()));

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
        OperationResult<Map<byte[], Map<byte[], byte[]>>> headerResult = table.get(rowKeys, columnKeys, readPointer);

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
      // If a claimed entry is now being dequeued then increment CLAIMED_ENTRY_BEGIN
      if(queueState.getActiveEntryId() == queueState.getClaimedEntryBegin()) {
        // If reached end of claimed entries, then reset the claimed ids
        if(queueState.getClaimedEntryBegin() == queueState.getClaimedEntryEnd()) {
          queueState.setClaimedEntryBegin(INVALID_ENTRY_ID);
          queueState.setClaimedEntryEnd(INVALID_ENTRY_ID);
        } else {
          queueState.setClaimedEntryBegin(queueState.getClaimedEntryBegin() + 1);
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
