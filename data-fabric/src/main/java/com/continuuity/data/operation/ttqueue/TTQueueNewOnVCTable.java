package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.common.utils.Bytes;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.ttqueue.admin.QueueInfo;
import com.continuuity.data.operation.ttqueue.internal.EntryMeta;
import com.continuuity.data.operation.ttqueue.internal.EvictionHelper;
import com.continuuity.data.operation.ttqueue.internal.TTQueueNewConstants;
import com.continuuity.data.table.Scanner;
import com.continuuity.data.table.VersionedColumnarTable;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
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
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

// TODO: catch Runtime exception and translate it into OperationException
// TODO: move all queue state manipulation methods into single class
// TODO: use common code to decode/encode lists and maps
// TODO: use raw Get instead of the workaround of incrementAtomicDirtily(0)

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

  // Column prefix for every configured consumer group
  static final byte [] GLOBAL_GROUPID_PREFIX = { 'I' }; // row <queueName>50G column I<groupId>
  // for cocnvenience: stop column that immediately follows the greatest possible consumer group
  static final byte [] GLOBAL_GROUPID_ENDRANGE = { (byte)(GLOBAL_GROUPID_PREFIX[0] + 1) };
  // the value we will write to every column. It does not matter what it is
  static final byte [] GLOBAL_GROUPID_EXISTS_MARKER = { '+' };

  // Columns for row = GLOBAL_ENTRY_ID_PREFIX
  // GLOBAL_ENTRYID_COUNTER contains the counter to generate entryIds during enqueue operation.
  // There is only one such counter for a queue.
  // GLOBAL_ENTRYID_COUNTER contains the highest valid entryId for the queue
  static final byte [] GLOBAL_ENTRYID_COUNTER = {'I'};  //row <queueName>10I, column I

  // GROUP_READ_POINTER is a group counter used by consumers of a FifoDequeueStrategy group to claim queue entries.
  // GROUP_READ_POINTER contains the higest entryId claimed by consumers of a FifoDequeueStrategy group
  static final byte [] GROUP_READ_POINTER = {'I'}; //row <queueName>10I<groupId>, column I

  // Columns for row = GLOBAL_DATA_PREFIX (Global data, shared by all consumer groups)
  // ENTRY_META contains the meta data for a queue entry, whether the entry is invalid or not.
  static final byte [] ENTRY_META = {'M'}; //row  <queueName>20D<entryId>, column M
  // ENTRY_DATA contains the queue entry.
  static final byte [] ENTRY_DATA = {'D'}; //row  <queueName>20D<entryId>, column D
  // ENTRY_HEADER contains the partitioning keys of a queue entry.
  static final byte [] ENTRY_HEADER = {'H'};  //row  <queueName>20D<entryId>, column H

  // GLOBAL_LAST_EVICT_ENTRY contains the entryId of the max evicted entry of the queue.
  // if GLOBAL_LAST_EVICT_ENTRY is not invalid,
  // GLOBAL_LAST_EVICT_ENTRY + 1 points to the first queue entry that can be dequeued.
  static final byte [] GLOBAL_LAST_EVICT_ENTRY = {'L'};   //row  <queueName>30M<groupId>, column L

  // Columns for row = GLOBAL_EVICT_META_ROW (Global data, shared by all consumers)
  // GROUP_EVICT_ENTRY contains the entryId upto which the queue entries can be evicted for a group.
  // It means all consumers in the group have acked until GROUP_EVICT_ENTRY
  static final byte [] GROUP_EVICT_ENTRY = {'E'};     //row  <queueName>30M<groupId>, column 20E

  // Columns for row = CONSUMER_META_PREFIX (consumer specific information)
  // CONSUMER_STATE contains the entire encoded queue state
  static final byte [] CONSUMER_STATE = { 'S' };
  // CLAIMED_ENTRY_BEGIN is used by a consumer of FifoDequeueStrategy to specify
  // the start entryId of the batch of entries claimed by it.
  static final byte [] LAST_EVICT_TIME_IN_SECS = {'T'};           //row <queueName>40C<groupId><consumerId>, column T

  static final long INVALID_ENTRY_ID = TTQueueNewConstants.INVALID_ENTRY_ID;
  static final long FIRST_QUEUE_ENTRY_ID = TTQueueNewConstants.FIRST_ENTRY_ID;

  private final int DEFAULT_BATCH_SIZE;
  private final long EVICT_INTERVAL_IN_SECS;
  private final int MAX_CRASH_DEQUEUE_TRIES;
  private final int MAX_CONSUMER_COUNT;

  private static final byte[] ENTRY_META_INVALID = new EntryMeta(EntryMeta.EntryState.INVALID).getBytes();
  private static final byte[] ENTRY_META_VALID = new EntryMeta(EntryMeta.EntryState.VALID).getBytes();


  public TTQueueNewOnVCTable(VersionedColumnarTable table, byte[] queueName, TransactionOracle oracle,
                                final CConfiguration conf) {
    this.table = table;
    this.queueName = queueName;
    this.oracle = oracle;

    final int defaultBatchSize = conf.getInt(TTQUEUE_BATCH_SIZE_DEFAULT, 100);
    this.DEFAULT_BATCH_SIZE = defaultBatchSize > 0 ? defaultBatchSize : 100;

    // Removing check for evictIntervalInSecs >= 0, since having it less than 0 does not cause errors in queue
    this.EVICT_INTERVAL_IN_SECS = conf.getLong(TTQUEUE_EVICT_INTERVAL_SECS, 60);

    final int maxCrashDequeueTries = conf.getInt(TTQUEUE_MAX_CRASH_DEQUEUE_TRIES, 15);
    this.MAX_CRASH_DEQUEUE_TRIES = maxCrashDequeueTries > 0 ? maxCrashDequeueTries : 15;

    final int maxConsumerCount = conf.getInt(TTQUEUE_MAX_CONSUMER_COUNT, 1000);
    this.MAX_CONSUMER_COUNT = maxConsumerCount > 0 ? maxConsumerCount : 1000;
  }

  private int getBatchSize(QueueConfig queueConfig) {
    if(queueConfig.getBatchSize() > 0) {
      return queueConfig.getBatchSize();
    }
    return DEFAULT_BATCH_SIZE;
  }

  @Override
  public EnqueueResult enqueue(QueueEntry[] entries, Transaction transaction) throws OperationException {
    int n = entries.length;
    if (LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage("Enqueueing " + n + " entries, transaction=" + transaction + ")"));
    }

    // Get our unique entry id
    long lastEntryId;
    try {
      // Make sure the increment below uses increment operation of the underlying implementation directly
      // so that it is atomic (Eg. HBase increment operation)
      lastEntryId = this.table.incrementAtomicDirtily(makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, n);
    } catch (OperationException e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, getLogMessage(
        String.format("Increment of global entry id failed with status code %d : %s",
                      e.getStatus(), e.getMessage())) , e);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage("New enqueue ends with entry id " + lastEntryId));
    }

    /*
    Insert each entry with version=<cleanWriteVersion> and
    row-key = <queueName>20D<entryId> , column/value 20D/<data>, 10M/EntryState.VALID, 30H/serialized hash keys
    */

    byte[][] rowKeys = new byte[n][];
    byte[][][] allColumnKeys = new byte[n][][];
    byte[][][] allColumnValues = new byte[n][][];

    // we always write the hash keys - even if empty - to the ENTRY_HEADER column
    // this is to distinguish entries without hash keys from entries that have no been written yet
    // we write in reverse order of reading:
    // - hash keys are read first - if at all - by the hash strategy. If it does not exist, fetch stops
    // - entry meta is read next. If that does not exist, fetch stops
    // - entry data is read last. We know it always exists if the entry meta exists
    final byte[][] columnKeys = new byte[][] { ENTRY_DATA, ENTRY_META, ENTRY_HEADER };
    final byte[] entryMetaValid = new EntryMeta(EntryMeta.EntryState.VALID).getBytes();

    QueueEntryPointer[] returnPointers = new QueueEntryPointer[n];

    long entryId = lastEntryId - n;
    for (int i = 0; i < n; i++) {
      ++entryId; // this puts the first one at lastId - n + 1, and the last one at lastId
      byte[] hashKeysBytes;
      try {
        hashKeysBytes = QueueEntry.serializeHashKeys(entries[i].getHashKeys());
      } catch(IOException e) {
        throw new OperationException(StatusCode.INTERNAL_ERROR,
                                     getLogMessage("Exception while serializing entry hash keys"), e);
      }

      rowKeys[i] = makeRowKey(GLOBAL_DATA_PREFIX, entryId);
      allColumnKeys[i] = columnKeys;
      allColumnValues[i] = new byte[][] { entries[i].getData(), entryMetaValid, hashKeysBytes };
      returnPointers[i] = new QueueEntryPointer(this.queueName, entryId);
    }
    // write all rows at once
    this.table.put(rowKeys, allColumnKeys, transaction.getWriteVersion(), allColumnValues);

    // Return success with pointer to entry
    return new EnqueueResult(EnqueueResult.EnqueueStatus.SUCCESS, returnPointers);
  }

  @Override
  public EnqueueResult enqueue(QueueEntry entry, Transaction transaction) throws OperationException {
    return enqueue(new QueueEntry[] { entry }, transaction);
  }

  @Override
  public void invalidate(QueueEntryPointer[] entryPointers, Transaction transaction) throws OperationException {
    int n = entryPointers.length;
    if (n == 0) {
      return;
    }
    if(LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage(String.format(
        "Invalidating %d entries starting at  %d", n, entryPointers[0].getEntryId())));
    }
    byte[][] rowNames = new byte[n][];
    byte[][] columns = new byte[n][];
    byte[][] values = new byte[n][];

    // Change meta data to INVALID for each entry id
    for (int i = 0; i < n; i++) {
      rowNames[i] = makeRowKey(GLOBAL_DATA_PREFIX, entryPointers[i].getEntryId());
      columns[i] = ENTRY_META;
      values[i] = ENTRY_META_INVALID;
    }

    this.table.put(rowNames, columns, transaction.getWriteVersion(), values);
    // No need to delete data/headers since they will be cleaned up during eviction later
    if (LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage("Invalidated " + n + " entry pointers"));
    }
  }

  @Override
  public DequeueResult dequeue(QueueConsumer consumer, ReadPointer readPointer) throws OperationException {
    final QueueConfig config = consumer.getQueueConfig();
    if (LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage(consumer, "Attempting dequeue [curNumDequeues=" + this.dequeueReturns.get() +
                  "] (" + config + ", " + readPointer + ")"));
    }

    // Determine what dequeue strategy to use based on the partitioner
    final DequeueStrategy dequeueStrategy = getDequeueStrategy(config.getPartitionerType());
    final QueueStateImpl queueState = getQueueState(consumer, readPointer);

    // If single entry mode return the previously dequeued entry that was not acked, otherwise dequeue the next entry
    if(config.isSingleEntry()) {
      final DequeuedEntrySet dequeueEntrySet = queueState.getDequeueEntrySet();
      if (!dequeueEntrySet.isEmpty()) {

        final TransientWorkingSet transientWorkingSet = queueState.getTransientWorkingSet();
        final Map<Long, byte[]> cachedEntries = queueState.getTransientWorkingSet().getCachedEntries();

        // how many should we return? The dequeueEntrySet may contain more than the requested batch size (this happens
        // if a consumer crashes and is reconfigured to a smaller batch size when restarted).
        int numToReturn = config.returnsBatch() ? getBatchSize(config) : 1;
        List<QueueEntry> entries = Lists.newArrayListWithCapacity(numToReturn);
        List<QueueEntryPointer> pointers = Lists.newArrayListWithCapacity(numToReturn);

        for (DequeueEntry returnEntry : dequeueEntrySet.getEntryList()) {
          if (entries.size() >= numToReturn) {
            break;
          }
          long returnEntryId = returnEntry.getEntryId();
          if(transientWorkingSet.hasNext() && transientWorkingSet.peekNext().getEntryId() == returnEntryId) {
            // Crash recovery case.
            // The cached entry list would not have been incremented for the first time in single entry mode
            transientWorkingSet.next();
          }
          byte[] entryBytes = cachedEntries.get(returnEntryId);
          if(entryBytes == null) {
            throw new OperationException(
              StatusCode.INTERNAL_ERROR,
              getLogMessage(consumer,
                            String.format("Cannot fetch dequeue entry id %d from cached entries", returnEntryId)));
          }
          entries.add(new QueueEntry(entryBytes));
          pointers.add(new QueueEntryPointer(this.queueName, returnEntryId, returnEntry.getTries()));
        }

        // if we found any entries in the dequeued set, return them again (that is the contract of singleEntry)
        if (entries.size() > 0) {
          dequeueStrategy.saveDequeueState(consumer, config, queueState, readPointer);
          this.dequeueReturns.incrementAndGet();
          return new DequeueResult(DequeueResult.DequeueStatus.SUCCESS,
                                   pointers.toArray(new QueueEntryPointer[pointers.size()]),
                                   entries.toArray(new QueueEntry[entries.size()]));
        }
      }
    }

    // If no more cached entries, read entries from storage
    if (!queueState.getTransientWorkingSet().hasNext()) {
      dequeueStrategy.fetchNextEntries(consumer, config, queueState, readPointer);
    }

    // If still no queue entries available to dequue, return queue empty
    if(!queueState.getTransientWorkingSet().hasNext()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(consumer, "End of queue reached using " + "read pointer " + readPointer));
      }
      dequeueStrategy.saveDequeueState(consumer, config, queueState, readPointer);
      return new DequeueResult(DequeueResult.DequeueStatus.EMPTY);
    }

    if (!config.returnsBatch()) {
      // if returnBatch == false, return only the first transient one
      DequeueEntry dequeueEntry = queueState.getTransientWorkingSet().next();
      queueState.getDequeueEntrySet().add(dequeueEntry);
      QueueEntry entry = new QueueEntry(queueState.getTransientWorkingSet()
                                          .getCachedEntries().get(dequeueEntry.getEntryId()));
      dequeueStrategy.saveDequeueState(consumer, config, queueState, readPointer);
      this.dequeueReturns.incrementAndGet();
      return new DequeueResult(DequeueResult.DequeueStatus.SUCCESS,
                               new QueueEntryPointer(this.queueName, dequeueEntry.getEntryId(),
                                                     dequeueEntry.getTries()), entry);

    } else {
      // if returnBatch == true, return all remaining transient entries up to the requested batch size
      final int batchSize = getBatchSize(config);
      List<QueueEntryPointer> pointers = Lists.newArrayListWithCapacity(batchSize);
      List<QueueEntry> entries = Lists.newArrayListWithCapacity(batchSize);
      while (queueState.getTransientWorkingSet().hasNext() && entries.size() < batchSize) {
        DequeueEntry dequeueEntry = queueState.getTransientWorkingSet().next();
        queueState.getDequeueEntrySet().add(dequeueEntry);
        entries.add(new QueueEntry(queueState.getTransientWorkingSet()
                                     .getCachedEntries().get(dequeueEntry.getEntryId())));
        pointers.add(new QueueEntryPointer(this.queueName, dequeueEntry.getEntryId(), dequeueEntry.getTries()));
      }
      dequeueStrategy.saveDequeueState(consumer, config, queueState, readPointer);
      this.dequeueReturns.incrementAndGet();
      return new DequeueResult(DequeueResult.DequeueStatus.SUCCESS,
                               pointers.toArray(new QueueEntryPointer[pointers.size()]),
                               entries.toArray(new QueueEntry[entries.size()]));
    }
  }

  private DequeueStrategy getDequeueStrategy(QueuePartitioner.PartitionerType partitionerType)
    throws OperationException {
    if(partitionerType == null) {
      return new NoDequeueStrategy();
    }

    switch (partitionerType) {
      case HASH:
        return new HashDequeueStrategy();
      case ROUND_ROBIN:
        return new RoundRobinDequeueStrategy();
      case FIFO:
        return new FifoDequeueStrategy();
      default:
        return new NoDequeueStrategy();
    }
  }

  @Override
  public void ack(QueueEntryPointer entryPointer, QueueConsumer consumer, Transaction transaction)
    throws OperationException {
    ack(new QueueEntryPointer[] { entryPointer }, consumer, transaction);
  }

  @Override
  public void ack(QueueEntryPointer[] entryPointers, QueueConsumer consumer, Transaction transaction)
    throws OperationException {
    final DequeueStrategy dequeueStrategy = getDequeueStrategy(consumer.getQueueConfig().getPartitionerType());

    // Get queue state
    ReadPointer readPointer = transaction.getReadPointer();
    QueueStateImpl queueState = getQueueState(consumer, readPointer);

    long minAckEntry = Long.MAX_VALUE;
    for (QueueEntryPointer entryPointer : entryPointers) {
      // Only an entry that has been dequeued by this consumer can be acked
      if(!queueState.getDequeueEntrySet().contains(entryPointer.getEntryId())) {
        throw new OperationException(
          StatusCode.ILLEGAL_ACK,
          getLogMessage(consumer,
            String.format("Entry %d is not dequeued by this consumer. Current active entries are %s",
                          entryPointer.getEntryId(), queueState.getDequeueEntrySet())));
      }
      // Set ack state
      queueState.getDequeueEntrySet().remove(entryPointer.getEntryId());

      // Determine min ack entry
      if(entryPointer.getEntryId() < minAckEntry) {
        minAckEntry = entryPointer.getEntryId();
      }
    }

    // Clean up acked entries in EvictionHelper, committed acked entries can be removed
    queueState.getEvictionHelper().cleanup(transaction);

    // Store the minAckEntry in EvictionHelper
    if(minAckEntry != Long.MAX_VALUE) {
      queueState.getEvictionHelper().addMinAckEntry(transaction.getWriteVersion(), minAckEntry);
    }

    // Write ack state
    dequeueStrategy.saveDequeueState(consumer, consumer.getQueueConfig(), queueState, readPointer);
  }

  @Override
  public void unack(QueueEntryPointer[] entryPointers, QueueConsumer consumer, Transaction transaction)
    throws OperationException {
    final DequeueStrategy dequeueStrategy = getDequeueStrategy(consumer.getQueueConfig().getPartitionerType());

    // Get queue state
    ReadPointer readPointer = transaction.getReadPointer();
    QueueStateImpl queueState = getQueueState(consumer, readPointer);

    // TODO: check opex crash state
    // Set unack state
    List<Long> unCachedEntries = Lists.newArrayList();
    for (QueueEntryPointer entryPointer : entryPointers) {
      queueState.getDequeueEntrySet().add(new DequeueEntry(entryPointer.getEntryId(), entryPointer.getTries()));

      // If entry is not present in the cache, we'll need it again
      if(!queueState.getTransientWorkingSet().getCachedEntries().containsKey(entryPointer.getEntryId())) {
        unCachedEntries.add(entryPointer.getEntryId());
      }
    }

    // Read entries that are not cached
    if(!unCachedEntries.isEmpty()) {
      QueueStateImpl tempState = new QueueStateImpl();
      dequeueStrategy.readEntries(tempState, readPointer, unCachedEntries);
      Map<Long, byte[]> newCachedEntries = tempState.getTransientWorkingSet().getCachedEntries();
      if(!newCachedEntries.isEmpty()) {
        TransientWorkingSet oldTransientWorkingSet = queueState.getTransientWorkingSet();
        queueState.setTransientWorkingSet(TransientWorkingSet.addCachedEntries(oldTransientWorkingSet,
                                                                               newCachedEntries));
      }
    }

    // Write unack state
    dequeueStrategy.saveDequeueState(consumer, consumer.getQueueConfig(), queueState, readPointer);
  }

  @Override
  public int configure(QueueConsumer newConsumer, ReadPointer readPointer) throws OperationException {
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

    final ReadPointer dirtyReadPointer = TransactionOracle.DIRTY_READ_POINTER;
    final QueueConfig config = newConsumer.getQueueConfig();
    final int newConsumerCount = newConsumer.getGroupSize();
    final long groupId = newConsumer.getGroupId();

    if(LOG.isDebugEnabled()) {
      LOG.trace(getLogMessage(newConsumer, String.format(
        "Running configure with consumer=%s, readPointer= %s", newConsumer, readPointer)));
    }

    if(newConsumerCount < 1) {
      throw new OperationException(
        StatusCode.ILLEGAL_GROUP_CONFIG_CHANGE,
        getLogMessage(newConsumer, String.format("New consumer count (%d) should atleast be 1", newConsumerCount)));
    }

    // Determine what dequeue strategy to use based on the partitioner
    final DequeueStrategy dequeueStrategy = getDequeueStrategy(config.getPartitionerType());

    // Read queue state for all consumers of the group to find the number of previous consumers
    int oldConsumerCount = 0;
    List<QueueConsumer> consumers = Lists.newArrayList();
    List<QueueStateImpl> queueStates = Lists.newArrayList();
    for(int i = 0; i < MAX_CONSUMER_COUNT; ++i) {
      // Note: the consumers created here do not contain QueueConsumer.partitioningKey or QueueConsumer.groupSize
      StatefulQueueConsumer consumer = new StatefulQueueConsumer(i, groupId, MAX_CONSUMER_COUNT, config);
      QueueStateImpl queueState = null;
      try {
        queueState = dequeueStrategy.readQueueState(consumer, config, dirtyReadPointer);
      } catch(OperationException e) {
        if(e.getStatus() != StatusCode.NOT_CONFIGURED) {
          throw e;
        }
      }
      if(queueState == null) {
        break;
      }
      ++oldConsumerCount;
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

    // Verify there are no inflight entries, or acked entries not committed yet
    for (int i = 0; i < oldConsumerCount; ++i) {
      QueueStateImpl queueState = queueStates.get(i);

      if(!queueState.getDequeueEntrySet().isEmpty()) {
        throw new OperationException(
          StatusCode.ILLEGAL_GROUP_CONFIG_CHANGE,
          getLogMessage(String.format("Consumer (%d, %d) still has %d inflight entries", newConsumer.getGroupId(), i,
                                      queueState.getDequeueEntrySet().size()))
        );
      }

      EvictionHelper evictionHelper = queueState.getEvictionHelper();
      // Run cleanup to remove committed transactions
      evictionHelper.cleanup(new Transaction(TransactionOracle.DIRTY_WRITE_VERSION, readPointer, true));
      if(evictionHelper.size() > 0) {
        throw new OperationException(
          StatusCode.ILLEGAL_GROUP_CONFIG_CHANGE,
          getLogMessage(String.format("Consumer (%d, %d) still has %d uncommited transactions",
                                      newConsumer.getGroupId(), i, evictionHelper.size()))
        );
      }
    }

    // Delete eviction information for all groups
    // We get the list of groups to evict from the group eviction information. Whenever there is a configuration change
    // we'll need to delete the eviction information for all groups so that we always maintain eviction information for
    // active groups only
    EvictionState evictionState = new EvictionState(table);
    evictionState.deleteGroupEvictionState(dirtyReadPointer, TransactionOracle.DIRTY_WRITE_VERSION);

    dequeueStrategy.configure(consumers, queueStates, config, groupId, oldConsumerCount, newConsumerCount, dirtyReadPointer);

    // Add it to the list of configured groups
    this.table.put(makeRowName(GLOBAL_GENERIC_PREFIX), makeColumnName(GLOBAL_GROUPID_PREFIX, groupId),
                   TransactionOracle.DIRTY_WRITE_VERSION, GLOBAL_GROUPID_EXISTS_MARKER);

    return oldConsumerCount;
  }

  // this has package visibility for access from test package
  List<Long> listAllConfiguredGroups() throws OperationException {
    // read column range that includes all group ids ['G'..'H'[
    OperationResult<Map<byte[], byte[]>> result =
      this.table.get(makeRowName(GLOBAL_GENERIC_PREFIX), GLOBAL_GROUPID_PREFIX, GLOBAL_GROUPID_ENDRANGE,
                     -1,  TransactionOracle.DIRTY_READ_POINTER);
    if (result.isEmpty() || result.getValue().isEmpty()) {
      return Collections.emptyList();
    }
    List<Long> ids = Lists.newArrayListWithExpectedSize(result.getValue().size());
    for (byte[] column : result.getValue().keySet()) {
      if (column.length != Bytes.SIZEOF_LONG + GLOBAL_GROUPID_PREFIX.length) {
        continue; // this must be something else that ended up in this row
      }
      if (!Bytes.startsWith(column, GLOBAL_GROUPID_PREFIX)) {
        continue; // this must be something else that ended up in this row (but why is it in the range?)
      }
      long id = Bytes.toLong(column, GLOBAL_GROUPID_PREFIX.length);
      ids.add(id);
    }
    return ids;
  }

  @Override
  public List<Long> configureGroups(List<Long> groupIds) throws OperationException {
    List<Long> toDeleteGroups = Lists.newLinkedList(listAllConfiguredGroups());
    toDeleteGroups.removeAll(groupIds);

    // Note: this dequeue strategy is a NoDequeueStrategy and cannot perform all operations of DequeueStrategy
    DequeueStrategy dequeueStrategy = getDequeueStrategy(null);

    for (long groupId : toDeleteGroups) {
      // iterate over every consumer and delete its state
      for (int consumerId = 0; consumerId < MAX_CONSUMER_COUNT; consumerId++) {
        if(dequeueStrategy.dequeueStateExists(groupId, consumerId)) {
          dequeueStrategy.deleteDequeueState(groupId, consumerId);
        } else {
          break;
        }
      }
      // We can now delete the group from the group list
      table.delete(makeRowName(GLOBAL_GENERIC_PREFIX), makeColumnName(GLOBAL_GROUPID_PREFIX, groupId),
                   TransactionOracle.DIRTY_WRITE_VERSION);
    }
    return toDeleteGroups;
  }


  @Override
  public long getGroupID() throws OperationException {
    List<Long> existing = this.listAllConfiguredGroups();
    long id;
    do {
      id = this.table.incrementAtomicDirtily(makeRowName(GLOBAL_GENERIC_PREFIX), Bytes.toBytes("GROUP_ID"), 1);
    } while (existing.contains(id));
    return id;
  }

  @Override
  public QueueInfo getQueueInfo() throws OperationException {
    List<Long> groupIds = this.listAllConfiguredGroups();
    Map<Long, List<QueueState>> groupInfos = Maps.newHashMap();
    for (long groupId : groupIds) {
      List<QueueState> states = Lists.newArrayList();
      // iterate over every consumer and retrieve its state
      for (int consumerId = 0; consumerId < MAX_CONSUMER_COUNT; consumerId++) {
        final byte[] rowKey = makeRowKey(CONSUMER_META_PREFIX, groupId, consumerId);
        OperationResult<byte[]> readResult = table.get(rowKey, CONSUMER_STATE, TransactionOracle.DIRTY_READ_POINTER);
        if (readResult.isEmpty()) {
          break;
        }
        try {
          states.add(QueueStateImpl.decode(new BinaryDecoder(new ByteArrayInputStream(readResult.getValue()))));
        } catch (IOException e) {
          // can't read this group state... ignoring because we want to return all useful info
        }
      }
      groupInfos.put(groupId, states);
    }
    Map<String, Object> info = Maps.newHashMap();

    // Read queue write pointer
    long queueWritePointer = table.incrementAtomicDirtily(
      makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 0);
    info.put("writePointer", queueWritePointer);
    info.put("groups", groupInfos);

    // Read eviction information
    EvictionState evictionState = new EvictionState(table);
    evictionState.readEvictionState(TransactionOracle.DIRTY_READ_POINTER);
    info.put("globalLastEvictEntry", evictionState.getGlobalLastEvictEntry());
    info.put("groupEvictEntriesMap", evictionState.getGroupEvictEntryMap());

    // return the entire map as a json string
    Gson gson = new Gson();
    return new QueueInfo(gson.toJson(info));
  }

  @Override
  public void finalize(QueueEntryPointer[] entryPointers, QueueConsumer consumer, int totalNumGroups,
                       Transaction transaction) throws OperationException {
    // for batch finalize, we don't know whether there are gaps in the sequence of entries getting finalized. So we
    // ignore the current finalize entry set and evict independent of that.
    finalize(entryPointers[entryPointers.length - 1], consumer, totalNumGroups, transaction);
  }

  @Override
  public void finalize(QueueEntryPointer entryPointer, QueueConsumer consumer, int totalNumGroups,
                       Transaction transaction) throws OperationException {

    // Figure out queue entries that can be evicted, and evict them.
    // The logic that figures out the min evict entry is in
    // {@link com.continuuity.data.operation.ttqueue.internal.EvictionHelper#getMinEvictionEntry}.
    // Note: entryPointer is not used in evict entry determination

    // The min of such evict entry is determined across all consumers across all groups,
    // and entries till the min evict entry are removed.

    // One consumer per consumer group will do the determination of min group evict entry for each group.
    // Finally, one consumer across all groups will get the least of min group evict entries for all groups
    // and does the eviction.

    // A simple leader election for selecting consumer to run eviction for group
    // Only consumers with id 0 (one per group)

    if(consumer.getInstanceId() != 0) {
      return;
    }

    if(totalNumGroups <= 0) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(consumer, String.format("totalNumGroups=%d, nothing to be evicted", totalNumGroups)));
      }
      return;
    }

    ReadPointer dirtyReadPointer = TransactionOracle.DIRTY_READ_POINTER;

    try {
      // Run eviction only if EVICT_INTERVAL_IN_SECS secs have passed since the last eviction run
      final long evictStartTimeInSecs = System.currentTimeMillis() / 1000;
      QueueStateImpl queueState = getQueueState(consumer, dirtyReadPointer);

      // if queue state does not have the last evict time (==0), read it from table
      byte[] rowKey;
      if (queueState.getLastEvictTimeInSecs() == 0) {
        rowKey = makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId());
        OperationResult<byte[]> result = table.get(rowKey, LAST_EVICT_TIME_IN_SECS, dirtyReadPointer);
        if (!result.isEmpty() && result.getValue() != null && result.getValue().length > 0) {
          queueState.setLastEvictTimeInSecs(Bytes.toLong(result.getValue()));
        }
      }

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
        LOG.trace(getLogMessage(consumer, String.format("Running eviction for group %d", consumer.getGroupId())));
      }

      // Find the min entry that can be evicted for the consumer's group
      final long minGroupEvictEntry = getMinGroupEvictEntry(consumer, queueState, transaction);
      // Save the minGroupEvictEntry for the consumer's group
      if(minGroupEvictEntry != INVALID_ENTRY_ID) {
        writeKeys.add(makeRowName(GLOBAL_EVICT_META_ROW));
        writeCols.add(makeColumnName(GROUP_EVICT_ENTRY, consumer.getGroupId()));
        writeValues.add(Bytes.toBytes(minGroupEvictEntry));
      }

      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(consumer, String.format(
          "minGroupEvictEntry=%d, groupId=%d", minGroupEvictEntry, consumer.getGroupId())));
      }

      // Only one consumer per queue will run the below eviction algorithm for the queue,
      // all others will save minGroupEvictEntry and return
      // If runEviction returns INVALID_ENTRY_ID, then this consumer did not run eviction
      final long currentMaxEvictedEntry = runEviction(consumer, minGroupEvictEntry, totalNumGroups, dirtyReadPointer);

      // Save the max of the entries that were evicted now
      if(currentMaxEvictedEntry != INVALID_ENTRY_ID) {
        writeKeys.add(makeRowName(GLOBAL_LAST_EVICT_ENTRY));
        writeCols.add(GLOBAL_LAST_EVICT_ENTRY);
        writeValues.add(Bytes.toBytes(currentMaxEvictedEntry));
      }

      // Save the state
      byte[][] keyArray = new byte[writeKeys.size()][];
      byte[][] colArray = new byte[writeCols.size()][];
      byte[][] valArray = new byte[writeValues.size()][];

      table.put(writeKeys.toArray(keyArray), writeCols.toArray(colArray),
                TransactionOracle.DIRTY_WRITE_VERSION, writeValues.toArray(valArray));

    } catch (OperationException e) {
      LOG.warn(getLogMessage(consumer,
                             String.format("Eviction for group %d failed with exception", consumer.getGroupId())), e);
      // ignore the error, we don't want throw exceptions in finalize()
    }
  }

  private long getMinGroupEvictEntry(QueueConsumer consumer, QueueStateImpl currentConsumerState,
                                     Transaction transaction) throws OperationException {
    // Find out the min entry that can be evicted across all consumers in the consumer's group

    // Read EvictionHelper for all consumers in the group to determine evict entry
    final byte[][] rowKeys = new byte[consumer.getGroupSize()][];
    for(int consumerId = 0; consumerId < consumer.getGroupSize(); ++consumerId) {
      rowKeys[consumerId] = makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumerId);
    }
    OperationResult<Map<byte[], Map<byte[], byte[]>>> operationResult =
      table.getAllColumns(rowKeys, new byte[][]{CONSUMER_STATE }, TransactionOracle.DIRTY_READ_POINTER);
    if(operationResult.isEmpty()) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(consumer, String.format(
          "Not able to fetch state of group %d for eviction", consumer.getGroupId())));
      }
      return INVALID_ENTRY_ID;
    }

    long minGroupEvictEntry = Long.MAX_VALUE;
    for(int consumerId = 0; consumerId < consumer.getGroupSize(); ++consumerId) {
      long evictEntry = getEvictEntryForConsumer(operationResult.getValue(), consumerId, consumer,
                                                 currentConsumerState, transaction);
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

  private long getEvictEntryForConsumer(Map<byte[], Map<byte[], byte[]>> rows, int consumerId, QueueConsumer consumer,
                                        QueueStateImpl currentConsumerState, Transaction transaction)
    throws OperationException {
    // evictEntry determination logic:
    // evictEntry = min of all committed acked entries

    // get the queue state for the consumer id
    QueueStateImpl queueState;
    if (consumerId == consumer.getInstanceId()) {
      queueState = currentConsumerState;
    } else {
      // different consumer, must read state from table
      Map<byte[], byte[]> row = rows.get(makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumerId));
      if(row == null) {
        if(LOG.isTraceEnabled()) {
          LOG.trace(getLogMessage(consumer, String.format("Not able to fetch groupState for consumerId %d, groupId %d",
                                                consumerId, consumer.getGroupId())));
        }
        return INVALID_ENTRY_ID;
      }
      final byte[] consumerStateBytes = row.get(CONSUMER_STATE);
      if(consumerStateBytes == null) {
        if(LOG.isTraceEnabled()) {
          LOG.trace(getLogMessage(consumer, String.format(
            "Not able to decode dequeue entry set for consumerId %d, groupId %d", consumerId, consumer.getGroupId())));
        }
        return INVALID_ENTRY_ID;
      }
      try {
        queueState = QueueStateImpl.decode(new BinaryDecoder(new ByteArrayInputStream(consumerStateBytes)));
      } catch (IOException e) {
        throw new OperationException(
          StatusCode.INTERNAL_ERROR,
          getLogMessage(
            consumer,
            String.format("Exception while deserializing cpnsumer state during finalize for consumerId %d, groupId %d",
                          consumerId, consumer.getGroupId())),
          e);
      }
    }
    DequeueStrategy dequeueStrategy = getDequeueStrategy(queueState.getPartitioner());
    EvictionHelper evictionHelper = queueState.getEvictionHelper();
    return evictionHelper.getMinEvictionEntry(dequeueStrategy.getMinUnAckedEntry(queueState), transaction);
  }

  class EvictionState {
    private long globalLastEvictEntry = FIRST_QUEUE_ENTRY_ID - 1;
    private Map<Long, Long> groupEvictEntries = Maps.newHashMap();

    private final VersionedColumnarTable table;

    public EvictionState(VersionedColumnarTable table) {
      this.table = table;
    }

    public void readEvictionState(ReadPointer readPointer) throws OperationException {
      // Read GLOBAL_LAST_EVICT_ENTRY
      OperationResult<byte[]> lastEvictEntryBytes = table.get(makeRowName(GLOBAL_LAST_EVICT_ENTRY),
                                                              GLOBAL_LAST_EVICT_ENTRY, readPointer);
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

      // Write byte[0] to delete entries. If we delete, then HBase may not allow us to add these rows again in the
      // future (this is due to using the same dirty time stamp every time)
      byte[][] columnKeys = new byte[groupEvictEntries.size()][];
      byte[][] values = new byte[groupEvictEntries.size()][];
      int i = 0;
      for(Map.Entry<Long, Long> entry : groupEvictEntries.entrySet()) {
        columnKeys[i] = makeColumnName(GROUP_EVICT_ENTRY, entry.getKey());
        values[i] = Bytes.EMPTY_BYTE_ARRAY;
        ++i;
      }
      table.put(makeRowName(GLOBAL_EVICT_META_ROW), columnKeys, writeVersion, values);
    }

    public long getGlobalLastEvictEntry() {
      return globalLastEvictEntry;
    }

    public Set<Long> getGroupIds() {
      return Collections.unmodifiableSet(groupEvictEntries.keySet());
    }

    public Long getGroupEvictEntry(long groupId) {
      return groupEvictEntries.get(groupId);
    }

    public Map<Long, Long> getGroupEvictEntryMap() {
      return Collections.unmodifiableMap(groupEvictEntries);
    }

    private void readGroupEvictInformationInternal(ReadPointer readPointer) throws OperationException {
      // Read evict information for all groups
      OperationResult<Map<byte[], byte[]>> groupEvictBytes = table.get(makeRowName(GLOBAL_EVICT_META_ROW), readPointer);
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
    EvictionState evictionState = new EvictionState(table);
    evictionState.readEvictionState(readPointer);

    final long lastEvictedEntry = evictionState.getGlobalLastEvictEntry();

    // Continue eviction only if eviction information for all totalNumGroups groups can be read
    final int numGroupsRead = evictionState.getGroupIds().size();
    if(numGroupsRead < totalNumGroups) {
      if(LOG.isDebugEnabled()) {
        LOG.trace(getLogMessage(consumer,
          String.format("Cannot get eviction information for all %d groups, got only for %d groups. Aborting eviction.",
                        totalNumGroups, numGroupsRead)));
      }
      return INVALID_ENTRY_ID;
    } else if(numGroupsRead > totalNumGroups) {
      LOG.warn(getLogMessage(consumer,
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
        LOG.trace(getLogMessage(consumer,
                                String.format("Min groupId=%d, current consumer groupId=%d. Aborting eviction",
                                              groupIds.get(0), consumer.getGroupId())));
      }
      return INVALID_ENTRY_ID;
    }

    if(LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage(consumer, "Running global eviction..."));
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
      if(entry == INVALID_ENTRY_ID) {
        if(LOG.isTraceEnabled()) {
          LOG.trace(getLogMessage(
            String.format("Invalid group evict entry for group %d. Aborting eviction.", groupId)));
        }
        return INVALID_ENTRY_ID;
      }
      // Save the least entry
      if(maxEntryToEvict > entry) {
        maxEntryToEvict = entry;
      }
    }

    if(maxEntryToEvict < FIRST_QUEUE_ENTRY_ID || maxEntryToEvict <= lastEvictedEntry ||
      maxEntryToEvict == Long.MAX_VALUE) {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(consumer, String.format(
          "Nothing to evict. Entry to be evicted = %d, lastEvictedEntry = %d", maxEntryToEvict, lastEvictedEntry)));
      }
      return INVALID_ENTRY_ID;
    }

    final long startEvictEntry = lastEvictedEntry + 1;

    if(LOG.isTraceEnabled()) {
      LOG.trace(getLogMessage(consumer,
                              String.format("Evicting entries from %d to %d", startEvictEntry, maxEntryToEvict)));
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
  public void dropInflightState(QueueConsumer consumer, ReadPointer readPointer) throws OperationException {
    QueueStateImpl queueState = getQueueState(consumer, readPointer);
    queueState.clearInflight();

    DequeueStrategy dequeueStrategy = getDequeueStrategy(consumer.getQueueConfig().getPartitionerType());
    dequeueStrategy.saveDequeueState(consumer, consumer.getQueueConfig(), queueState, readPointer);
  }

  private QueueStateImpl getQueueState(QueueConsumer consumer, ReadPointer readPointer) throws OperationException {
    // Determine what dequeue strategy to use based on the partitioner
    final DequeueStrategy dequeueStrategy = getDequeueStrategy(consumer.getQueueConfig().getPartitionerType());

    QueueStateImpl queueState;
    // If QueueState is null, read the queue state from underlying storage.
    if(consumer.getQueueState() == null) {
      queueState = dequeueStrategy.readQueueState(consumer, consumer.getQueueConfig(), readPointer);
    } else {
      if(! (consumer.getQueueState() instanceof QueueStateImpl)) {
        throw new OperationException(StatusCode.INTERNAL_ERROR,
              getLogMessage(consumer,
                            String.format("Don't know how to use QueueState class %s",
                                          consumer.getQueueState().getClass())));
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

  protected static Long removePrefixFromLongId(byte[] bytes, byte[] prefix) {
    if(Bytes.startsWith(bytes, prefix) && (bytes.length >= Bytes.SIZEOF_LONG + prefix.length)) {
      return Bytes.toLong(bytes, prefix.length);
    }
    return null;
  }

  protected String getLogMessage(String message) {
    return String.format("Queue-%s: %s", Bytes.toString(queueName), message);
  }

  protected String getLogMessage(QueueConsumer consumer, String message) {
    return String.format("Queue-%s Consumer(%d, %d): %s", Bytes.toString(queueName), consumer.getGroupId(),
                         consumer.getInstanceId(), message);
  }

  public static class DequeueEntry implements Comparable<DequeueEntry> {
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
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DequeueEntry that = (DequeueEntry) o;
      return entryId == that.entryId;
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

  public static class DequeuedEntrySet {
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

    public void clear() {
      entrySet.clear();
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

    public static DequeuedEntrySet decode(Decoder decoder) throws IOException {
      int size = decoder.readInt();
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
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DequeuedEntrySet that = (DequeuedEntrySet) o;
      return entrySet == null ? that.entrySet == null : entrySet.equals(that.entrySet);
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
    private final int weight;

    private static final TransientWorkingSet EMPTY_SET =
      new TransientWorkingSet(Collections.<Long>emptyList(), Collections.<Long, byte[]>emptyMap());

    public static TransientWorkingSet emptySet() {
      return EMPTY_SET;
    }

    public TransientWorkingSet(List<Long> entryIds, Map<Long, byte[]> cachedEntries) {
      int wt = 0;
      List<DequeueEntry> entries = Lists.newArrayListWithCapacity(entryIds.size());
      for(long id : entryIds) {
        byte[] entry = cachedEntries.get(id);
        if(entry == null) {
          throw new IllegalArgumentException(String.format("Cached entries does not contain entry %d", id));
        }
        entries.add(new DequeueEntry(id, 0));
        wt += entry.length;
      }
      this.entryList = entries;
      this.cachedEntries = cachedEntries;
      this.weight = wt;
    }

    public TransientWorkingSet(List<DequeueEntry> entryList, int curPtr, Map<Long, byte[]> cachedEntries, int weight) {
      this.entryList = entryList;
      this.curPtr = curPtr;
      this.cachedEntries = cachedEntries;
      this.weight = weight;
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

    public void moveToEnd() {
      curPtr = entryList.size();
    }

    private DequeueEntry fetch(int ptr) {
      if(ptr >= entryList.size()) {
        throw new IllegalArgumentException(String.format(
          "%d exceeds bounds of claimed entries %d", ptr, entryList.size()));
      }
      return entryList.get(ptr);
    }

    public Map<Long, byte[]> getCachedEntries() {
      return Collections.unmodifiableMap(cachedEntries);
    }

    public static TransientWorkingSet addCachedEntries(TransientWorkingSet oldTransientWorkingSet,
                                                       Map<Long, byte[]> moreCachedEntries) {
      Map<Long, byte[]> newCachedEntries = Maps.newHashMap(oldTransientWorkingSet.getCachedEntries());
      newCachedEntries.putAll(moreCachedEntries);
      return new TransientWorkingSet(oldTransientWorkingSet.entryList, oldTransientWorkingSet.curPtr,
                                     newCachedEntries, oldTransientWorkingSet.getWeight());
    }

    public int getWeight() {
      return weight;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("entryList", entryList)
        .add("curPtr", curPtr)
        .add("cachedEntries", cachedEntries)
        .add("weight", weight)
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
      List<DequeueEntry> entries = Lists.newArrayListWithExpectedSize(size);
      while(size > 0) {
        for(int i = 0; i < size; ++i) {
          entries.add(DequeueEntry.decode(decoder));
        }
        size = decoder.readInt();
      }

      int curPtr = decoder.readInt();

      int mapSize = decoder.readInt();
      int weight = 0;
      Map<Long, byte[]> cachedEntries = Maps.newHashMapWithExpectedSize(mapSize);
      while(mapSize > 0) {
        for(int i= 0; i < mapSize; ++i) {
          long id = decoder.readLong();
          byte[] entry = decoder.readBytes().array();
          cachedEntries.put(id, entry);
          weight += entry.length;
        }
        mapSize = decoder.readInt();
      }
      return new TransientWorkingSet(entries, curPtr, cachedEntries, weight);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TransientWorkingSet that = (TransientWorkingSet) o;
      return (curPtr == that.curPtr) &&
        (entryList == null ? that.entryList == null : entryList.equals(that.entryList)) &&
        (cachedEntries == null ? that.cachedEntries == null : cachedEntriesEquals(cachedEntries, that .cachedEntries));
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
      return minAckEntryId > maxAckEntryId;
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
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ReconfigPartitionInstance that = (ReconfigPartitionInstance) o;
      return instanceId == that.instanceId && maxAckEntryId == that.maxAckEntryId;
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

    @Override
    public boolean shouldEmit(int groupSize, int instanceId, long entryId, Integer hash) {
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
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ReconfigPartitioner that = (ReconfigPartitioner) o;
      return groupSize == that.groupSize &&
        partitionerType == that.partitionerType &&
        reconfigPartitionInstances.equals(that.reconfigPartitionInstances);
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
      new ReconfigPartitionersList(Collections.<ReconfigPartitioner>emptyList());

    public static ReconfigPartitionersList getEmptyList() {
      return EMPTY_RECONFIGURATION_PARTITIONERS_LIST;
    }

    public ReconfigPartitionersList(List<ReconfigPartitioner> reconfigPartitioners) {
      this.reconfigPartitioners = Lists.newLinkedList(reconfigPartitioners);
    }

    public List<ReconfigPartitioner> getReconfigPartitioners() {
      return reconfigPartitioners;
    }

    @Override
    public boolean shouldEmit(int groupSize, int instanceId, long entryId, Integer hash) {
      // Return false if the entry has been acknowledged by any of the previous partitions
      for(ReconfigPartitioner partitioner : reconfigPartitioners) {
        if(!partitioner.shouldEmit(groupSize, instanceId, entryId, hash)) {
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
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ReconfigPartitionersList that = (ReconfigPartitionersList) o;
      return Objects.equal(reconfigPartitioners, that.reconfigPartitioners);
    }

    @Override
    public int hashCode() {
      return reconfigPartitioners != null ? reconfigPartitioners.hashCode() : 0;
    }
  }

  public static class QueueStateImpl implements QueueState {

    // we use this to mark encoded queue state in the table - when decoding we can verify the correct version
    final static int encodingVersion = 1;

    // Note: QueueStateImpl does not override equals and hashcode,
    // since in some cases we would want to include transient state in comparison and in some other cases not.

    private QueuePartitioner.PartitionerType partitioner;
    private TransientWorkingSet transientWorkingSet;
    private DequeuedEntrySet dequeueEntrySet;
    private EvictionHelper evictionHelper;
    private long consumerReadPointer = INVALID_ENTRY_ID;
    private long queueWritePointer = FIRST_QUEUE_ENTRY_ID - 1;
    private ClaimedEntryList claimedEntryList;
    private long lastEvictTimeInSecs = 0;

    private ReconfigPartitionersList reconfigPartitionersList;

    public QueueStateImpl() {
      initCollections();
    }

    private void initCollections() {
      transientWorkingSet = TransientWorkingSet.EMPTY_SET;
      dequeueEntrySet = new DequeuedEntrySet();
      evictionHelper = new EvictionHelper();
      claimedEntryList = new ClaimedEntryList();
      reconfigPartitionersList = ReconfigPartitionersList.EMPTY_RECONFIGURATION_PARTITIONERS_LIST;
    }

    public QueuePartitioner.PartitionerType getPartitioner() {
      return partitioner;
    }

    public void setPartitioner(QueuePartitioner.PartitionerType partitioner) {
      this.partitioner = partitioner;
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

    public EvictionHelper getEvictionHelper() {
      return evictionHelper;
    }

    public void setEvictionHelper(EvictionHelper evictionHelper) {
      this.evictionHelper = evictionHelper;
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

    public void clearInflight() {
      initCollections();
    }

    @Override
    public int weight() {
      return transientWorkingSet.getWeight() + 200;  // Add a small constant for other overhead
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("transientWorkingSet", transientWorkingSet)
        .add("dequeueEntrySet", dequeueEntrySet)
        .add("evictionHelper", evictionHelper)
        .add("consumerReadPointer", consumerReadPointer)
        .add("claimedEntryList", claimedEntryList)
        .add("queueWritePointer", queueWritePointer)
        .add("lastEvictTimeInSecs", lastEvictTimeInSecs)
        .add("reconfigPartitionersList", reconfigPartitionersList)
        .toString();
    }

    public void encode(Encoder encoder) throws IOException {
      // transient working set is not encoded - in case we have to reconstruct the state, we can re-read it
      // last evict time is not encoded - it is stored in a separate column
      encoder.writeInt(encodingVersion);
      partitioner.encode(encoder);
      dequeueEntrySet.encode(encoder);
      evictionHelper.encode(encoder);
      encoder.writeLong(consumerReadPointer);
      claimedEntryList.encode(encoder);
      encoder.writeLong(queueWritePointer);
      reconfigPartitionersList.encode(encoder);
    }

    public void encodeTransient(Encoder encoder) throws IOException {
      encode(encoder);
      transientWorkingSet.encode(encoder);
    }

    public static QueueStateImpl decode(Decoder decoder) throws IOException {
      // transient working set is not encoded - in case we have to reconstruct the state, we can re-read it
      // last evict time is not encoded - it is stored in a separate column
      int versionFound = decoder.readInt();
      if (versionFound != encodingVersion) {
        throw new IOException("Cannot decode QueueStateImpl with incomatible version " + versionFound);
      }
      QueueStateImpl queueState = new QueueStateImpl();
      queueState.setPartitioner(QueuePartitioner.PartitionerType.decode(decoder));
      queueState.setDequeueEntrySet(DequeuedEntrySet.decode(decoder));
      queueState.setEvictionHelper(EvictionHelper.decode(decoder));
      queueState.setConsumerReadPointer(decoder.readLong());
      queueState.setClaimedEntryList(ClaimedEntryList.decode(decoder));
      queueState.setQueueWritePointer(decoder.readLong());
      queueState.setReconfigPartitionersList(ReconfigPartitionersList.decode(decoder));
      return queueState;
    }

    public static QueueStateImpl decodeTransient(Decoder decoder) throws IOException {
      QueueStateImpl queueState = QueueStateImpl.decode(decoder);
      queueState.setTransientWorkingSet(TransientWorkingSet.decode(decoder));
      return queueState;
    }
  }

  interface DequeueStrategy {
    /**
     * method to read the queue state from storage, in case it was not passed in with a request
     */
    QueueStateImpl readQueueState(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
      throws OperationException;

    /**
     * method to read the queue state from storage or construct a new one (if called from configure)
     */
    QueueStateImpl constructQueueState(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
      throws OperationException;

    /**
     * method to fetch more entries into the queue state
     */
    void fetchNextEntries(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                          ReadPointer readPointer) throws OperationException;

    /**
     * returns the min entry that is not yet acked for a consumer. Entries equal to or greater than this entry are
     * still not dequeued/acked, and hence cannot be evicted. Entries below this can be evicted if they are acked.
     * @return min entry that is not yet acked for a consumer.
     */
    long getMinUnAckedEntry(QueueStateImpl queueState);

    /**
     * method to read queue entries specified in entryIds into queue state
     * @return  true if all entries were skipped because they are invalid or evicted. That means we have to move the
     * consumer past these entries and fetch again.
     */
    public boolean readEntries(QueueStateImpl queueState, ReadPointer readPointer, List<Long> entryIds)
      throws OperationException;
    /**
     * method to save the queue state to storage (dual to readQueueState)
     */
    void saveDequeueState(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                          ReadPointer readPointer) throws OperationException;

    /**
     * configure all consumers of a group and persist the newly constructed consumer states
     */
    void configure(List<QueueConsumer> consumers, List<QueueStateImpl> queueStates, QueueConfig config,
                   long groupId, int currentConsumerCount, int newConsumerCount, ReadPointer readPointer)
      throws OperationException;

    /**
     * Deletes the dequeue state of consumer (groupId, instanceId)
     */
    void deleteDequeueState(long groupId, int instanceId) throws OperationException;

    /**
     * Returns true if state of consumer (groupId, instanceId) exists
     */
    boolean dequeueStateExists(long groupId, int instanceId) throws OperationException;
  }

  abstract class AbstractDequeueStrategy implements DequeueStrategy {
    /**
     * If true, indicates that this is the first time this consumer is running
     */
    protected boolean init = false;

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

      OperationResult<byte[]> result =
        table.get(makeRowName(GLOBAL_LAST_EVICT_ENTRY), GLOBAL_LAST_EVICT_ENTRY, TransactionOracle.DIRTY_READ_POINTER);

      if (!result.isEmpty() && !isNullOrEmpty(result.getValue())) {
        return Bytes.toLong(result.getValue());
      }
      return INVALID_ENTRY_ID;
    }

    @Override
    public QueueStateImpl readQueueState(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
      throws OperationException {
      // Note: QueueConfig.groupSize and QueueConfig.partitioningKey will not be set when calling from configure
      return constructQueueStateInternal(consumer, config, readPointer, false);
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
                                                         ReadPointer  readPointer, boolean construct)
      throws OperationException {

      // Note: 1. QueueConfig.groupSize and QueueConfig.partitioningKey will not be set when calling from configure
      // Note: 2. We define deleted queue state as empty bytes or zero length byte array

      final byte[] rowKey = makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId());
      OperationResult<byte[]> readResult = table.get(rowKey, CONSUMER_STATE, TransactionOracle.DIRTY_READ_POINTER);
      QueueStateImpl queueState;
      if (!readResult.isEmpty() && readResult.getValue() != null && readResult.getValue().length > 0) {
        ByteArrayInputStream bin = new ByteArrayInputStream(readResult.getValue());
        BinaryDecoder decoder = new BinaryDecoder(bin);
        try {
          queueState = QueueStateImpl.decode(decoder);
        } catch (IOException e) {
          throw new OperationException(StatusCode.INTERNAL_ERROR,
                                       getLogMessage(consumer, "Exception while deserializing consumer state"), e);
        }
      } else if (construct) {
        queueState = new QueueStateImpl();
        queueState.setPartitioner(consumer.getQueueConfig().getPartitionerType());
      } else {
        throw new OperationException(
          StatusCode.NOT_CONFIGURED,
          getLogMessage(consumer,
                        String.format("Cannot find configuration for consumer %d. Is configure method called?",
                                      consumer.getInstanceId())));
      }

      // If read pointer is invalid then this the first time the consumer is running, initialize the read pointer
      if(queueState.getConsumerReadPointer() == INVALID_ENTRY_ID) {
        init = true;
        queueState.setConsumerReadPointer(getReadPointerIntialValue());
      }

      // Read queue write pointer
      long queueWritePointer = table.incrementAtomicDirtily(
        makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 0);
      queueState.setQueueWritePointer(queueWritePointer);

      // If dequeue entries present, read them from storage
      if(!queueState.getDequeueEntrySet().isEmpty()) {
        if(consumer.getStateType() == QueueConsumer.StateType.UNINITIALIZED) {
          // This is the crash recovery case, the consumer has stopped processing before acking the previous dequeues
          SortedSet<Long> droppedEntries = queueState.getDequeueEntrySet().startNewTry(MAX_CRASH_DEQUEUE_TRIES);
          if(!droppedEntries.isEmpty() && LOG.isWarnEnabled()) {
            LOG.warn(getLogMessage(consumer, String.format("Dropping entries %s after %d tries",
                                                 droppedEntries, MAX_CRASH_DEQUEUE_TRIES)));
          }
        }

        readEntries(queueState, readPointer, queueState.getDequeueEntrySet().getEntryIds());

        if(consumer.getStateType() == QueueConsumer.StateType.NOT_FOUND) {
          // If consumer state is NOT_FOUND, the previously dequeued entries will not need to be dequeued again
          queueState.getTransientWorkingSet().moveToEnd();
        }
        // If consumer state is UNINITIALIZED, any previously dequeued entries will need to be dequeued again
      }

      // Consumer state is now INITIALIZED
      consumer.setStateType(QueueConsumer.StateType.INITIALIZED);

      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(consumer, String.format("Constructed new QueueState - %s", queueState)));
      }

      // Check if the partitioner in saved state is compatible with the requested partitioning type
      if(queueState.getPartitioner() != config.getPartitionerType()) {
        throw new OperationException(
          StatusCode.INVALID_STATE,
          getLogMessage(consumer, String.format("Saved state partition type=%s, requested partition=%s for consumer %s",
                                                queueState.getPartitioner(), consumer.getQueueConfig().getPartitionerType(),
                                                consumer.getInstanceId())));
      }
      return queueState;
    }

    @Override
    public void saveDequeueState(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                                 ReadPointer readPointer) throws OperationException {
      // possibly adjust the read pointer before saving, to the max entry id that has been dequeued
      long consumerReadPointer = queueState.getConsumerReadPointer();
      if(!queueState.getDequeueEntrySet().isEmpty()) {
        long maxEntryId = queueState.getDequeueEntrySet().max().getEntryId();
        if(consumerReadPointer < maxEntryId) {
          consumerReadPointer = maxEntryId;
        }
      }
      queueState.setConsumerReadPointer(consumerReadPointer);

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Encoder encoder = new BinaryEncoder(bos);
      try {
        queueState.encode(encoder);
      } catch(IOException e) {
        throw new OperationException(StatusCode.INTERNAL_ERROR,
                                     getLogMessage(consumer, "Exception while serializing queue state"), e);
      }

      final byte[] rowKey = makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId());
      table.put(rowKey, CONSUMER_STATE, TransactionOracle.DIRTY_WRITE_VERSION, bos.toByteArray());
    }

    public void deleteDequeueState(long groupId, int instanceId) throws OperationException {
      // Delete queue state for the consumer, see notes in constructQueueStateInternal
      final byte[] rowKey = makeRowKey(CONSUMER_META_PREFIX, groupId, instanceId);
      table.put(rowKey, CONSUMER_STATE, TransactionOracle.DIRTY_WRITE_VERSION, Bytes.EMPTY_BYTE_ARRAY);
    }

    /**
     * @return  true if all entries were skipped because they are invalid or evicted. That means we have to move the
     * consumer past these entries and fetch again.
     */
    @Override
    public boolean readEntries(QueueStateImpl queueState, ReadPointer readPointer, List<Long> entryIds)
      throws OperationException
    {
      if(LOG.isTraceEnabled()) {
        LOG.trace(getLogMessage(String.format("Reading entries from storage - %s", entryIds)));
      }

      if(entryIds.isEmpty()) {
        return false;
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
        final byte[][] entryRowKeys = new byte[entryIds.size()][];
        for(int i = 0; i < entryIds.size(); ++i) {
          entryRowKeys[i] = makeRowKey(GLOBAL_DATA_PREFIX, entryIds.get(i));
        }

        final byte[][] entryColKeys = new byte[][]{ ENTRY_META, ENTRY_DATA };
        OperationResult<Map<byte[], Map<byte[], byte[]>>> entriesResult =
          table.getAllColumns(entryRowKeys, entryColKeys, readPointer);
        if (entriesResult.isEmpty()) {
          return false;
        } else {
          boolean allInvalid = true;
          for(int i = 0; i < entryIds.size(); ++i) {
            Map<byte[], byte[]> entryMap = entriesResult.getValue().get(entryRowKeys[i]);
            if(entryMap == null) {
              if (LOG.isTraceEnabled()) {
                LOG.trace(getLogMessage(
                  String.format("Entry with entryId %d does not exist yet. Treating this as end of queue.",
                                entryIds.get(i))));
              }
              return false;
            }
            byte[] entryMetaBytes = entryMap.get(ENTRY_META);
            if(entryMetaBytes == null) {
              if (LOG.isTraceEnabled()) {
                LOG.trace(getLogMessage(
                  String.format("No entry meta data found for existing entryId %d. Treating this as end of queue.",
                                entryIds.get(i))));
              }
              return false;
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
              allInvalid = false;
            }
          }
          return allInvalid;
        }
      } finally {
        // Update queue state
        queueState.setTransientWorkingSet(new TransientWorkingSet(readEntryIds, newCachedEntries));
      }
    }

    @Override
    public void fetchNextEntries(QueueConsumer consumer, QueueConfig config,
                                       QueueStateImpl queueState, ReadPointer readPointer) throws OperationException {

      while (!queueState.getTransientWorkingSet().hasNext()) {
        List<Long> nextEntryIds = claimNextEntries(consumer, config, queueState, readPointer);
        if (nextEntryIds.isEmpty()) {
          return;
        }
        boolean allInvalid = readEntries(queueState, readPointer, nextEntryIds);
        if (allInvalid) {
          // all the entries returned by the dequeue strategy are invalid.
          // we must ignore them and remember that in the queue state, and try again
          ignoreInvalidEntries(queueState, nextEntryIds);
          continue;
        }
        // we either found no more entries (not even invalid ones), or we have at least one valid entry.
        break;
      }
    }

    @Override
    public boolean dequeueStateExists(long groupId, int instanceId) throws OperationException {
      final byte[] rowKey = makeRowKey(CONSUMER_META_PREFIX, groupId, instanceId);
      OperationResult<byte[]> readResult = table.get(rowKey, CONSUMER_STATE, TransactionOracle.DIRTY_READ_POINTER);
      return !readResult.isEmpty() && readResult.getValue() != null && readResult.getValue().length > 0;
    }

    /**
     * claim the next batch of entries from the queue using the partitioner, without reading the entry data yet
     */
    protected abstract List<Long> claimNextEntries(QueueConsumer consumer, QueueConfig config,
                                                   QueueStateImpl queueState, ReadPointer readPointer)
      throws OperationException;

    /**
     * mark the given entry ids as consumed, so that will not be claimed subsequently
     */
    protected abstract void ignoreInvalidEntries(QueueStateImpl queueState, List<Long> invalidEntryIds)
      throws OperationException;
  }

  /**
   * A dequeue strategy that supports only 2 operations - dequeueStateExists and deleteDequeueState
   */
  class NoDequeueStrategy extends AbstractDequeueStrategy implements DequeueStrategy {
    @Override
    protected List<Long> claimNextEntries(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                                          ReadPointer readPointer) throws OperationException {
      throw getException();
    }

    @Override
    protected void ignoreInvalidEntries(QueueStateImpl queueState, List<Long> invalidEntryIds)
      throws OperationException {
      throw getException();
    }

    @Override
    public long getMinUnAckedEntry(QueueStateImpl queueState) {
      return TTQueueNewConstants.FIRST_ENTRY_ID - 1;
    }

    @Override
    public void configure(List<QueueConsumer> consumers, List<QueueStateImpl> queueStates, QueueConfig config,
                          long groupId, int currentConsumerCount, int newConsumerCount, ReadPointer readPointer)
      throws OperationException {
      throw getException();
    }

    @Override
    protected long getReadPointerIntialValue() throws OperationException {
      throw getException();
    }

    @Override
    protected long getLastEvictEntry() throws OperationException {
      throw getException();
    }

    @Override
    public QueueStateImpl readQueueState(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
      throws OperationException {
      throw getException();
    }

    @Override
    public QueueStateImpl constructQueueState(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
      throws OperationException {
      throw getException();
    }

    @Override
    protected QueueStateImpl constructQueueStateInternal(QueueConsumer consumer, QueueConfig config,
                                                         ReadPointer readPointer, boolean construct)
      throws OperationException {
      throw getException();
    }

    @Override
    public void saveDequeueState(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                                 ReadPointer readPointer) throws OperationException {
      throw getException();
    }

    @Override
    public boolean readEntries(QueueStateImpl queueState, ReadPointer readPointer, List<Long> entryIds)
      throws OperationException {
      throw getException();
    }

    @Override
    public void fetchNextEntries(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                                 ReadPointer readPointer) throws OperationException {
      throw getException();
    }

    private RuntimeException getException() {
      return new UnsupportedOperationException(
        getLogMessage(String.format("%s does not support this operation!",
                                    NoDequeueStrategy.class.getCanonicalName())));
    }
  }

  abstract class AbstractDisjointDequeueStrategy extends AbstractDequeueStrategy implements DequeueStrategy {
    @Override
    public long getMinUnAckedEntry(QueueStateImpl queueState) {
      // Min unacked entry is min(min(dequeue entry), consumerReadPointer) for disjoint dequeue strategy
      long dEntry = queueState.getDequeueEntrySet().isEmpty() ?
        Long.MAX_VALUE : queueState.getDequeueEntrySet().min().getEntryId();

      return Math.min(dEntry, queueState.getConsumerReadPointer());
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

      DequeueStrategy dequeueStrategy = getDequeueStrategy(config.getPartitionerType());

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
        saveDequeueState(consumer, config, queueState, readPointer);
      }

      // Delete queue state for removed consumers, if any
      for(int j = newConsumerCount; j < currentConsumerCount; ++j) {
        QueueConsumer consumer = currentConsumers.get(j);
        deleteDequeueState(consumer.getGroupId(), consumer.getInstanceId());
      }
    }

    @Override
    protected void ignoreInvalidEntries(QueueStateImpl queueState, List<Long> invalidEntryIds) throws OperationException {
      queueState.setConsumerReadPointer(invalidEntryIds.get(invalidEntryIds.size() - 1));
    }
  }

  class HashDequeueStrategy extends AbstractDisjointDequeueStrategy implements DequeueStrategy {
    @Override
    public List<Long> claimNextEntries(
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
          long queueWritePointer = table.incrementAtomicDirtily(
            makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 0);
          queueState.setQueueWritePointer(queueWritePointer);
          if(LOG.isTraceEnabled()) {
            LOG.trace(getLogMessage(consumer, String.format("New queueWritePointer = %d", queueWritePointer)));
          }
          // If still no progress, return empty queue
          if(entryId >= queueState.getQueueWritePointer()) {
            return Collections.emptyList();
          }
        }

        final int batchSize = getBatchSize(config);
        long startEntryId = entryId + 1;
        long endEntryId =
                startEntryId + (batchSize * consumer.getGroupSize()) < queueState.getQueueWritePointer() ?
                    startEntryId + (batchSize * consumer.getGroupSize()) : queueState.getQueueWritePointer();

        // Read  header data from underlying storage, if any
        final int cacheSize = (int)(endEntryId - startEntryId + 1);
        final String partitioningKey = consumer.getPartitioningKey();
        if(partitioningKey == null || partitioningKey.isEmpty()) {
          throw new OperationException(StatusCode.INTERNAL_ERROR,
            getLogMessage(consumer, "Using Hash Partitioning with null/empty partitioningKey."));
        }
        final byte [][] rowKeys = new byte[cacheSize][];
        for(int id = 0; id < cacheSize; ++id) {
          rowKeys[id] = makeRowKey(GLOBAL_DATA_PREFIX, startEntryId + id);
        }
        final byte[][] columnKeys = { ENTRY_HEADER };
        OperationResult<Map<byte[], Map<byte[], byte[]>>> headerResult =
          table.getAllColumns(rowKeys, columnKeys, readPointer);

        // Determine which entries  need to be read from storage
        for(int id = 0; id < cacheSize; ++id) {
          if (newEntryIds.size() >= batchSize) {
            break;
          }
          final long currentEntryId = startEntryId + id;
          if (!headerResult.isEmpty()) {
            Map<byte[], Map<byte[], byte[]>> headerValue = headerResult.getValue();
            Map<byte[], byte[]> headerMap = headerValue.get(rowKeys[id]);
            byte[] hashBytes = headerMap == null ? null : headerMap.get(ENTRY_HEADER);
            if (hashBytes == null) {
              // every existing entry has hashBytes. If it is null, we have reached the end of queue
              break outerLoop;
            }
            Map<String, Integer> hashKeyMap;
            try {
              hashKeyMap = QueueEntry.deserializeHashKeys(hashBytes);
            } catch(IOException e) {
              throw new OperationException(StatusCode.INTERNAL_ERROR,
                                           getLogMessage(consumer, "Exception while deserializing hash keys"), e);
            }
            Integer hashValue = hashKeyMap.get(partitioningKey);
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
    public List<Long> claimNextEntries(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                                       ReadPointer readPointer) throws OperationException {
      long entryId = queueState.getConsumerReadPointer();
      QueuePartitioner partitioner=config.getPartitionerType().getPartitioner();
      List<Long> newEntryIds = new ArrayList<Long>();

      while (newEntryIds.isEmpty()) {
        if(entryId >= queueState.getQueueWritePointer()) {
          // Reached the end of queue as per cached QueueWritePointer,
          // read it again to see if there is any progress made by producers
          long queueWritePointer = table.incrementAtomicDirtily(
            makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 0);
          queueState.setQueueWritePointer(queueWritePointer);
          // If still no progress, return empty queue
          if(entryId >= queueState.getQueueWritePointer()) {
            return Collections.emptyList();
          }
        }

        final int batchSize = getBatchSize(config);
        long startEntryId = entryId + 1;
        long endEntryId =
                  startEntryId + (batchSize * consumer.getGroupSize()) < queueState.getQueueWritePointer() ?
                  startEntryId + (batchSize * consumer.getGroupSize()) : queueState.getQueueWritePointer();

        final int cacheSize = (int)(endEntryId - startEntryId + 1);

        // Determine which entries  need to be read from storage
        for(int id = 0; id < cacheSize; ++id) {
          if (newEntryIds.size() >= batchSize) {
            break;
          }
          final long currentEntryId = startEntryId + id;
          if (partitioner.shouldEmit(consumer.getGroupSize(), consumer.getInstanceId(), currentEntryId, null)
            && queueState.getReconfigPartitionersList()
              .shouldEmit(consumer.getGroupSize(), consumer.getInstanceId(), currentEntryId, null)) {
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
                                                         ReadPointer readPointer, boolean construct)
      throws OperationException {
      QueueStateImpl queueState = super.constructQueueStateInternal(consumer, config, readPointer, construct);

      if(init) {
        // This is the first time a consumer is running, it could be the first time any consumer of the group is running
        // Make sure the GROUP_READ_POINTER is valid, i.e, not less than GLOBAL_LAST_EVICT_ENTRY

        // Fetch the group read pointer
        final byte[] rowKey = makeRowKey(GROUP_READ_POINTER, consumer.getGroupId());
        long groupReadPointer = table.incrementAtomicDirtily(rowKey, GROUP_READ_POINTER, 0);

        long lastEvictEntry = getLastEvictEntry();
        if(lastEvictEntry != INVALID_ENTRY_ID && groupReadPointer < lastEvictEntry) {
          // Entries have been evicted. Set GROUP_READ_POINTER to GLOBAL_LAST_EVICT_ENTRY
          table.compareAndSwapDirty(rowKey, GROUP_READ_POINTER, Bytes.toBytes(groupReadPointer),
                                    Bytes.toBytes(lastEvictEntry));
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

      ClaimedEntryRange firstClaimed = queueState.getClaimedEntryList().getClaimedEntry();
      if (firstClaimed.isValid() && firstClaimed.getBegin() < queueState.getConsumerReadPointer()) {
        queueState.setConsumerReadPointer(firstClaimed.getBegin());
      }

      super.saveDequeueState(consumer, config, queueState, readPointer);
    }

    /**
     * Returns the group read pointer for the consumer.
     * @return group read pointer
     * @throws OperationException
     */
    private long getGroupReadPointer(QueueConsumer consumer) throws OperationException {
      // Fetch the group read pointer
      final byte[] rowKey = makeRowKey(GROUP_READ_POINTER, consumer.getGroupId());

      return table.incrementAtomicDirtily(rowKey, GROUP_READ_POINTER, 0);
    }

    @Override
    public List<Long> claimNextEntries(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState,
                                       ReadPointer readPointer) throws OperationException {
      // Determine the next batch of entries that can be dequeued by this consumer
      List<Long> newEntryIds = new ArrayList<Long>();

      // If claimed entries exist, return them. This can happen when the queue cache is lost due to consumer
      // crash or other reasons
      ClaimedEntryRange claimedEntryRange = queueState.getClaimedEntryList().getClaimedEntry();
      if(claimedEntryRange.isValid()) {
        for(long i = claimedEntryRange.getBegin(); i <= claimedEntryRange.getEnd(); ++i) {
          newEntryIds.add(i);
        }
        return newEntryIds;
      }

      // Else claim new queue entries
      final int batchSize = getBatchSize(config);
      while (newEntryIds.isEmpty()) {
        // Fetch the group read pointer
        long groupReadPointer = getGroupReadPointer(consumer);
        if(groupReadPointer + batchSize >= queueState.getQueueWritePointer()) {
          // Reached the end of queue as per cached QueueWritePointer,
          // read it again to see if there is any progress made by producers
          long queueWritePointer = table.incrementAtomicDirtily(
            makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 0);
          queueState.setQueueWritePointer(queueWritePointer);
        }

        // End of queue reached
        if(groupReadPointer >= queueState.getQueueWritePointer()) {
          return Collections.emptyList();
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
          newEntryIds.add(currentEntryId);
        }
      }
      return newEntryIds;
    }

    @Override
    protected void ignoreInvalidEntries(QueueStateImpl queueState, List<Long> invalidEntryIds)
      throws OperationException {
      queueState.getClaimedEntryList().moveForwardTo(invalidEntryIds.get(invalidEntryIds.size() - 1) + 1);
    }

    @Override
    public long getMinUnAckedEntry(QueueStateImpl queueState) {
      // min unacked entry is min(min(dequeue entry set), min(claimed entries), consumerReadPointer)
      long dEntry = queueState.getDequeueEntrySet().isEmpty() ?
        Long.MAX_VALUE : queueState.getDequeueEntrySet().min().getEntryId();
      long cEntry = queueState.getClaimedEntryList().getClaimedEntry() == ClaimedEntryRange.INVALID ?
        Long.MAX_VALUE : queueState.getClaimedEntryList().getClaimedEntry().getBegin();

      return Math.min(dEntry, Math.min(cEntry, queueState.getConsumerReadPointer()));
    }

    @Override
    public void configure(List<QueueConsumer> currentConsumers, List<QueueStateImpl> queueStates,
                          QueueConfig config, final long groupId, final int currentConsumerCount,
                          final int newConsumerCount, ReadPointer readPointer) throws OperationException {
      if(newConsumerCount >= currentConsumerCount) {
        DequeueStrategy dequeueStrategy = getDequeueStrategy(config.getPartitionerType());
        for(int i = currentConsumerCount; i < newConsumerCount; ++i) {
          StatefulQueueConsumer consumer = new StatefulQueueConsumer(i, groupId, newConsumerCount, config);
          QueueStateImpl queueState = dequeueStrategy.constructQueueState(consumer, config, readPointer);
          consumer.setQueueState(queueState);
          saveDequeueState(consumer, config, queueState, readPointer);
        }
        return;
      }

      if(currentConsumers.size() != currentConsumerCount) {
        throw new OperationException(
          StatusCode.INTERNAL_ERROR,
          getLogMessage(String.format("Size of passed in consumer list (%d) is not equal to currentConsumerCount (%d)",
                                      currentConsumers.size(), currentConsumerCount)));
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
        saveDequeueState(currentConsumers.get(i), config, queueStates.get(i), readPointer);
      }

      // Delete the state of removed consumers
      for(int i = newConsumerCount; i < currentConsumerCount; ++i) {
        QueueConsumer c = currentConsumers.get(i);
        deleteDequeueState(c.getGroupId(), c.getInstanceId());
      }
    }
  }

  /**
   * TTQueueNewIterator implements Iterator interface to provide iteration on top of table Scanner.
   * remove functionality is not implemented
   */
  @SuppressWarnings("UnusedDeclaration")
  private static class TTQueueNewIterator implements Iterator<QueueEntry> {

    private final Scanner scanner;
    private boolean peeked;
    private QueueEntry currentQueueEntry;
    /**
     * Construct new TTQueueNewIterator taking in Table Scanner as a parameter
     * @param scanner Table scanner
     */
    private TTQueueNewIterator(Scanner scanner) {
      this.scanner = scanner;
      this.peeked = false;
      this.currentQueueEntry = null;
    }

    private QueueEntry getNextValidQueueEntry() throws IOException {
      QueueEntry entry = null;
      if (scanner == null) {
        return null;
      }
      //Get the first Entry that is Valid. If not found return null;
      boolean done  = false;
      while (!done){
        ImmutablePair<byte[], Map<byte[], byte[]>> value = scanner.next();
        if (value == null ) {
          done  = true;
        }  else {
           byte [] entryMetaValue = value.getSecond().get(ENTRY_META);
           byte [] queueEntryBytes = value.getSecond().get(GLOBAL_DATA_PREFIX);
           boolean validEntryMeta  = (entryMetaValue != null) && (Arrays.equals(entryMetaValue, ENTRY_META_VALID));
           if ( validEntryMeta && (queueEntryBytes !=null) ) {
               entry  = new QueueEntry(queueEntryBytes);
               done = true;
             }
        }
      }
      return entry;
    }

    @Override
    public boolean hasNext() {
      if(!peeked) {
        peeked =true;
        try {
          currentQueueEntry = getNextValidQueueEntry();
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
      return (currentQueueEntry != null);
    }

    @Override
    public QueueEntry next() {
      try{
        if(peeked) {
          peeked = false;
          return currentQueueEntry;
        } else {
          return getNextValidQueueEntry();
        }
      } catch (IOException e) {
        throw Throwables.propagate(e);
      } finally {
        currentQueueEntry = null;
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported in this iterator");
    }
  }

}
