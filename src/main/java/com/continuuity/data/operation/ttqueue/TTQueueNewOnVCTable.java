package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.ttqueue.internal.CachedList;
import com.continuuity.data.operation.ttqueue.internal.EntryConsumerMeta;
import com.continuuity.data.operation.ttqueue.internal.EntryMeta;
import com.continuuity.data.table.VersionedColumnarTable;
import com.google.common.base.Objects;
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
    row-key       | column | value
    <queueName>10 | 10     | <entryId>

    data and meta data (=entryState) for each entry (together in one row per entry)
    (GLOBAL_DATA_PREFIX)
    row-key                | column | value
    <queueName>20<entryId> | 20     | <data>
                           | 10     | <entryState>
                           | 30     | <header data>

  for each group of consumers (= each group of flowlet instances):
    group read pointer for highest entry id processed by group of consumers
    row-key                | column | value
    <queueName>10<groupId> | 10     | <entryId>

  for each consumer(=flowlet instance)
    state of entry ids processed by consumer (one column per entry id), current active entry and consumer read pointer
    (CONSUMER_META_PREFIX)
    row-key                            | column          | value
    <queueName>30<groupId><consumerId> | 20<entryId>     | <entryState>
                                       | 10              | <entryId>
                                       | 30              | <entryId>
   */

  // Row prefix names and flags
  static final byte [] GLOBAL_ENTRY_ID_PREFIX = {10};  //row <queueName>10
  static final byte [] GLOBAL_DATA_PREFIX = {20};   //row <queueName>20
  static final byte [] CONSUMER_META_PREFIX = {30}; //row <queueName>30

  // Columns for row = GLOBAL_ENTRY_ID_PREFIX
  static final byte [] GLOBAL_ENTRYID_COUNTER = {10};  //newest (highest) entry id per queue (global)

  // Columns for row = GLOBAL_DATA_PREFIX
  static final byte [] ENTRY_META = {10}; //row  <queueName>20<entryId>, column 10
  static final byte [] ENTRY_DATA = {20}; //row  <queueName>20<entryId>, column 20
  static final byte [] ENTRY_HEADER = {30};  //row  <queueName>20<entryId>, column 30

  static final byte [] GROUP_READ_POINTER = {10}; //row <queueName>10<groupId>, column 10

  // Columns for row = CONSUMER_META_PREFIX
  static final byte [] ACTIVE_ENTRY = {10};          //row <queueName>30<groupId><consumerId>, column 10
  static final byte [] META_ENTRY_PREFIX = {20};     //row <queueName>30<groupId><consumerId>, column 20<entryId>
  static final byte [] CONSUMER_READ_POINTER = {30}; //row <queueName>30<groupId><consumerId>, column 30
  static final byte [] ACTIVE_ENTRY_CRASH_TRIES = {40};

  static final long INVALID_ENTRY_ID = -1;
  static final byte[] INVALID_ENTRY_ID_BYTES = Bytes.toBytes(INVALID_ENTRY_ID);
  static final long FIRST_QUEUE_ENTRY_ID = 1;

  protected TTQueueNewOnVCTable(VersionedColumnarTable table, byte[] queueName, TransactionOracle oracle,
                                final CConfiguration conf) {
    this.table = table;
    this.queueName = queueName;
    this.oracle = oracle;
  }

  @Override
  public EnqueueResult enqueue(QueueEntry entry, long cleanWriteVersion) throws OperationException {
    byte[] data = entry.getData();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Enqueueing (data.len=" + data.length + ", writeVersion=" + cleanWriteVersion + ")");
    }

    // Get our unique entry id
    long entryId;
    try {
      // Make sure the increment below uses increment operation of the underlying implementation directly
      // so that it is atomic (Eg. HBase increment operation)
      entryId = this.table.incrementAtomicDirtily(makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 1);
    } catch (OperationException e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, "Increment " +
        "of global entry id failed with status code " + e.getStatus() + ": " + e.getMessage(), e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("New enqueue got entry id " + entryId);
    }

    /*
    Insert entry with version=<cleanWriteVersion> and
    row-key = <queueName>20<entryId> , column/value 20/<data> and 10/EntryState.VALID
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
    if(LOG.isDebugEnabled()) {
      LOG.debug(String.format("Invalidating entry ", entryPointer.getEntryId()));
    }
    final byte [] rowName = makeRowKey(GLOBAL_DATA_PREFIX, entryPointer.getEntryId());
    // Change meta data to INVALID
    this.table.put(rowName, ENTRY_META,
                   cleanWriteVersion, new EntryMeta(EntryMeta.EntryState.INVALID).getBytes());
    // No need to delete data/headers since they will be cleaned up during eviction later
    if (LOG.isDebugEnabled()) {
      LOG.debug("Invalidated " + entryPointer);
    }
  }

  @Override
  public DequeueResult dequeue(QueueConsumer consumer, ReadPointer readPointer) throws OperationException {
    final QueueConfig config = consumer.getQueueConfig();
    if (LOG.isDebugEnabled())
      LOG.debug("Attempting dequeue [curNumDequeues=" + this.dequeueReturns.get() +
                  "] (" + consumer + ", " + config + ", " + readPointer + ")");

    // Determine what dequeue strategy to use based on the partitioner
    final QueuePartitioner queuePartitioner = config.getPartitionerType().getPartitioner();
    DequeueStrategy dequeueStrategy;
    if(queuePartitioner instanceof QueuePartitioner.HashPartitioner) {
      dequeueStrategy = new HashDequeueStrategy();
    } else if(queuePartitioner instanceof QueuePartitioner.RoundRobinPartitioner) {
      dequeueStrategy = new RoundRobinDequeueStrategy();
    } else if(queuePartitioner instanceof QueuePartitioner.FifoPartitioner) {
      dequeueStrategy = new FifoDequeueStrategy();
    } else {
      throw new OperationException(StatusCode.INTERNAL_ERROR,
        String.format("Cannot figure out the dequeue strategy to use for partitioner %s", queuePartitioner.getClass()));
    }

    // If QueueState is null, read the queue state from underlying storage.
    QueueStateImpl queueState = getQueueStateImpl(consumer.getQueueState());
    if(queueState == null) {
      queueState = dequeueStrategy.constructQueueState(consumer, config, readPointer);
      consumer.setQueueState(queueState);
    }

    // If the previous entry was not acked, return the same one (Note: will need to change for async mode)
    if(queueState.getActiveEntryId() != INVALID_ENTRY_ID) {
      if(!queueState.getCachedEntries().hasCurrent()) {
        throw new OperationException(StatusCode.INTERNAL_ERROR, "Queue state error - cannot fetch active entry id from cached entries");
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("End of queue reached using " + "read pointer " + readPointer);
      }
      dequeueStrategy.saveDequeueState(consumer, config, queueState, readPointer);
      DequeueResult dequeueResult = new DequeueResult(DequeueResult.DequeueStatus.EMPTY);
      return dequeueResult;
    }
  }

  private void readEntries(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState, ReadPointer readPointer,
                            List<Long> entryIds) throws OperationException{
    if(LOG.isDebugEnabled()) {
      LOG.debug(String.format("Reading entries from storage - ", Arrays.toString(entryIds.toArray())));
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
        if (LOG.isDebugEnabled()) {
          LOG.debug("entryId:" + entryIds.get(i) + ". entryMeta : " + entryMeta.toString());
        }

        // Check if entry has been invalidated or evicted
        if (entryMeta.isInvalid() || entryMeta.isEvicted()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found invalidated or evicted entry at " + entryIds.get(i) +
                        " (" + entryMeta.toString() + ")");
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
    ackInternal(entryPointer, consumer, readPointer);
    if(consumer.isStateful()) {
      QueueStateImpl queueState = getQueueStateImpl(consumer.getQueueState());
      queueState.setActiveEntryId(INVALID_ENTRY_ID);
      queueState.setActiveEntryTries(0);
    }
  }

  public void ackInternal(QueueEntryPointer entryPointer, QueueConsumer consumer, ReadPointer readPointer)
    throws OperationException {
    // TODO: 1. Later when active entry can saved in memory, there is no need to write it into HBase
    // TODO: 2. Need to treat Ack as a simple write operation so that it can use a simple write rollback for unack
    // TODO: 3. Use Transaction.getWriteVersion instead ReadPointer
    QueuePartitioner partitioner=consumer.getQueueConfig().getPartitionerType().getPartitioner();

    if (partitioner.isDisjoint()) {
      this.table.put(makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()),
                     new byte[][] {makeColumnName(META_ENTRY_PREFIX, entryPointer.getEntryId()), ACTIVE_ENTRY, ACTIVE_ENTRY_CRASH_TRIES},
                     readPointer.getMaximum(),
                     new byte[][] {new EntryConsumerMeta(EntryConsumerMeta.EntryState.ACKED, 0).getBytes(), INVALID_ENTRY_ID_BYTES, Bytes.toBytes(0)});
    } else {

      byte[] rowKey = makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId());
      byte[] colKey = makeColumnName(META_ENTRY_PREFIX, entryPointer.getEntryId());
      byte[] acked = new EntryConsumerMeta(EntryConsumerMeta.EntryState.ACKED, 0).getBytes();

      // verify it is not acked yet
      OperationResult<byte[]> result = this.table.get(rowKey, colKey, readPointer);
      if (result.isEmpty()) {
        throw new OperationException(StatusCode.ILLEGAL_ACK, "Entry has never been claimed. ");
      }
      EntryConsumerMeta meta = EntryConsumerMeta.fromBytes(result.getValue());
      if (!meta.isClaimed()) {
        throw new OperationException(StatusCode.ILLEGAL_ACK, "Entry is " + meta.getState().name());
      }
      // now put the new value
      this.table.put(rowKey,
                     new byte[][] { colKey, ACTIVE_ENTRY },
                     readPointer.getMaximum(),
                     new byte[][] { acked, INVALID_ENTRY_ID_BYTES} );
    }
  }

  @Override
  public void finalize(QueueEntryPointer entryPointer, QueueConsumer consumer, int totalNumGroups, long writePoint)
    throws OperationException {
    // TODO: Evict queue entries
  }

  @Override
  public void unack(QueueEntryPointer entryPointer, QueueConsumer consumer, ReadPointer readPointer)
    throws OperationException {
    // TODO: 1. Later when active entry can saved in memory, there is no need to write it into HBase
    // TODO: 2. Need to treat Ack as a simple write operation so that it can use a simple write rollback for unack
    // TODO: 3. Ack gets rolled back with tries=0. Need to fix this by fixing point 2 above.
    this.table.put(makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()),
      new byte[][] {makeColumnName(META_ENTRY_PREFIX, entryPointer.getEntryId()), ACTIVE_ENTRY, ACTIVE_ENTRY_CRASH_TRIES},
      readPointer.getMaximum(),
      new byte[][] {new EntryConsumerMeta(EntryConsumerMeta.EntryState.CLAIMED, 0).getBytes(),
                                                         Bytes.toBytes(entryPointer.getEntryId()), Bytes.toBytes(0)});
    if(consumer.isStateful()) {
      getQueueStateImpl(consumer.getQueueState()).setActiveEntryId(entryPointer.getEntryId());
    }
  }
  static long groupId = 0;
  @Override
  public long getGroupID() throws OperationException {
    // TODO: implement this :)
    return ++groupId;
  }

  @Override
  public QueueAdmin.QueueInfo getQueueInfo() throws OperationException {
    // TODO: implement this :)
    return null;
  }

  private static QueueStateImpl getQueueStateImpl(QueueState queueState) throws OperationException {
    if(queueState == null) {
      return null;
    }
    if(! (queueState instanceof QueueStateImpl)) {
      throw new OperationException(StatusCode.INTERNAL_ERROR,
                                   String.format("Don't know how to use QueueState class %s", queueState.getClass()));
    }
    return (QueueStateImpl) queueState;
  }

  protected ImmutablePair<ReadPointer, Long> dirtyPointer() {
    return new ImmutablePair<ReadPointer,Long>(oracle.dirtyReadPointer(), oracle.dirtyWriteVersion());
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

  public static class QueueStateImpl implements QueueState {
    private long activeEntryId = INVALID_ENTRY_ID;
    private int activeEntryTries = 0;
    private long consumerReadPointer = FIRST_QUEUE_ENTRY_ID - 1;
    private long queueWrtiePointer = FIRST_QUEUE_ENTRY_ID - 1;
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

    public long getQueueWritePointer() {
      return queueWrtiePointer;
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
        .add("queueWritePointer", queueWrtiePointer)
        .add("cachedEntries", cachedEntries.toString())
        .toString();
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

  abstract class AbstractDisjointDequeueStrategy implements DequeueStrategy {
    @Override
    public QueueStateImpl constructQueueState(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
                                                       throws OperationException {
      // ACTIVE_ENTRY contains the entry if any that is dequeued, but not acked
      // CONSUMER_READ_POINTER + 1 points to the next entry that can be read by this queue consuemr
      QueueStateImpl queueState = new QueueStateImpl();
      OperationResult<Map<byte[], byte[]>> stateBytes =
        table.get(makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()),
                       new byte[][] {ACTIVE_ENTRY, ACTIVE_ENTRY_CRASH_TRIES, CONSUMER_READ_POINTER}, readPointer);
      if(!stateBytes.isEmpty()) {
        queueState.setActiveEntryId(Bytes.toLong(stateBytes.getValue().get(ACTIVE_ENTRY)));
        queueState.setActiveEntryTries(Bytes.toInt(stateBytes.getValue().get(ACTIVE_ENTRY_CRASH_TRIES)));

        byte[] consumerReadPointerBytes = stateBytes.getValue().get(CONSUMER_READ_POINTER);
        if(consumerReadPointerBytes != null) {
          queueState.setConsumerReadPointer(Bytes.toLong(consumerReadPointerBytes));
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
          queueState.getCachedEntries().getNext();
        } else {
          // TODO: what do we do with the active entry?
          if(LOG.isDebugEnabled()) {
            LOG.debug(String.format("Ignoring dequeue of entry %d after %d tries", queueState.getActiveEntryId(), MAX_CRASH_DEQUEUE_TRIES));
          }
          queueState.setActiveEntryId(INVALID_ENTRY_ID);
          queueState.setActiveEntryTries(0);
        }
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug(String.format("Constructed new QueueState - %s", queueState));
      }
      return queueState;
    }

    @Override
    public void saveDequeueState(QueueConsumer consumer, QueueConfig config, QueueStateImpl queueState, ReadPointer readPointer) throws OperationException {
      // Persist the entryId this consumer will be working on
      // TODO: Later when active entry can saved in memory, there is no need to write it into HBase -> (not true for FIFO!)
      long entryId = queueState.getActiveEntryId();

      table.put(makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()),
                     new byte[][]{
                       CONSUMER_READ_POINTER,
                       ACTIVE_ENTRY, ACTIVE_ENTRY_CRASH_TRIES,
                       makeColumnName(META_ENTRY_PREFIX, entryId)
                     },
                     readPointer.getMaximum(),
                     new byte[][]{
                       Bytes.toBytes(queueState.getConsumerReadPointer()),
                       Bytes.toBytes(queueState.getActiveEntryId()),
                       Bytes.toBytes(queueState.getActiveEntryTries()),
                       new EntryConsumerMeta(EntryConsumerMeta.EntryState.CLAIMED, 0).getBytes()
                     }
      );
    }
  }

  class HashDequeueStrategy extends AbstractDisjointDequeueStrategy implements DequeueStrategy {
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
          if(LOG.isDebugEnabled()) {
            LOG.debug(String.format("New queueWritePointer = %d", queueWritePointer));
          }
          // If still no progress, return empty queue
          if(entryId >= queueState.getQueueWritePointer()) {
            return Collections.EMPTY_LIST;
          }
        }

        long startEntryId = entryId + 1;
        long endEntryId =
                startEntryId + (config.getBatchSize() * consumer.getGroupSize()) < queueState.getQueueWritePointer() ?
                    startEntryId + (config.getBatchSize() * consumer.getGroupSize()) : queueState.getQueueWritePointer();

        // Read  header data from underlying storage, if any
        final int cacheSize = (int)(endEntryId - startEntryId + 1);
        final String partitioningKey = consumer.getPartitioningKey();
        if(partitioningKey == null || partitioningKey.isEmpty()) {
          throw new OperationException(StatusCode.INTERNAL_ERROR, String.format("Using Hash Partitioning with null/empty partitioningKey!"));
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

  class RoundRobinDequeueStrategy extends AbstractDisjointDequeueStrategy implements DequeueStrategy {
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

        long startEntryId = entryId + 1;
        long endEntryId =
                  startEntryId + (config.getBatchSize() * consumer.getGroupSize()) < queueState.getQueueWritePointer() ?
                  startEntryId + (config.getBatchSize() * consumer.getGroupSize()) : queueState.getQueueWritePointer();

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

  class FifoDequeueStrategy extends AbstractDisjointDequeueStrategy implements DequeueStrategy {
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

        // For now non-disjoint partitions use batchSize = 1
        int tempBatchSize = 1;
        long endEntryId = table.incrementAtomicDirtily(makeRowKey(GROUP_READ_POINTER, consumer.getGroupId()),
                                                       GROUP_READ_POINTER, tempBatchSize);
        long startEntryId = endEntryId;
        // Note: incrementing GROUP_READ_POINTER, and storing the ActiveEntryId in HBase ideally need to happen atomically.
        //       HBase doesn't support atomic increment and put.
        //       Also, for performance reasons we have moved the write to method saveDequeueEntryState where all writes for a dequeue happen
        queueState.setActiveEntryId(endEntryId);

        // Read  header data from underlying storage, if any
        final int cacheSize = (int)(endEntryId - startEntryId + 1);

        // Determine which entries  need to be read from storage based on partition type
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
}
