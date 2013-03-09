package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.operation.ttqueue.internal.CachedList;
import com.continuuity.data.operation.ttqueue.internal.EntryConsumerMeta;
import com.continuuity.data.operation.ttqueue.internal.EntryMeta;
import com.continuuity.data.operation.ttqueue.internal.QueuePartitionMapSerializer;
import com.continuuity.data.table.VersionedColumnarTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class TTQueueNewOnVCTable implements TTQueue {

  protected final VersionedColumnarTable table;
  private final byte [] queueName;
  final TimestampOracle timeOracle;

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

  static final long INVALID_ENTRY_ID = -1;
  static final byte[] INVALID_ENTRY_ID_BYTES = Bytes.toBytes(INVALID_ENTRY_ID);
  static final long FIRST_QUEUE_ENTRY_ID = 1;

  protected TTQueueNewOnVCTable(VersionedColumnarTable table, byte[] queueName, TimestampOracle timeOracle,
                                final CConfiguration conf) {
    this.table = table;
    this.queueName = queueName;
    this.timeOracle = timeOracle;
  }

//  protected abstract long fetchNextEntryId(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
//    throws OperationException;

  protected long fetchNextEntryId(QueueConsumer consumer, QueueConfig config, QueueState queueState,
                                  ReadPointer readPointer) throws OperationException {
//    return this.table.incrementAtomicDirtily(
//      makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()),
//      CONSUMER_READ_POINTER, consumer.getGroupSize());
    long entryId = queueState.getConsumerReadPointer();
    boolean foundEntry=false;
    QueuePartitioner partitioner=config.getPartitionerType().getPartitioner();

    while (!foundEntry) {
      if(entryId >= queueState.getQueueWritePointer()) {
        // Reached the end of queue as per cached QueueWritePointer,
        // read it again to see if there is any progress made by producers
        // TODO: use raw get insead of the workaround of incrementing zero
        long queueWritePointer = this.table.incrementAtomicDirtily(makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 0);
        queueState.setQueueWritePointer(queueWritePointer);
        // If still no progress, return empty queue
        if(entryId >= queueState.getQueueWritePointer()) {
          return INVALID_ENTRY_ID;
        }
      }

      if (partitioner.isDisjoint()) {
        ++entryId;
      } else {
        entryId = this.table.incrementAtomicDirtily(makeRowKey(GROUP_READ_POINTER, consumer.getGroupId()),
                                                    GROUP_READ_POINTER, 1);
      }
      if (partitioner.usesHeaderData()) {
        OperationResult<byte[]> result = this.table.get(makeRowKey(GLOBAL_DATA_PREFIX, entryId), ENTRY_HEADER,
                                                        readPointer);
        String partitioningKey = consumer.getPartitioningKey();
        Map<String, Integer> partitionMap;
        if (!result.isEmpty() && partitioningKey != null && partitioningKey.length() != 0) {
          try {
            partitionMap = QueuePartitionMapSerializer.deserialize(result.getValue());
          } catch (IOException e) {
            throw new OperationException(StatusCode.INTERNAL_ERROR, "Exception while deserializing queue partition map", e);
          }
          if(partitionMap == null) {
            throw new OperationException(StatusCode.INTERNAL_ERROR, "queue partition map is null");
          }
          int hashValue = partitionMap.get(partitioningKey);
          foundEntry = partitioner.shouldEmit(consumer, entryId, hashValue);
        } else {
          foundEntry = partitioner.shouldEmit(consumer, entryId);
        }
      } else {
        foundEntry = partitioner.shouldEmit(consumer, entryId);
      }
    }
    return entryId;
  }

  @Override
  public EnqueueResult enqueue(byte[] data, long cleanWriteVersion) throws OperationException {
    return this.enqueue(new QueueEntryImpl(data), cleanWriteVersion);
  }
  @Override
  public EnqueueResult enqueue(QueueEntry entry, long cleanWriteVersion) throws OperationException {
    byte[] data = entry.getData();
    byte[] headerData;
    try {
      headerData = QueuePartitionMapSerializer.serialize(entry.getPartitioningMap());
    } catch (IOException e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, "Queue partition map serialization failed due to IOException");
    }
    if (TRACE)
      log("Enqueueing (data.len=" + data.length + ", writeVersion=" +
            cleanWriteVersion + ")");

    // Get our unique entry id
    long entryId;
    try {
      // Make sure the increment below uses increment operation of the underlying implementation directly
      // so that it is atomic (Eg. HBase increment operation)
      entryId = this.table.incrementAtomicDirtily(makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 1);
    } catch (OperationException e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, "Increment " +
        "of global entry id failed with status code " + e.getStatus() +
        ": " + e.getMessage(), e);
    }
    if (TRACE) log("New enqueue got entry id " + entryId);

    /*
    Insert entry with version=<cleanWriteVersion> and
    row-key = <queueName>20<entryId> , column/value 20/<data> and 10/EntryState.VALID
    */
    this.table.put(makeRowKey(GLOBAL_DATA_PREFIX, entryId),
                   new byte [][] { ENTRY_DATA, ENTRY_META, ENTRY_HEADER},
                   cleanWriteVersion,
                   new byte [][] {data, new EntryMeta(EntryMeta.EntryState.VALID).getBytes(), headerData});

    // Return success with pointer to entry
    return new EnqueueResult(EnqueueResult.EnqueueStatus.SUCCESS, new QueueEntryPointer(this.queueName, entryId));
  }

  @Override
  public void invalidate(QueueEntryPointer entryPointer, long cleanWriteVersion) throws OperationException {
    final byte [] rowName = makeRowKey(GLOBAL_DATA_PREFIX, entryPointer.getEntryId());
    // Change meta data to INVALID
    this.table.put(rowName, ENTRY_META,
                   cleanWriteVersion, new EntryMeta(EntryMeta.EntryState.INVALID).getBytes());
    // Delete data since it's invalidated
    this.table.delete(rowName, ENTRY_DATA, cleanWriteVersion);
    if (TRACE) log("Invalidated " + entryPointer);
  }

  // TODO: consolidate dequeue method
  /**
   * {@inheritDoc}
   */
  @Override
  public DequeueResult dequeue(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
    throws OperationException {
    return dequeueInternal(consumer, config, null, readPointer);
  }

  @Override
  public DequeueResult dequeue(QueueConsumer consumer, ReadPointer readPointer) throws OperationException {
    return dequeueInternal(consumer, consumer.getQueueConfig(), null, readPointer);
  }

  @Override
  public DequeueResult dequeue(QueueConsumer consumer, QueueConfig config, QueueState queueState,
                               ReadPointer readPointer) throws OperationException {
    return dequeueInternal(consumer, config, queueState, readPointer);
  }

  private DequeueResult dequeueInternal(QueueConsumer consumer, QueueConfig config, QueueState queueState, ReadPointer readPointer)
    throws OperationException {
    // DO NOT USE THIS CLASS!
    if (TRACE)
      log("Attempting dequeue [curNumDequeues=" + this.dequeueReturns.get() +
            "] (" + consumer + ", " + config + ", " + readPointer + ")");

    // TODO: the active entry should be read during initialization from HBase and stored in memory if tries < MAX_TRIES,
    // TODO: so that repeated reads to HBase on each dequeue is not required
    // QueueState is null, read the queue state from underlying storage.
    // ACTIVE_ENTRY contains the entry if any that is dequeued, but not acked
    // CONSUMER_READ_POINTER + 1 points to the next entry that can be read by this queue consuemr
    //
    if(queueState == null) {
      queueState = new QueueStateImpl();
      OperationResult<Map<byte[], byte[]>> state =
        this.table.get(makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()),
                       new byte[][] {ACTIVE_ENTRY, CONSUMER_READ_POINTER}, readPointer);
      if(!state.isEmpty()) {
        long activeEntryId = Bytes.toLong(state.getValue().get(ACTIVE_ENTRY));
        // TODO: check max tries
        queueState.setActiveEntryId(activeEntryId);

        byte[] consumerReadPointerBytes = state.getValue().get(CONSUMER_READ_POINTER);
        if(consumerReadPointerBytes != null) {
          queueState.setConsumerReadPointer(Bytes.toLong(consumerReadPointerBytes));
        }
      }

      // TODO: use raw get insead of the workaround of incrementing zero
      long queueWritePointer = this.table.incrementAtomicDirtily(makeRowName(GLOBAL_ENTRY_ID_PREFIX), GLOBAL_ENTRYID_COUNTER, 0);
      queueState.setQueueWritePointer(queueWritePointer);
    }

    while(true) {
      // Get the next entryId that can be dequeued by this consumer
      final long entryId = determineNextEntryId(consumer, config, queueState, readPointer);
      if(entryId == INVALID_ENTRY_ID) {
        if (TRACE) log("End of queue reached using " + "read pointer " + readPointer);
        return new DequeueResult(DequeueResult.DequeueStatus.EMPTY);
      }

      final byte [] entryRowName = makeRowKey(GLOBAL_DATA_PREFIX, entryId);
      // Read visible entry meta and entry data for the entryId
      OperationResult<Map<byte[], byte[]>> result = this.table.get(entryRowName,
                      new byte[][]{ ENTRY_META, ENTRY_DATA }, readPointer);

      if(result.isEmpty()) {
        // This entry doesn't exist or is not visible, queue is empty for this consumer
        if (TRACE) log("Queue is empty, nothing found at " + entryId + " using " +
              "read pointer " + readPointer);
        return new DequeueResult(DequeueResult.DequeueStatus.EMPTY);
      }

      // Queue entry exists and is visible, check the global state of it
      EntryMeta entryMeta = EntryMeta.fromBytes(result.getValue().get(ENTRY_META));
      if (TRACE) log("entryMeta : " + entryMeta.toString());

      // Check if entry has been invalidated or evicted
      if (entryMeta.isInvalid() || entryMeta.isEvicted()) {
        if (TRACE) log("Found invalidated or evicted entry at " + entryId +
                         " (" + entryMeta.toString() + ")");
        // This entry is invalid, move to the next entry and loop
        continue;
      }

      // Entry is visible and valid!
      assert(entryMeta.isValid());
      byte [] entryData = result.getValue().get(ENTRY_DATA);
      if (TRACE) log("Entry : " + entryId + " is dequeued");
      this.dequeueReturns.incrementAndGet();
      QueueEntry entry=new QueueEntryImpl(entryData);
      return new DequeueResult(DequeueResult.DequeueStatus.SUCCESS,
                               new QueueEntryPointer(this.queueName, entryId), entry);
    }
  }

  private long determineNextEntryId(QueueConsumer consumer, QueueConfig config, QueueState queueState, ReadPointer readPointer)
    throws OperationException {
    if(queueState.getActiveEntryId() != INVALID_ENTRY_ID) {
      return queueState.getActiveEntryId();
    }

    // Else get the next entryId
    long entryId = fetchNextEntryId(consumer, config, queueState, readPointer);
    if(entryId == INVALID_ENTRY_ID) {
      // End of queue reached for this consumer
      return INVALID_ENTRY_ID;
    }

    // Persist the entryId this consumer will be working on
    // TODO: Later when active entry can saved in memory, there is no need to write it into HBase -> (not true for FIFO!)
    QueuePartitioner partitioner=config.getPartitionerType().getPartitioner();

    if (partitioner.isDisjoint()) {
      this.table.put(makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()),
                     new byte[][] {CONSUMER_READ_POINTER, ACTIVE_ENTRY},
                     readPointer.getMaximum(),
                     new byte[][] {Bytes.toBytes(entryId), Bytes.toBytes(entryId)});
      queueState.setConsumerReadPointer(entryId);
      queueState.setActiveEntryId(entryId);
    } else {
      this.table.put(makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()),
                     new byte[][] {makeColumnName(META_ENTRY_PREFIX, entryId), ACTIVE_ENTRY},
                     readPointer.getMaximum(),
                     new byte[][] {new EntryConsumerMeta(EntryConsumerMeta.EntryState.CLAIMED, 0).getBytes(),
                       Bytes.toBytes(entryId)});
      queueState.setActiveEntryId(entryId);
    }
    return entryId;
  }


  @Override
  public void ack(QueueEntryPointer entryPointer, QueueConsumer consumer, ReadPointer readPointer)
    throws OperationException {
    // TODO: 1. Later when active entry can saved in memory, there is no need to write it into HBase
    // TODO: 2. Need to treat Ack as a simple write operation so that it can use a simple write rollback for unack
    // TODO: 3. Use Transaction.getTransactionId instead ReadPointer
    QueuePartitioner partitioner=consumer.getQueueConfig().getPartitionerType().getPartitioner();

    if (partitioner.isDisjoint()) {
      this.table.put(makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()),
                     new byte[][] {makeColumnName(META_ENTRY_PREFIX, entryPointer.getEntryId()), ACTIVE_ENTRY},
                     readPointer.getMaximum(),
                     new byte[][] {new EntryConsumerMeta(EntryConsumerMeta.EntryState.ACKED, 0).getBytes(), INVALID_ENTRY_ID_BYTES});
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
  public void finalize(QueueEntryPointer entryPointer, QueueConsumer consumer, int totalNumGroups) throws
    OperationException {
    // TODO: Evict queue entries
  }

  @Override
  public void unack(QueueEntryPointer entryPointer, QueueConsumer consumer, ReadPointer readPointer)
    throws OperationException {
    // TODO: 1. Later when active entry can saved in memory, there is no need to write it into HBase
    // TODO: 2. Need to treat Ack as a simple write operation so that it can use a simple write rollback for unack
    // TODO: 3. Ack gets rolled back with tries=0. Need to fix this by fixing point 2 above.
    this.table.put(makeRowKey(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()),
      new byte[][] {makeColumnName(META_ENTRY_PREFIX, entryPointer.getEntryId()), ACTIVE_ENTRY}, readPointer.getMaximum(),
      new byte[][] {new EntryConsumerMeta(EntryConsumerMeta.EntryState.CLAIMED, 0).getBytes(),
                                                                        Bytes.toBytes(entryPointer.getEntryId())});
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

  protected static boolean TRACE = false;

  protected void log(String msg) {
    if (TRACE) System.out.println(Thread.currentThread().getId() + " : " + msg);
  }

  protected ImmutablePair<ReadPointer, Long> dirtyPointer() {
    long now = this.timeOracle.getTimestamp();
    return new ImmutablePair<ReadPointer,Long>(new MemoryReadPointer(now), 1L);
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

  public static class QueueStateImpl implements QueueState {
    private long activeEntryId = INVALID_ENTRY_ID;
    private long consumerReadPointer = FIRST_QUEUE_ENTRY_ID - 1;
    private long queueWrtiePointer = FIRST_QUEUE_ENTRY_ID - 1;
    private final CachedList<QueueStateEntry> cachedEntries;

    public QueueStateImpl() {
      cachedEntries = new CachedList<QueueStateEntry>(Collections.EMPTY_LIST);
    }
    public QueueStateImpl(CachedList<QueueStateEntry> cachedEntries) {
      this.cachedEntries = cachedEntries;
    }

    @Override
    public long getActiveEntryId() {
      return activeEntryId;
    }

    @Override
    public void setActiveEntryId(long activeEntryId) {
      this.activeEntryId = activeEntryId;
    }

    @Override
    public long getConsumerReadPointer() {
      return consumerReadPointer;
    }

    @Override
    public void setConsumerReadPointer(long consumerReadPointer) {
      this.consumerReadPointer = consumerReadPointer;
    }

    @Override
    public long getQueueWritePointer() {
      return queueWrtiePointer;
    }

    @Override
    public void setQueueWritePointer(long queueWritePointer) {
      this.queueWrtiePointer = queueWritePointer;
    }

    @Override
    public CachedList<QueueStateEntry> getCachedEntries() {
      return cachedEntries;
    }
  }
}
