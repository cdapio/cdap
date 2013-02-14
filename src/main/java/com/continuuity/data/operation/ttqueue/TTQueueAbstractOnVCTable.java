package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.operation.ttqueue.internal.EntryConsumerMeta;
import com.continuuity.data.operation.ttqueue.internal.EntryMeta;
import com.continuuity.data.table.ReadPointer;
import com.continuuity.data.table.VersionedColumnarTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public abstract class TTQueueAbstractOnVCTable implements TTQueue {

  protected final VersionedColumnarTable table;
  private final byte [] queueName;
  final TimestampOracle timeOracle;

  // For testing
  AtomicLong dequeueReturns = new AtomicLong(0);

  // Row prefix names and flags
  static final byte [] GLOBAL_ENTRY_PREFIX = {10};
  static final byte [] GLOBAL_DATA_PREFIX = {20};
  static final byte [] CONSUMER_META_PREFIX = {30};

  // Columns for row = GLOBAL_ENTRY_PREFIX
  static final byte [] GLOBAL_ENTRYID_COUNTER = {10};

  // Columns for row = GLOBAL_DATA_PREFIX
  static final byte [] ENTRY_META = {10};
  static final byte [] ENTRY_DATA = {20};

  // Columns for row = CONSUMER_META_PREFIX
  static final byte [] ACTIVE_ENTRY = {10};
  static final byte [] META_ENTRY_PREFIX = {20};

  static final long INVALID_ACTIVE_ENTRY_ID_VALUE = -1;

  protected TTQueueAbstractOnVCTable(VersionedColumnarTable table, byte[] queueName, TimestampOracle timeOracle,
                                     final CConfiguration conf) {
    this.table = table;
    this.queueName = queueName;
    this.timeOracle = timeOracle;
  }

  protected abstract long fetchNextEntryId(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
    throws OperationException;

  @Override
  public EnqueueResult enqueue(byte[] data, long cleanWriteVersion) throws OperationException {
    if (TRACE)
      log("Enqueueing (data.len=" + data.length + ", writeVersion=" +
            cleanWriteVersion + ")");

    // Get our unique entry id
    long entryId;
    try {
      // Make sure the increment below uses increment operation of the underlying implementation directly
      // so that it is atomic (Eg. HBase increment operation)
      entryId = this.table.incrementAtomicDirtily(makeRowName(GLOBAL_ENTRY_PREFIX), GLOBAL_ENTRYID_COUNTER, 1);
    } catch (OperationException e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, "Increment " +
        "of global entry id failed with status code " + e.getStatus() +
        ": " + e.getMessage(), e);
    }
    if (TRACE) log("New enqueue got entry id " + entryId);

    // Insert entry
    this.table.put(makeRowName(GLOBAL_DATA_PREFIX, entryId),
                   new byte [][] { ENTRY_DATA, ENTRY_META},
                   cleanWriteVersion,
                   new byte [][] {data, new EntryMeta(EntryMeta.EntryState.VALID).getBytes()});

    // Return success with pointer to entry
    return new EnqueueResult(EnqueueResult.EnqueueStatus.SUCCESS, new QueueEntryPointer(this.queueName, entryId));
  }

  @Override
  public void invalidate(QueueEntryPointer entryPointer, long cleanWriteVersion) throws OperationException {
    final byte [] rowName = makeRowName(GLOBAL_DATA_PREFIX, entryPointer.getEntryId());
    // Change meta data to INVALID
    this.table.put(rowName, ENTRY_META,
                   cleanWriteVersion, new EntryMeta(EntryMeta.EntryState.INVALID).getBytes());
    // Delete data since it's invalidated
    this.table.delete(rowName, ENTRY_DATA, cleanWriteVersion);
    log("Invalidated " + entryPointer);
  }

  @Override
  public DequeueResult dequeue(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
    throws OperationException {
    if (TRACE)
      log("Attempting dequeue [curNumDequeues=" + this.dequeueReturns.get() +
            "] (" + consumer + ", " + config + ", " + readPointer + ")");

    while(true) {
      // Get the next entryId that can be dequeued by this consumer
      final long entryId = determineNextEntryId(consumer, config, readPointer);

      final byte [] rowName = makeRowName(GLOBAL_DATA_PREFIX, entryId);
      // Read visible entry meta and entry data for the entryId
      OperationResult<Map<byte[], byte[]>> result = this.table.get(rowName,
                      new byte[][]{ ENTRY_META, ENTRY_DATA }, readPointer);

      if(result.isEmpty()) {
        // This entry doesn't exist or is not visible, queue is empty for this consumer
        log("Queue is empty, nothing found at " + entryId + " using " +
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
      return new DequeueResult(DequeueResult.DequeueStatus.SUCCESS,
                               new QueueEntryPointer(this.queueName, entryId), entryData);
    }
  }

  private long determineNextEntryId(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
    throws OperationException {
    // Read any claimed entry that was not processed
    // TODO: the active entry should be read during initialization from HBase and stored in memory if tries < MAX_TRIES,
    // TODO: so that repeated reads to HBase on each dequeue is not required
    OperationResult<byte[]> result =
      this.table.get(makeRowName(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()),
                     ACTIVE_ENTRY, readPointer);
    if(!result.isEmpty()) {
      long activeEntryID = Bytes.toLong(result.getValue());
      if(activeEntryID != INVALID_ACTIVE_ENTRY_ID_VALUE) {
        return activeEntryID;
      }
    }

    // Else get the next entryId
    long entryId = fetchNextEntryId(consumer, config, readPointer);
    // Persist the entryId this consumer will be working on
    // TODO: Later when active entry can saved in memory, there is no need to write it into HBase
    this.table.put(makeRowName(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()),
      new byte[][] {makeColumnName(META_ENTRY_PREFIX, entryId), ACTIVE_ENTRY}, readPointer.getMaximum(),
      new byte[][] {new EntryConsumerMeta(EntryConsumerMeta.EntryState.CLAIMED, 0).getBytes(), Bytes.toBytes(entryId)});
    return entryId;
  }

  @Override
  public void ack(QueueEntryPointer entryPointer, QueueConsumer consumer, ReadPointer readPointer)
    throws OperationException {
    // TODO: 1. Later when active entry can saved in memory, there is no need to write it into HBase
    // TODO: 2. Need to treat Ack as a simple write operation so that it can use a simple write rollback for unack
    this.table.put(makeRowName(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()),
      new byte[][] {makeColumnName(META_ENTRY_PREFIX, entryPointer.getEntryId()), ACTIVE_ENTRY}, readPointer.getMaximum(),
      new byte[][] {new EntryConsumerMeta(EntryConsumerMeta.EntryState.ACKED, 0).getBytes(),
                                                                        Bytes.toBytes(INVALID_ACTIVE_ENTRY_ID_VALUE)});
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
    this.table.put(makeRowName(CONSUMER_META_PREFIX, consumer.getGroupId(), consumer.getInstanceId()),
      new byte[][] {makeColumnName(META_ENTRY_PREFIX, entryPointer.getEntryId()), ACTIVE_ENTRY}, readPointer.getMaximum(),
      new byte[][] {new EntryConsumerMeta(EntryConsumerMeta.EntryState.CLAIMED, 0).getBytes(),
                                                                        Bytes.toBytes(entryPointer.getEntryId())});
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

  protected byte[] makeRowName(byte[] bytesToAppendToQueueName, long id1) {
    return Bytes.add(this.queueName, bytesToAppendToQueueName, Bytes.toBytes(id1));
  }

  protected byte[] makeRowName(byte[] bytesToAppendToQueueName, long id1, int id2) {
    return Bytes.add(
      Bytes.add(this.queueName, bytesToAppendToQueueName, Bytes.toBytes(id1)),
      Bytes.toBytes(id2));
  }
  protected byte[] makeColumnName(byte[] bytesToPrependToId, long id) {
    return Bytes.add(bytesToPrependToId, Bytes.toBytes(id));
  }
}
