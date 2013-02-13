package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
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

  private final VersionedColumnarTable table;
  private final byte [] queueName;
  final TimestampOracle timeOracle;

  // For testing
  AtomicLong dequeueReturns = new AtomicLong(0);

  // Row header names and flags
  static final byte [] GLOBAL_ENTRY_HEADER = {10};
  static final byte [] GLOBAL_DATA_HEADER = {20};
  static final byte [] CONSUMER_META_HEADER = {30};

  // Columns for row = GLOBAL_ENTRY_HEADER
  static final byte [] GLOBAL_ENTRYID_COUNTER = {10};

  // Columns for row = GLOBAL_DATA_HEADER
  static final byte [] ENTRY_META = {10};
  static final byte [] ENTRY_DATA = {20};

  // Columns for row = CONSUMER_META_HEADER are dynamic, based on entryID

  protected TTQueueAbstractOnVCTable(VersionedColumnarTable table, byte[] queueName, TimestampOracle timeOracle,
                                     final CConfiguration conf) {
    this.table = table;
    this.queueName = queueName;
    this.timeOracle = timeOracle;
  }

  protected abstract long fetchNextEntryId(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer);

  @Override
  public EnqueueResult enqueue(byte[] data, long cleanWriteVersion) throws OperationException {
    if (TRACE)
      log("Enqueueing (data.len=" + data.length + ", writeVersion=" +
            cleanWriteVersion + ")");

    // Get our unique entry id
    long entryId;
    try {
      // Make sure the increment below uses increment operation of the underlying implementation directly so that it is atomic
      // (Eg. HBase increment operation)
      entryId = this.table.incrementAtomicDirtily(makeRowName(GLOBAL_ENTRY_HEADER), GLOBAL_ENTRYID_COUNTER, 1);
    } catch (OperationException e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, "Increment " +
        "of global entry id failed with status code " + e.getStatus() +
        ": " + e.getMessage(), e);
    }
    if (TRACE) log("New enqueue got entry id " + entryId);

    // Insert entry at active shard
    this.table.put(makeRowName(GLOBAL_DATA_HEADER),
                   new byte [][] { makeColumnName(entryId, ENTRY_DATA), makeColumnName(entryId, ENTRY_META)},
                   cleanWriteVersion,
                   new byte [][] {data, new EntryMeta(EntryMeta.EntryState.VALID).getBytes()});

    // Return success with pointer to entry
    return new EnqueueResult(EnqueueResult.EnqueueStatus.SUCCESS, new QueueEntryPointer(this.queueName, entryId));
  }

  @Override
  public void invalidate(QueueEntryPointer entryPointer, long cleanWriteVersion) throws OperationException {
    final byte [] rowName = makeRowName(GLOBAL_DATA_HEADER);
    // Change meta data to INVALID
    this.table.put(rowName, makeColumnName(entryPointer.getEntryId(), ENTRY_META),
                   cleanWriteVersion, new EntryMeta(EntryMeta.EntryState.INVALID).getBytes());
    // Delete data since it's invalidated
    this.table.delete(rowName, makeColumnName(entryPointer.getEntryId(), ENTRY_DATA), cleanWriteVersion);
    log("Invalidated " + entryPointer);
  }

  @Override
  public DequeueResult dequeue(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer) throws OperationException {
    if (TRACE)
      log("Attempting dequeue [curNumDequeues=" + this.dequeueReturns.get() +
            "] (" + consumer + ", " + config + ", " + readPointer + ")");

    final byte [] rowName = makeRowName(GLOBAL_DATA_HEADER);
    while(true) {
      // Get the next entryId that can be dequeued by this consumer
      final long entryId = fetchNextEntryId(consumer, config, readPointer);
      final byte [] metaColName = makeColumnName(entryId, ENTRY_META);
      final byte [] dataColName = makeColumnName(entryId, ENTRY_DATA);
      // Read visible entry meta and entry data for the entryId
      OperationResult<Map<byte[], byte[]>> result = this.table.get(rowName,
                      new byte[][]{ metaColName, dataColName }, readPointer);

      if(result.isEmpty()) {
        // This entry doesn't exist or is not visible, queue is empty for this consumer
        log("Queue is empty, nothing found at " + entryId + " using " +
              "read pointer " + readPointer);
        return new DequeueResult(DequeueResult.DequeueStatus.EMPTY);
      }

      // Queue entry exists and is visible, check the global state of it
      EntryMeta entryMeta = EntryMeta.fromBytes(result.getValue().get(metaColName));
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
      byte [] entryData = result.getValue().get(dataColName);
      if (TRACE) log("Entry : " + entryId + " is dequeued");
      this.dequeueReturns.incrementAndGet();
      return new DequeueResult(DequeueResult.DequeueStatus.SUCCESS,
                               new QueueEntryPointer(this.queueName, entryId), entryData);
    }
  }

  @Override
  public void ack(QueueEntryPointer entryPointer, QueueConsumer consumer) throws OperationException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void finalize(QueueEntryPointer entryPointer, QueueConsumer consumer, int totalNumGroups) throws
    OperationException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void unack(QueueEntryPointer entryPointer, QueueConsumer consumer) throws OperationException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public long getGroupID() throws OperationException {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public QueueAdmin.QueueInfo getQueueInfo() throws OperationException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
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

  protected byte[] makeColumnName(long id, byte[] bytesToAppendToId) {
    return Bytes.add(Bytes.toBytes(id), bytesToAppendToId);
  }
}
