package com.continuuity.data.stream;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.EnqueueResult;
import com.continuuity.data.operation.ttqueue.TTQueue;
import com.continuuity.data.operation.ttqueue.internal.EntryPointer;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.google.common.base.Charsets;
import com.google.inject.Inject;

/**
 * StreamTable implementation using queues
 */
public class StreamTable {

  private final byte[] name;
  private final TTQueue queue;
  private boolean partitionSeeked = false;
  private long startPartition ;
  private OrderedVersionedColumnarTable metaTable;
  private final byte [] column = "o".getBytes(Charsets.UTF_8);  // o for offset
  private long lastRecordedOffset = -1;
  private static final long DEFAULT_METADATA_WRITE_INTERVAL_SECONDS = 60*60;
  private final long metaDataWriteIntervalSeconds;
  private final byte [] streamPartitionPrefix = "streamParition.".getBytes(Charsets.UTF_8);

  /**
   * Construct Stream table with name, Queue and metaTable
   * @param name Name of the StreamTable
   * @param queue Queue to store the entries for read/write operations
   * @param metaTable Table to store time-based offsets to be used for addressing streams by partitions
   */
  @Inject
  public StreamTable(byte[] name, TTQueue queue, OrderedVersionedColumnarTable metaTable) {
   this(name,queue,metaTable, DEFAULT_METADATA_WRITE_INTERVAL_SECONDS);
 }

  /**
   * Construct Stream table with name, queue, metaTable and metaDataWrite interval
   * @param name Name of the StreamTable
   * @param queue  Queue to store the entries for read/write operations
   * @param metaTable Table to store time-based offsets to be used for addressing streams by partitions
   * @param metaDataWriteIntervalSeconds Interval to write Stream Meta data
   */
  @Inject
  public StreamTable(byte[] name, TTQueue queue, OrderedVersionedColumnarTable metaTable,
                     long metaDataWriteIntervalSeconds) {
    this.name = name;
    this.queue = queue;
    this.metaTable = metaTable;
    this.metaDataWriteIntervalSeconds = metaDataWriteIntervalSeconds;
    startPartition = -1;
  }

  /**
   * Set partition offset to indicate the start point from which the data should be read
   * @param offset
   */
  public void setStartPartition(long offset) {
    this.startPartition = offset;
    this.partitionSeeked = false;
  }

  /**
   * Write StreamEntry to stream
   * @param entry {@code StreamEntry} to be written to stream
   * @param writeVersion write version
   * @return instance of {@StreamWriteResult} with status success if the write was successful.
   * Never returns null.
   * @throws OperationException
   */
  public StreamWriteResult write(StreamEntry entry, long writeVersion) throws OperationException{
    EnqueueResult result = queue.enqueue(entry.toQueueEntry(),writeVersion);
    if (result.isSuccess() && writeStreamMeta())  {
      Long timeOffset = System.currentTimeMillis()/1000;
      byte [] partitionKey = generatePartitionKey(timeOffset);
      EntryPointer entryPointer = new EntryPointer(result.getEntryPointer().getEntryId(),
                                                   result.getEntryPointer().getShardId());
      metaTable.put(partitionKey,column,1L,entryPointer.getBytes());
      lastRecordedOffset = timeOffset;
    }
    return StreamWriteResult.fromEnqueueResult(result);
  }

  /**
   * Read entry from stream
   * @param consumer instance of {@code StreamQueueConsumer}
   * @param readPointer instance of {@code ReadPointer}
   * @return instance of {@StreamReadResult} with status success if the read was successful. Status Empty otherwise
   * never return null
   * @throws OperationException
   */
  public StreamReadResult read(StreamQueueConsumer consumer, ReadPointer readPointer) throws OperationException {
    if ( this.startPartition == -1 || this.partitionSeeked )  {
      return StreamReadResult.fromDequeueResult(this.queue.dequeue(consumer,readPointer));
    } else {
      return skipToStreamResult(consumer,readPointer);
    }
  }

  /**
   * Ack the entry that was previously read
   * @param entryPointer Instance of {@code StreamEntryPointer}
   * @param consumer   instance of {@code StreamQueueConsumer}
   * @param readPointer  instance of {@code ReadPointer}
   * @throws OperationException
   */
  public void ack(StreamEntryPointer entryPointer, StreamQueueConsumer consumer, ReadPointer readPointer)
    throws OperationException {
    queue.ack(entryPointer,consumer,readPointer);
  }

  /**
   * UnAck the entry that was previously read
   * @param entryPointer Instance of {@code StreamEntryPointer}
   * @param consumer   instance of {@code StreamQueueConsumer}
   * @param readPointer  instance of {@code ReadPointer}
   * @throws OperationException
   */
  public void unack(StreamEntryPointer entryPointer, StreamQueueConsumer consumer, ReadPointer readPointer)
    throws  OperationException {
    queue.unack(entryPointer,consumer,readPointer);
  }

  /**
   * Skip to the requested partition. Note: This code will change when the new Queues are ready - the plan
   * is to use QueueConfig to maintain the time addressable partition. This functions reads all entries until
   * acks all queue entries until the first entry in desired partition is read.
   * @param consumer  StreamQueueConsumer
   * @param readPointer Used in transactions - to determine which transaction point is visible
   * @return Instance of {@code StreamReadResult} with Status Success if data is present. Status empty otherwise
   * Never returns null.
   * @throws OperationException
   */
  private StreamReadResult skipToStreamResult (StreamQueueConsumer consumer, ReadPointer readPointer)
    throws OperationException {

    this.partitionSeeked = true;
    byte [] partition = generatePartitionKey(startPartition);
    OperationResult<byte[]> result = this.metaTable.getCeilValue(partition,column, readPointer);
    if (result.isEmpty()) {
      return new StreamReadResult(DequeueResult.DequeueStatus.EMPTY);
    }

    byte[] startPointer = result.getValue();

    if (startPointer != null) {
      EntryPointer pointer = EntryPointer.fromBytes(startPointer);
      DequeueResult dequeueResult = queue.dequeue(consumer,readPointer);

      while (dequeueResult.isSuccess() &&
        dequeueResult.getEntryPointer().getEntryId() <= pointer.getEntryId() &&
        dequeueResult.getEntryPointer().getShardId() <= pointer.getShardId() ) {
        queue.ack(dequeueResult.getEntryPointer(),consumer,readPointer);
        dequeueResult = queue.dequeue(consumer,readPointer);
      }

      if (dequeueResult.isSuccess()) {
        return StreamReadResult.fromDequeueResult(dequeueResult);
      } else {
        return new StreamReadResult(DequeueResult.DequeueStatus.EMPTY);
      }
    }
    else {
      return new StreamReadResult(DequeueResult.DequeueStatus.EMPTY);
    }
  }

  byte [] generatePartitionKey(long timeSeconds) {
   return Bytes.add(streamPartitionPrefix,Bytes.toBytes(timeSeconds));
  }

  /**
   * Decide if the meta data needs to be written
   * @return True if  a) lastRecorded offset is empty or b)  if metaDataWriteIntervalSeconds is passed since last
   * metaData has been written. False otherwise
   */
  private boolean writeStreamMeta(){
    if (lastRecordedOffset == -1) {
      return true;
    }
    if ( (System.currentTimeMillis()/1000 - lastRecordedOffset) >= metaDataWriteIntervalSeconds) {
      return true;
    }
    return false;
  }

}