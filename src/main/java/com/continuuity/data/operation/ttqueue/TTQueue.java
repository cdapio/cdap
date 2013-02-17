package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.table.ReadPointer;
import org.apache.hadoop.hbase.util.Bytes;

import static com.continuuity.data.operation.ttqueue.QueueAdmin.QueueInfo;

/**
 * A Transactional Tabular Queue interface.
 *
 * See <pre>https://wiki.continuuity.com/display/PROD/Transactional+Tabular+Queues</pre>
 * for more information about TTQueue semantics.
 */
public interface TTQueue {

  public static final byte [] QUEUE_NAME_PREFIX = Bytes.toBytes("queue://");
  public static final byte [] STREAM_NAME_PREFIX = Bytes.toBytes("stream://");
  
  /**
   * Inserts an entry into the tail of the queue using the specified write
   * version.
   * @param data the data to be inserted into the queue
   * @return return code, and if success, the unique entryId of the queue entry
   * @throws OperationException if unsuccessful
   */
  public EnqueueResult enqueue(byte [] data, byte[] headerData, long writeVersion)
//  public EnqueueResult enqueue(byte [] data, byte [] mapData, long writeVersion)
      throws OperationException;

  /**
   * Invalidates an entry that was enqueued into the queue.  This is used only
   * as part of a transaction rollback.
   * @param entryPointer entry id and shard id of enqueued entry to invalidate
   * @param writeVersion version entry was written with and version invalidated
   *                     entry will be written with
   * @throws OperationException if unsuccessful
   */
  public void invalidate(QueueEntryPointer entryPointer, long writeVersion) throws OperationException;

  /**
   * Attempts to mark and return an entry from the queue for the specified
   * consumer from the specified group, according to the specified configuration
   * and read pointer.
   * @param consumer the queue consumer
   * @return dequeue result object
   * @throws OperationException if unsuccessful
   * @deprecated
   */
  public DequeueResult dequeue(QueueConsumer consumer, QueueConfig config, ReadPointer readPointer)
    throws OperationException;

  /**
   * Attempts to mark and return an entry from the queue for the specified
   * consumer from the specified group, according to the specified configuration
   * and read pointer.
   * @param consumer the queue consumer
   * @return dequeue result object
   * @throws OperationException if unsuccessful
   */
  public DequeueResult dequeue(QueueConsumer consumer, ReadPointer readPointer) throws OperationException;

  /**
   * Acknowledges a previously dequeue'd queue entry.  Returns true if consumer
   * that is acknowledging is allowed to do so, false if not.
   * @throws OperationException if unsuccessful
   */
  public void ack(QueueEntryPointer entryPointer, QueueConsumer consumer, ReadPointer readPointer)
      throws OperationException;

  /**
   * Finalizes an ack.
   *
   * @param totalNumGroups total number of groups to use when doing evict-on-ack
   *                       or -1 to disable
   * @throws OperationException if unsuccessful
   */
  public void finalize(QueueEntryPointer entryPointer,
                       QueueConsumer consumer, int totalNumGroups) throws OperationException;

  /**
   * Unacknowledges a previously acknowledge ack.
   *
   * @throws OperationException if unsuccessful
   */
  void unack(QueueEntryPointer entryPointer, QueueConsumer consumer, ReadPointer readPointer) throws OperationException;

  /**
   * Generates and returns a unique group id for this queue.
   * 
   * Note: uniqueness only guaranteed if you always use this call to generate
   * groups ids.
   * 
   * @return a unique group id for this queue
   */
  public long getGroupID() throws OperationException;
  
  /**
   * Gets the meta information for this queue.  This includes all meta
   * data available without walking the entire queue.
   * @return global meta information for this queue and its groups
   */
  public QueueInfo getQueueInfo() throws OperationException;
}
