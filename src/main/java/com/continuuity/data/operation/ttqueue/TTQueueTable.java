package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.ttqueue.QueueAdmin.QueueMeta;
import com.continuuity.data.table.ReadPointer;

/**
 * A table of {@link TTQueue}s.  See that API for details.
 */
public interface TTQueueTable {

  /**
   * Inserts an entry into the tail of the queue using the specified write
   * version.
   * @param queueName name of the queue
   * @param data the data to be inserted into the queue
   * @param writeVersion
   * @return return code, and if success, the unique entryId of the queue entry
   * @throws OperationException if something goes wrong
   */
  public EnqueueResult enqueue(byte [] queueName, byte [] data,
      long writeVersion) throws OperationException;

  /**
   * Invalidates an entry that was enqueued into the queue.  This is used only
   * as part of a transaction rollback.
   * @param queueName name of the queue
   * @param entryPointer entry id and shard id of enqueued entry to invalidate
   * @param writeVersion version entry was written with and version invalidated
   *                     entry will be written with
   * @throws OperationException if something goes wrong
   */
  public void invalidate(byte [] queueName, QueueEntryPointer entryPointer,
      long writeVersion) throws OperationException;

  /**
   * Attempts to mark and return an entry from the queue for the specified
   * consumer from the specified group, according to the specified configuration
   * and read pointer.
   * @param queueName name of the queue
   * @param consumer
   * @param config
   * @param readPointer
   * @return dequeue result object
   * @throws OperationException if something goes wrong
   */
  public DequeueResult dequeue(byte [] queueName, QueueConsumer consumer,
      QueueConfig config, ReadPointer readPointer) throws OperationException;

  /**
   * Acknowledges a previously dequeue'd queue entry.  Returns true if consumer
   * that is acknowledging is allowed to do so, false if not.
   *
   * @param queueName name of the queue
   * @param entryPointer
   * @param consumer
   * @throws OperationException if something goes wrong
   */
  public void ack(byte[] queueName, QueueEntryPointer entryPointer,
                  QueueConsumer consumer) throws OperationException;

  /**
   * Finalizes an ack.
   *
   * @param queueName name of the queue
   * @param entryPointer
   * @param consumer
   * @param totalNumGroups total number of groups to use when doing evict-on-ack
   *                       or -1 to disable
   * @throws OperationException if something goes wrong
   */
  public void finalize(byte[] queueName, QueueEntryPointer entryPointer,
                       QueueConsumer consumer, int totalNumGroups) throws OperationException;

  /**
   * Unacknowledges a previously acknowledge ack.
   *
   * @param queueName name of the queue
   * @param entryPointer
   * @param consumer
   * @throws OperationException if something goes wrong
   */
  void unack(byte[] queueName, QueueEntryPointer entryPointer,
             QueueConsumer consumer) throws OperationException;

  /**
   * Generates and returns a unique group id for the specified queue.
   *
   * Note: uniqueness only guaranteed if you always use this call to generate
   * groups ids.
   *
   * @param queueName
   * @return a unique group id for the specified queue
   */
  public long getGroupID(byte [] queueName) throws OperationException;

  /**
   * Gets the meta information for the specified queue.  This includes all meta
   * data available without walking the entire queue.
   * @param queueName
   * @return global meta information for the queue and its groups
   */
  public QueueMeta getQueueMeta(byte [] queueName) throws OperationException;

  /**
   * Clears this queue table, completely wiping all queues.
   */
  public void clear() throws OperationException;

  // Old debugging methods

  public String getGroupInfo(byte[] queueName, int groupId) throws OperationException;

  public String getEntryInfo(byte[] queueName, long entryId) throws OperationException;
}
