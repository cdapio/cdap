package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.executor.ReadPointer;

import static com.continuuity.data.operation.ttqueue.QueueAdmin.QueueInfo;

/**
 * A table of {@link TTQueue}s.  See that API for details.
 */
public interface TTQueueTable {

  /**
   * Inserts an entry into the tail of the queue using the specified write
   * version.
   * @param entry the queue entry to be inserted into the queue
   * @return return enqueue result that contains the entry pointer of the new queue entry
   * @throws OperationException if unsuccessful
   */
  public EnqueueResult enqueue(byte [] queueName, QueueEntry entry, long writeVersion) throws OperationException;

  /**
   * Inserts a batch of entries into the tail of the queue using the specified write
   * version.
   * @param entries the queue entries to be inserted into the queue
   * @return return enqueue result that contains the entry pointers of the new queue entries
   * @throws OperationException if unsuccessful
   */
  public EnqueueResult enqueue(byte [] queueName, QueueEntry [] entries, long writeVersion)
    throws OperationException;

  /**
   * Invalidates a batch of entries that were enqueued into the queue.  This is used only
   * as part of a transaction rollback.
   * @param entryPointers the entry pointers of the entries to invalidate
   * @param writeVersion version entries were written with and version invalidated
   *                     entries will be written with
   * @throws OperationException if unsuccessful
   */
  public void invalidate(byte [] queueName, QueueEntryPointer [] entryPointers,
      long writeVersion) throws OperationException;

  /**
   * Attempts to mark and return an entry from the queue for the specified
   * consumer from the specified group, according to the specified configuration
   * and read pointer.
   * @param queueName name of the queue
   * @return dequeue result object
   * @throws OperationException if something goes wrong
   */
  public DequeueResult dequeue(byte [] queueName, QueueConsumer consumer, ReadPointer readPointer)
                               throws OperationException;

  /**
   * Acknowledges a previously dequeue'd queue entry.  Returns true if consumer
   * that is acknowledging is allowed to do so, false if not.
   * @throws OperationException if unsuccessful
   */
  public void ack(byte[] queueName, QueueEntryPointer entryPointer, QueueConsumer consumer, ReadPointer readPointer)
    throws OperationException;

  /**
   * Acknowledges a previously dequeue'd batch of queue entries. Returns true if consumer
   * that is acknowledging is allowed to do so, false if not.
   * @throws OperationException if unsuccessful
   */
  public void ack(byte[] queueName, QueueEntryPointer[] entryPointers, QueueConsumer consumer, ReadPointer readPointer)
    throws OperationException;


  /**
   * Finalizes a batch of acks.
   * @param queueName name of the queue
   * @param totalNumGroups total number of groups to use when doing evict-on-ack or -1 to disable
   * @param writePoint the version to use for writing queue state
   * @throws OperationException if unsuccessful
   */
  public void finalize(byte[] queueName, QueueEntryPointer [] entryPointers,
                       QueueConsumer consumer, int totalNumGroups, long writePoint) throws OperationException;

  /**
   * Unacknowledges a previously acknowledge batch of acks ack.
   * @throws OperationException if unsuccessful
   */
  void unack(byte[] queueName, QueueEntryPointer [] entryPointers, QueueConsumer consumer, ReadPointer readPointer)
             throws OperationException;

  void configure(byte[] queueName, QueueConsumer newConsumer)
    throws OperationException;

  /**
   * Generates and returns a unique group id for the specified queue.
   *
   * Note: uniqueness only guaranteed if you always use this call to generate
   * groups ids.
   *
   * @return a unique group id for the specified queue
   */
  public long getGroupID(byte [] queueName) throws OperationException;

  /**
   * Gets the meta information for the specified queue.  This includes all meta
   * data available without walking the entire queue.
   * @return global meta information for the queue and its groups
   */
  public QueueInfo getQueueInfo(byte [] queueName) throws OperationException;

  /**
   * Clears this queue table, completely wiping all queues.
   */
  public void clear() throws OperationException;

  // Old debugging methods

  public String getGroupInfo(byte[] queueName, int groupId) throws OperationException;

  public String getEntryInfo(byte[] queueName, long entryId) throws OperationException;
}
