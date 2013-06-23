package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.ttqueue.admin.QueueInfo;

import java.util.List;

/**
 * A table of {@link TTQueue}s.  See that API for details.
 */
public interface TTQueueTable {

  /**
   * Inserts an entry into the tail of the queue using the specified write
   * version.
   *
   * @param entry the queue entry to be inserted into the queue
   * @param transaction transaction pointer
   * @return return enqueue result that contains the entry pointer of the new queue entry
   * @throws OperationException if unsuccessful
   */
  public EnqueueResult enqueue(byte[] queueName, QueueEntry entry, Transaction transaction) throws OperationException;

  /**
   * Inserts a batch of entries into the tail of the queue using the specified write
   * version.
   *
   * @param entries the queue entries to be inserted into the queue
   * @param transaction transaction pointer
   * @return return enqueue result that contains the entry pointers of the new queue entries
   * @throws OperationException if unsuccessful
   */
  public EnqueueResult enqueue(byte[] queueName, QueueEntry[] entries, Transaction transaction)
    throws OperationException;

  /**
   * Invalidates a batch of entries that were enqueued into the queue.  This is used only
   * as part of a transaction rollback.
   *
   * @param entryPointers the entry pointers of the entries to invalidate
   * @param transaction transaction pointer
   * @throws OperationException if unsuccessful
   */
  public void invalidate(byte[] queueName, QueueEntryPointer[] entryPointers, Transaction transaction)
    throws OperationException;

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
  public void ack(byte[] queueName, QueueEntryPointer entryPointer, QueueConsumer consumer,
                  Transaction transaction) throws OperationException;

  /**
   * Acknowledges a previously dequeue'd batch of queue entries. Returns true if consumer
   * that is acknowledging is allowed to do so, false if not.
   * @throws OperationException if unsuccessful
   */
  public void ack(byte[] queueName, QueueEntryPointer[] entryPointers, QueueConsumer consumer, Transaction transaction)
    throws OperationException;


  /**
   * Finalizes a batch of acks.
   *
   *
   * @param queueName name of the queue
   * @param totalNumGroups total number of groups to use when doing evict-on-ack or -1 to disable
   * @param transaction transaction pointer
   * @throws OperationException if unsuccessful
   */
  public void finalize(byte[] queueName, QueueEntryPointer[] entryPointers, QueueConsumer consumer, int totalNumGroups,
                       Transaction transaction) throws OperationException;

  /**
   * Unacknowledges a previously acknowledge batch of acks ack.
   * @throws OperationException if unsuccessful
   */
  void unack(byte[] queueName, QueueEntryPointer[] entryPointers, QueueConsumer consumer, Transaction transaction)
             throws OperationException;

  /**
   * Used to configure the queue on start-up, or when consumer instances are changed.
   *
   * @param queueName name of the queue
   * @param newConsumer consumer that contains the new configuration information.
   * @param readPointer read pointer
   * @throws OperationException if unsuccessful
   */
  int configure(byte[] queueName, QueueConsumer newConsumer, ReadPointer readPointer)
    throws OperationException;

  /**
   * Used to configure the consumer groups of a queue on start-up.
   * Any other existing consumer groups if any will be removed.
   *
   * @param queueName name of the queue
   * @param groupIds list of groupIds to configure
   * @return the list of removed groupIds
   * @throws OperationException
   */
  List<Long> configureGroups(byte[] queueName, List<Long> groupIds) throws OperationException;

  /**
   * Drops any inflight entries for a consumer of a queue.
   *
   * @param queueName name of the queue
   * @param consumer consumer whose inflight entries need to be dropped.
   * @param readPointer read pointer
   * @throws OperationException
   */
  void dropInflightState(byte[] queueName, QueueConsumer consumer, ReadPointer readPointer) throws OperationException;

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
