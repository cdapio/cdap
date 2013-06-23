package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.ttqueue.admin.QueueInfo;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

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
   * @param entry the queue entry to be inserted into the queue
   * @param transaction transaction pointer
   * @return return enqueue result that contains the entry pointer of the new queue entry
   * @throws OperationException if unsuccessful
   */
  public EnqueueResult enqueue(QueueEntry entry, Transaction transaction)
    throws OperationException;

  /**
   * Inserts a batch of entries into the tail of the queue using the specified write
   * version.
   * @param entries the queue entries to be inserted into the queue
   * @param transaction transaction pointer
   * @return return enqueue result that contains the entry pointers of the new queue entries
   * @throws OperationException if unsuccessful
   */
  public EnqueueResult enqueue(QueueEntry[] entries, Transaction transaction)
    throws OperationException;

  /**
   * Invalidates a batch of entries that were enqueued into the queue.  This is used only
   * as part of a transaction rollback.
   * @param entryPointers the entry pointers of the entries to invalidate
   * @param transaction transaction pointer
   * @throws OperationException if unsuccessful
   */
  public void invalidate(QueueEntryPointer[] entryPointers, Transaction transaction) throws OperationException;

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
  public void ack(QueueEntryPointer entryPointer, QueueConsumer consumer, Transaction transaction)
    throws OperationException;

  /**
   * Acknowledges a previously dequeue'd batch of queue entries. Returns true if consumer
   * that is acknowledging is allowed to do so, false if not.
   * @throws OperationException if unsuccessful
   */
  public void ack(QueueEntryPointer[] entryPointers, QueueConsumer consumer, Transaction transaction)
    throws OperationException;

  /**
   * Finalizes a batch of acks.
   * @param totalNumGroups total number of groups to use when doing evict-on-ack or -1 to disable
   * @param transaction transaction pointer
   * @throws OperationException if unsuccessful
   */
  public void finalize(QueueEntryPointer[] entryPointers, QueueConsumer consumer, int totalNumGroups,
                       Transaction transaction) throws OperationException;

  /**
   * Finalizes an ack.
   * @param totalNumGroups total number of groups to use when doing evict-on-ack or -1 to disable
   * @param transaction transaction pointer
   * @throws OperationException if unsuccessful
   */
  // TODO remove this
  public void finalize(QueueEntryPointer entryPointer, QueueConsumer consumer, int totalNumGroups,
                       Transaction transaction) throws OperationException;

  /**
   * Unacknowledges a previously acknowledge batch of acks ack.
   * @throws OperationException if unsuccessful
   */
  void unack(QueueEntryPointer[] entryPointers, QueueConsumer consumer, Transaction transaction)
    throws OperationException;

  /**
   * Used to configure the queue on start-up, or when consumer instances are changed.
   * @param newConsumer consumer that contains the new configuration information.
   * @param readPointer read pointer
   * @return the old consumer count
   * @throws OperationException if unsuccessful
   */
  int configure(QueueConsumer newConsumer, ReadPointer readPointer) throws OperationException;

  /**
   * Used to configure the consumer groups of a queue on start-up.
   * Any other existing consumer groups if any will be removed.
   * @param groupIds list of groupIds to configure
   * @return the list of removed groupIds
   * @throws OperationException
   */
  List<Long> configureGroups(List<Long> groupIds) throws OperationException;

  /**
   * Drops any inflight entries for a consumer.
   * @param consumer consumer whose inflight entries need to be dropped.
   * @param readPointer read pointer
   * @throws OperationException
   */
  void dropInflightState(QueueConsumer consumer, ReadPointer readPointer) throws OperationException;

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
