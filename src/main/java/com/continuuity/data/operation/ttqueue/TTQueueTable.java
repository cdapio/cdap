package com.continuuity.data.operation.ttqueue;

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
   */
  public EnqueueResult enqueue(byte [] queueName, byte [] data,
      long writeVersion);

  /**
   * Invalidates an entry that was enqueued into the queue.  This is used only
   * as part of a transaction rollback.
   * @param queueName name of the queue
   * @param entryPointer entry id and shard id of enqueued entry to invalidate
   * @param writeVersion version entry was written with and version invalidated
   *                     entry will be written with
   */
  public void invalidate(byte [] queueName, QueueEntryPointer entryPointer,
      long writeVersion);

  /**
   * Attempts to mark and return an entry from the queue for the specified
   * consumer from the specified group, according to the specified configuration
   * and read pointer.
   * @param queueName name of the queue
   * @param consumer
   * @param config
   * @param readPointer
   * @return
   */
  public DequeueResult dequeue(byte [] queueName, QueueConsumer consumer,
      QueueConfig config, ReadPointer readPointer);

  /**
   * Acknowledges a previously dequeue'd queue entry.  Returns true if consumer
   * that is acknowledging is allowed to do so, false if not.
   * @param queueName name of the queue
   * @param entryPointer
   * @param consumer
   * @return
   */
  public boolean ack(byte [] queueName, QueueEntryPointer entryPointer,
      QueueConsumer consumer);

  /**
   * Finalizes an ack.
   * @param queueName name of the queue
   * @param entryPointer
   * @param consumer
   * @return
   */
  public boolean finalize(byte [] queueName, QueueEntryPointer entryPointer,
      QueueConsumer consumer);

  /**
   * Unacknowledges a previously acknowledge ack.
   * @param queueName name of the queue
   * @param entryPointer
   * @param consumer
   * @return
   */
  boolean unack(byte [] queueName, QueueEntryPointer entryPointer,
      QueueConsumer consumer);

  public String getGroupInfo(byte[] queueName, int groupId);

  public String getEntryInfo(byte[] queueName, long entryId);
}
