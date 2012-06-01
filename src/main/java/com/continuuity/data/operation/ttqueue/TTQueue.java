package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.table.ReadPointer;

/**
 * A Transactional Tabular Queue interface.
 *
 * See <pre>https://wiki.continuuity.com/display/PROD/Transactional+Tabular+Queues</pre>
 * for more information about TTQueue semantics.
 */
public interface TTQueue {

  /**
   * Inserts an entry into the tail of the queue using the specified write
   * version.
   * @param data the data to be inserted into the queue
   * @param writeVersion
   * @return return code, and if success, the unique entryId of the queue entry
   */
  public EnqueueResult enqueue(byte [] data, long writeVersion);

  /**
   * Invalidates an entry that was enqueued into the queue.  This is used only
   * as part of a transaction rollback.
   * @param entryPointer entry id and shard id of enqueued entry to invalidate
   * @param writeVersion version entry was written with and version invalidated
   *                     entry will be written with
   */
  public void invalidate(QueueEntryPointer entryPointer, long writeVersion);

  /**
   * Attempts to mark and return an entry from the queue for the specified
   * consumer from the specified group, according to the specified configuration
   * and read pointer.
   * @param consumer
   * @param config
   * @param readPointer
   * @return
   */
  public DequeueResult dequeue(QueueConsumer consumer, QueueConfig config,
      ReadPointer readPointer);

  /**
   * Acknowledges a previously dequeue'd queue entry.  Returns true if consumer
   * that is acknowledging is allowed to do so, false if not.
   * @param entryPointer
   * @param consumer
   * @return
   */
  public boolean ack(QueueEntryPointer entryPointer, QueueConsumer consumer);
}
