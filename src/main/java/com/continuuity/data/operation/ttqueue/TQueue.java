package com.continuuity.data.operation.ttqueue;


/**
 * A Transactional Tabular Queue interface.
 *
 * See <pre>https://wiki.continuuity.com/display/PROD/Transactional+Tabular+Queues</pre>
 * for more information about TTQueue semantics.
 */
public interface TQueue {

  /**
   * Inserts an entry into the tail of the queue.
   * @param data the data to be inserted into the queue
   * @return return code, and if success, the unique entryId of the queue entry
   */
  public EnqueueResult enqueue(byte [] data);

  /**
   * Attempts to mark and return an entry from the queue for the specified
   * consumer from the specified group, according to the specified config.
   * @param consumer
   * @param config
   * @return
   */
  public DequeueResult dequeue(QueueConsumer consumer, QueueConfig config);

  /**
   * Acknowledges a previously dequeue'd queue entry.  Returns true if consumer
   * that is acknowledging is allowed to do so, false if not.
   * @param entryPointer
   * @param consumer
   * @return
   */
  public boolean ack(QueueEntryPointer entryPointer, QueueConsumer consumer);
}
