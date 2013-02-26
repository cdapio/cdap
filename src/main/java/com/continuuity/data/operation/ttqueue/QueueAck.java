package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.operation.ConditionalWriteOperation;
import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Acknowledges a previously dequeue'd entry. Ack must come from the same
 * consumer that dequeue'd.
 */
public class QueueAck extends ConditionalWriteOperation {

  private final byte [] queueName;
  private final QueueEntryPointer entryPointer;
  private final QueueConsumer consumer;
  private final int numGroups;
  
  /**
   * Acknowledge the specified queue entry for the specified queue name as the
   * specified queue consumer.
   * @param queueName
   * @param entryPointer
   * @param consumer
   */
  public QueueAck(final byte [] queueName, final QueueEntryPointer entryPointer,
      final QueueConsumer consumer) {
    this(queueName, entryPointer, consumer, -1);
  }

  /**
   * Acknowledge the specified queue entry for the specified queue name as the
   * specified queue consumer, and evict this queue entry when the specified
   * number of groups have acknowledged this queue entry.
   * @param queueName
   * @param entryPointer
   * @param consumer
   * @param numGroups total number of groups that use this queue used to evict
   *                  queue entries when they are ack'd, or -1 to disable this
   *                  feature
   */
  public QueueAck(final byte [] queueName, final QueueEntryPointer entryPointer,
                  final QueueConsumer consumer, int numGroups) {
    this.queueName = queueName;
    this.entryPointer = entryPointer;
    this.consumer = consumer;
    this.numGroups = numGroups;
  }

  /**
   * Acknowledge the specified queue entry for the specified queue name as the
   * specified queue consumer, and evict this queue entry when the specified
   * number of groups have acknowledged this queue entry.
   * @param id explicit unique id of this operation
   * @param queueName
   * @param entryPointer
   * @param consumer
   * @param numGroups total number of groups that use this queue used to evict
   *                  queue entries when they are ack'd, or -1 to disable this
   *                  feature
   */
  public QueueAck(final long id,
                  final byte [] queueName,
                  final QueueEntryPointer entryPointer,
                  final QueueConsumer consumer,
                  int numGroups) {
    super(id);
    this.queueName = queueName;
    this.entryPointer = entryPointer;
    this.consumer = consumer;
    this.numGroups = numGroups;
  }

  public QueueEntryPointer getEntryPointer() {
    return this.entryPointer;
  }
  
  public QueueConsumer getConsumer() {
    return this.consumer;
  }
  
  @Override
  public byte[] getKey() {
    return this.queueName;
  }

  /**
   * Returns the total number of groups that are using this queue to determine
   * when entries can be safely removed from the queue.  Value is -1 when
   * eviction is disabled.
   * @return total number of groups consuming from this group, or -1 to disable
   *         entry eviction
   */
  public int getNumGroups() {
    return this.numGroups;
  }
  
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("queueName", Bytes.toString(this.queueName))
        .add("entryPointer", this.entryPointer)
        .add("queueConsumer", this.consumer)
        .add("totalNumGroups", this.numGroups)
        .toString();
  }

  @Override
  public int getPriority() {
    return 3;
  }

  @Override
  public int getSize() {
    return 0;
  }
}
