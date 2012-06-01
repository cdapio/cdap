package com.continuuity.data.operation.ttqueue;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.operation.type.ConditionalWriteOperation;
import com.google.common.base.Objects;

/**
 * Acknowledges a previously dequeue'd entry. Ack must come from the same
 * consumer that dequeue'd.
 */
public class QueueAck implements ConditionalWriteOperation {

  private final byte [] queueName;
  private final long entryId;
  private final QueueConsumer consumer;
  
  public QueueAck(final byte [] queueName, final long entryId,
      final QueueConsumer consumer) {
    this.queueName = queueName;
    this.entryId = entryId;
    this.consumer = consumer;
  }
  
  public long getEntryId() {
    return this.entryId;
  }
  
  public QueueConsumer getConsumer() {
    return this.consumer;
  }
  
  @Override
  public byte[] getKey() {
    return this.queueName;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("queueName", Bytes.toString(this.queueName))
        .add("entryId", this.entryId)
        .add("queueConsumer", this.consumer)
        .toString();
  }

  @Override
  public int getPriority() {
    return 3;
  }
}
