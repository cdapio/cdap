package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationBase;
import com.continuuity.api.data.ReadOperation;
import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Marks and retrieves the next entry in the queue for the specified consumer,
 * starting from the head of the queue. Entry selected depends on the mode of
 * dequeue execution.
 */
public class QueueDequeue implements ReadOperation {

  /** Unique id for the operation */
  private final long id;
  private final byte [] queueName;
  private final QueueConsumer consumer;
  private final QueueConfig config;

  public QueueDequeue(final byte [] queueName,
                      final QueueConsumer consumer,
                      final QueueConfig config) {
    this(OperationBase.getId(), queueName, consumer, config);
  }

  public QueueDequeue(final long id,
                      final byte [] queueName,
                      final QueueConsumer consumer,
                      final QueueConfig config) {
    this.id = id;
    this.queueName = queueName;
    this.consumer = consumer;
    this.config = config;
  }

  public byte[] getKey() {
    return this.queueName;
  }

  public QueueConsumer getConsumer() {
    return this.consumer;
  }

  public QueueConfig getConfig() {
    return this.config;
  }

  public String toString() {
    return Objects.toStringHelper(this)
        .add("queueName", Bytes.toString(this.queueName))
        .add("consumer", Objects.toStringHelper(this.consumer))
        .add("config", this.config.toString())
        .toString();
  }

  @Override
  public long getId() {
    return id;
  }
}
