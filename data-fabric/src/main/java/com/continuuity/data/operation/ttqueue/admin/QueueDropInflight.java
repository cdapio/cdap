package com.continuuity.data.operation.ttqueue.admin;

import com.continuuity.data.operation.Operation;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Operation to drop all inflight entries of a consumer without processing.
 */
public class QueueDropInflight extends Operation {
  private final byte [] queueName;
  private final QueueConsumer consumer;

  public QueueDropInflight(byte[] queueName, QueueConsumer consumer) {
    this.queueName = queueName;
    this.consumer = consumer;
  }

  public byte [] getQueueName() {
    return this.queueName;
  }

  public QueueConsumer getConsumer() {
    return consumer;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("queueName", Bytes.toString(this.queueName))
      .add("consumer", this.consumer)
      .toString();
  }
}
