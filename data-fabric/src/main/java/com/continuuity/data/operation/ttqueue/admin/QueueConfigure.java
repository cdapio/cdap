package com.continuuity.data.operation.ttqueue.admin;

import com.continuuity.data.operation.Operation;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Configures the queue.
 */
public class QueueConfigure extends Operation {

  private final byte [] queueName;
  private final QueueConsumer newConsumer;

  public QueueConfigure(byte[] queueName, QueueConsumer newConsumer) {
    this.queueName = queueName;
    this.newConsumer = newConsumer;
  }

  public byte [] getQueueName() {
    return this.queueName;
  }

  public QueueConsumer getNewConsumer() {
    return newConsumer;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("queuename", Bytes.toString(this.queueName))
      .add("newConsumer", this.newConsumer)
      .toString();
  }
}
