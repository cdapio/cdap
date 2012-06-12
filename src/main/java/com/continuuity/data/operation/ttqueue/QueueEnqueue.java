package com.continuuity.data.operation.ttqueue;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.api.data.ReadOperation;
import com.continuuity.api.data.WriteOperation;
import com.google.common.base.Objects;

/**
 * Inserts an entry to the tail of a queue.
 */
public class QueueEnqueue
implements WriteOperation, ReadOperation<EnqueueResult> {

  private final byte [] queueName;
  private final byte [] data;

  private EnqueueResult result;
  
  public QueueEnqueue(final byte [] queueName, final byte [] data) {
    this.queueName = queueName;
    this.data = data;
  }

  public byte [] getData() {
    return this.data;
  }

  @Override
  public byte[] getKey() {
    return this.queueName;
  }

  @Override
  public void setResult(EnqueueResult result) {
    this.result = result;
  }

  @Override
  public EnqueueResult getResult() {
    return this.result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("queueName", Bytes.toString(this.queueName))
        .add("data.length", this.data != null ? this.data.length : 0)
        .toString();
  }

  @Override
  public int getPriority() {
    return 2;
  }
}
