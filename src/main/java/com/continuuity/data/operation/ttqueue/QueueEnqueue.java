package com.continuuity.data.operation.ttqueue;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.operation.type.ReadOperation;
import com.continuuity.data.operation.type.WriteOperation;
import com.google.common.base.Objects;

/**
 * Inserts an entry to the tail of a queue.
 */
public class QueueEnqueue
implements WriteOperation, ReadOperation<EnqueueResult> {

  private final byte [] queueName;
  private final byte [] data;
  private final long writeVersion;

  private EnqueueResult result;
  
  public QueueEnqueue(final byte [] queueName, final byte [] data,
      final long writeVersion) {
    this.queueName = queueName;
    this.data = data;
    this.writeVersion = writeVersion;
  }

  public byte [] getData() {
    return this.data;
  }

  public long getWriteVersion() {
    return this.writeVersion;
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
        .add("writeVersion", this.writeVersion)
        .toString();
  }

  @Override
  public int getPriority() {
    return 2;
  }
}
