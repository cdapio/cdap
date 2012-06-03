package com.continuuity.data.operation.ttqueue;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.operation.type.WriteOperation;
import com.google.common.base.Objects;

public class QueueInvalidate implements WriteOperation {

  private final byte [] queueName;
  private final QueueEntryPointer entryPointer;

  public QueueInvalidate(final byte [] queueName,
      final QueueEntryPointer entryPointer) {
    this.queueName = queueName;
    this.entryPointer = entryPointer;
  }

  public QueueEntryPointer getEntryPointer() {
    return this.entryPointer;
  }

  @Override
  public byte[] getKey() {
    return this.queueName;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("queueName", Bytes.toString(this.queueName))
        .add("entryPointer", this.entryPointer)
        .toString();
  }

  @Override
  public int getPriority() {
    return 0;
  }
}