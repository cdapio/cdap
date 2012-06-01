package com.continuuity.data.operation.ttqueue;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.operation.type.WriteOperation;
import com.google.common.base.Objects;

public class QueueInvalidate implements WriteOperation {

  private final byte [] queueName;
  private final long entryId;
  private final long writeVersion;

  public QueueInvalidate(final byte [] queueName, final long entryId,
      final long writeVersion) {
    this.queueName = queueName;
    this.entryId = entryId;
    this.writeVersion = writeVersion;
  }

  public long getEntryId() {
    return this.entryId;
  }

  public long getWriteVersion() {
    return this.writeVersion;
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
        .add("writeVersion", this.writeVersion)
        .toString();
  }

  @Override
  public int getPriority() {
    return 0;
  }
}