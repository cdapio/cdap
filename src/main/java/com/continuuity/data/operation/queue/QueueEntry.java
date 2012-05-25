package com.continuuity.data.operation.queue;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Objects;

public class QueueEntry {

  private final byte [] value;

  private final long id;

  private QueueConsumer consumer;

  public QueueEntry(byte [] value, long id) {
    this.value = value;
    this.id = id;
    this.consumer = null;
  }

  public byte [] getValue() {
    return this.value;
  }

  public long getId() {
    return this.id;
  }

  public void setConsumer(QueueConsumer consumer) {
    this.consumer = consumer;
  }

  public QueueConsumer getConsumer() {
    return this.consumer;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("id", this.id)
        .add("value", Bytes.toString(this.value))
        .add("consumer", this.consumer)
        .toString();
  }
}
