package com.continuuity.data.operation.queue;

import org.apache.hadoop.hbase.util.Bytes;

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
    return value;
  }

  public long getId() {
    return id;
  }

  public void setConsumer(QueueConsumer consumer) {
    this.consumer = consumer;
  }

  public QueueConsumer getConsumer() {
    return this.consumer;
  }
  @Override
  public String toString() {
    return "QueueEntry id=" + id + ", value=" + Bytes.toString(value) +
        ", consumer=" + consumer.toString();
  }
}
