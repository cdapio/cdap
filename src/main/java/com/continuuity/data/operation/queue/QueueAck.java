package com.continuuity.data.operation.queue;

import com.continuuity.data.operation.type.WriteOperation;

public class QueueAck implements WriteOperation {
  private byte [] queueName;
  private QueueEntry queueEntry;
 
  public QueueAck(byte [] queueName, QueueEntry queueEntry) {
    this.queueName = queueName;
    this.queueEntry = queueEntry;
  }

  public byte [] getQueueName() {
    return this.queueName;
  }
 
  public QueueEntry getQueueEntry() {
    return this.queueEntry;
  }

  @Override
  public byte[] getKey() {
    return queueName;
  }
}
