package com.continuuity.fabric.operations.queues;

import com.continuuity.fabric.operations.WriteOperation;

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
}
