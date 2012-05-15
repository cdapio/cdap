package com.continuuity.fabric.operations.queues;

import com.continuuity.fabric.operations.WriteOperation;

public class QueuePush implements WriteOperation {
  private byte [] queueName;
  private byte [] value;
 
  public QueuePush(byte [] queueName, byte [] value) {
    this.queueName = queueName;
    this.value = value;
  }

  public byte [] getQueueName() {
    return this.queueName;
  }
 
  public byte [] getValue() {
    return this.value;
  }
}
