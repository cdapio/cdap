package com.continuuity.fabric.operations.impl;

import com.continuuity.fabric.operations.ReadOperation;

public class QueuePop implements ReadOperation<byte[]> {
  private final byte [] queueName;

  private byte [] result;

  public QueuePop(byte [] queueName) {
    this.queueName = queueName;
  }

  public byte [] getQueueName() {
    return this.queueName;
  }

  @Override
  public byte[] getResult() {
    return this.result;
  }

  @Override
  public void setResult(byte[] result) {
    this.result = result;
  }

}
