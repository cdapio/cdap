package com.continuuity.data.operation.queue;

import com.continuuity.data.operation.type.ReadOperation;

public class QueuePop implements ReadOperation<QueueEntry> {
  private final byte [] name;
  private final QueueConsumer consumer;
  private final QueueConfig config;
  private boolean drain;

  private QueueEntry result;

  public QueuePop(byte [] queueName, QueueConsumer consumer,
      QueueConfig config) {
    this(queueName, consumer, config, false);
  }
  
  public QueuePop(byte [] queueName, QueueConsumer consumer,
      QueueConfig config, boolean drain) {
    this.name = queueName;
    this.consumer = consumer;
    this.config = config;
    this.drain = drain;
  }

  public byte [] getQueueName() {
    return this.name;
  }

  @Override
  public QueueEntry getResult() {
    return this.result;
  }

  @Override
  public void setResult(QueueEntry result) {
    this.result = result;
  }

  public QueueConsumer getConsumer() {
    return consumer;
  }

  public QueueConfig getConfig() {
    return config;
  }

  public boolean getDrain() {
    return drain;
  }
  
  public void setDrain(boolean drain) {
    this.drain = drain;
  }
}
