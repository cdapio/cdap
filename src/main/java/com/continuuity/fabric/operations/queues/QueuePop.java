package com.continuuity.fabric.operations.queues;

import com.continuuity.fabric.operations.ReadOperation;

public class QueuePop implements ReadOperation<QueueEntry> {
  private final byte [] name;
  private final QueueConsumer consumer;
  private final QueuePartitioner partitioner;

  private QueueEntry result;

  public QueuePop(byte [] queueName, QueueConsumer consumer,
      QueuePartitioner partitioner) {
    this.name = queueName;
    this.consumer = consumer;
    this.partitioner = partitioner;
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

  public QueuePartitioner getPartitioner() {
    return partitioner;
  }

}
