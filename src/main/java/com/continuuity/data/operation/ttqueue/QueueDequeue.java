package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.engine.ReadPointer;
import com.continuuity.data.operation.type.ReadOperation;

/**
 * Marks and retrieves the next entry in the queue for the specified consumer,
 * starting from the head of the queue. Entry selected depends on the mode of
 * dequeue execution.
 */
public class QueueDequeue implements ReadOperation<DequeueResult> {

  private final byte [] queueName;
  private final QueueConsumer consumer;
  private final QueueConfig config;
  private final ReadPointer readPointer;

  private DequeueResult result;

  public QueueDequeue(final byte [] queueName, final QueueConsumer consumer,
      final QueueConfig config, final ReadPointer readPointer) {
    this.queueName = queueName;
    this.consumer = consumer;
    this.config = config;
    this.readPointer = readPointer;
  }

  ReadPointer getReadPointer() {
    return this.readPointer;
  }

  public byte[] getKey() {
    return this.queueName;
  }

  public QueueConsumer getConsumer() {
    return this.consumer;
  }

  public QueueConfig getConfig() {
    return this.config;
  }
  @Override
  public void setResult(DequeueResult result) {
    this.result = result;
  }

  @Override
  public DequeueResult getResult() {
    return this.result;
  }
}
