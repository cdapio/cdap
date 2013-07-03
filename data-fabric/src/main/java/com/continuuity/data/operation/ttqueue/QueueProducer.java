package com.continuuity.data.operation.ttqueue;

/**
 * used to transmit information about the producer of an enqueue.
 */
public class QueueProducer {

  private final String producerName; // may be null

  public QueueProducer() {
    this(null);
  }

  public QueueProducer(String producerName) {
    this.producerName = producerName;
  }

  public String getProducerName() {
    return producerName;
  }
}
