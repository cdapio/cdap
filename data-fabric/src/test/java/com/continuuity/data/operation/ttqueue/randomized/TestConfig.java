package com.continuuity.data.operation.ttqueue.randomized;

import com.continuuity.data.operation.ttqueue.QueuePartitioner;

/**
*
*/
public class TestConfig {
  private final SelectionFunction selectionFunction;

  public TestConfig(SelectionFunction selectionFunction) {
    this.selectionFunction = selectionFunction;
  }

  public int getNumProducers() {
    return selectionFunction.select(5) + 1;
  }

  public int getNumberEnqueues() {
    return selectionFunction.select(1000) + 100;
  }

  public int getEnqueueBatchSize() {
    return selectionFunction.select(50) + 1;
  }

  public int getEnqueueSleepMs() {
    return selectionFunction.select(1000);
  }

  public boolean shouldInvalidate() {
    return selectionFunction.isProbable(0.1f);
  }

  public boolean shouldUnack() {
    return selectionFunction.isProbable(0.1f);
  }

  public int getNumConsumerGroups() {
    return selectionFunction.select(5) + 1;
  }

  public int getNumConsumers() {
    return selectionFunction.select(5) + 1;
  }

  public int getConsumerBatchSize() {
    return selectionFunction.select(50) + 1;
  }

  public boolean shouldBatchReturn() {
    return selectionFunction.isProbable(0.85f);
  }

  public int getDequeueSleepMs() {
    return selectionFunction.select(100);
  }

  public int getNumDequeueRuns() {
    return selectionFunction.select(10);
  }

  public boolean shouldFinalize() {
    return selectionFunction.isProbable(0.75f);
  }

  public QueuePartitioner.PartitionerType getPartitionType() {
    return QueuePartitioner.PartitionerType.values()[
      selectionFunction.select(QueuePartitioner.PartitionerType.values().length)
      ];
  }

  public boolean shouldConsumerCrash() {
    return selectionFunction.isProbable(0.15f);
  }

  public boolean shouldRunAsync() {
    return selectionFunction.isProbable(0.5f);
  }

  public int getAsyncDegree() {
    return selectionFunction.select(4) + 1;
  }
}
