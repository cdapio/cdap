package com.continuuity.data.operation.ttqueue.randomized;

import java.util.concurrent.BrokenBarrierException;

/**
 *
 */
public class ConsumerGroupControl {
  private final FlexibleCyclicBarrier configBarrier;
  private final FlexibleCyclicBarrier runBarrier;

  public ConsumerGroupControl(int numGroups) {
    configBarrier = new FlexibleCyclicBarrier(numGroups);
    runBarrier = new FlexibleCyclicBarrier(numGroups);
  }

  public FlexibleCyclicBarrier getConfigBarrier() {
    return configBarrier;
  }

  public FlexibleCyclicBarrier getRunBarrier() {
    return runBarrier;
  }

  public void reduceConfigBarrier() throws InterruptedException, BrokenBarrierException {
    configBarrier.decrementParties();
  }

  public void reduceRunBarrier() throws InterruptedException, BrokenBarrierException {
    runBarrier.decrementParties();
  }
}
