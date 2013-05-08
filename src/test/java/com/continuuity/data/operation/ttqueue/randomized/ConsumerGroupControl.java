package com.continuuity.data.operation.ttqueue.randomized;

import java.util.concurrent.CyclicBarrier;

/**
 *
 */
public class ConsumerGroupControl {
  private volatile CyclicBarrier configBarrier;
  private volatile CyclicBarrier runBarrier;

  public ConsumerGroupControl(int numGroups) {
    configBarrier = new CyclicBarrier(numGroups);
    runBarrier = new CyclicBarrier(numGroups);
  }

  public CyclicBarrier getConfigBarrier() {
    return configBarrier;
  }

  public CyclicBarrier getRunBarrier() {
    return runBarrier;
  }

  public void reduceConfigBarrier() {
    synchronized (this) {
      int num = configBarrier.getParties();
      if(num == 1) {
        return;
      }
      configBarrier = new CyclicBarrier(num - 1);
    }
  }

  public void reduceRunBarrier() {
    synchronized (this) {
      int num = runBarrier.getParties();
      if(num == 1) {
        return;
      }
      runBarrier = new CyclicBarrier(num - 1);
    }
  }
}
