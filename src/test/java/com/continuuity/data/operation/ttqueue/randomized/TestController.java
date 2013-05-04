package com.continuuity.data.operation.ttqueue.randomized;

import java.util.concurrent.CountDownLatch;

/**
*
*/
public class TestController {
  private final CountDownLatch startLatch = new CountDownLatch(1);
  private volatile long enqueueDoneTime = 0;

  public void waitToStart() throws Exception {
    startLatch.await();
  }

  public void startTest() {
    startLatch.countDown();
  }

  public void setEnqueueDoneTime(long enqueueDoneTime) {
    this.enqueueDoneTime = enqueueDoneTime;
  }

  public boolean canDequeueStop() {
    // Wait for a few seconds after enqueue is done for things to settle down
    return enqueueDoneTime > 0 && System.currentTimeMillis() > enqueueDoneTime + 5000;
  }
}
