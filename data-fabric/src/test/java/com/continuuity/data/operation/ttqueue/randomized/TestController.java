package com.continuuity.data.operation.ttqueue.randomized;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
*
*/
public class TestController {
  private static final Logger LOG = LoggerFactory.getLogger(TestController.class);
  private final CountDownLatch startLatch = new CountDownLatch(1);
  private volatile long enqueueDoneTime = 0;

  public void waitToStart() throws Exception {
    startLatch.await();
  }

  public void startTest() {
    startLatch.countDown();
  }

  public void setEnqueueDoneTime(long enqueueDoneTime) {
    LOG.info(String.format("setEnqueueDoneTime: enqueueDoneTime=%s", enqueueDoneTime));
    this.enqueueDoneTime = enqueueDoneTime;
  }

  public boolean canDequeueStop() {
    // Wait for a few seconds after enqueue is done for things to settle down
    if (enqueueDoneTime > 0 && System.currentTimeMillis() > enqueueDoneTime + 5000) {
      LOG.info(String.format("canDequeueStop: enqueueDoneTime=%s", enqueueDoneTime));
      return true;
    }
    return false;
  }
}
