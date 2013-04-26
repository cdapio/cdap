package com.continuuity.data.operation.ttqueue.randomized;

import com.google.common.collect.Sets;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
*
*/
public class ConsumerGroupControl {
  private final int id;
  private final int size;
  private final int numDequeueRuns;
  private final CountDownLatch configureLatch;
  private final Set<Integer> consumersAtQueueEnd = Sets.newCopyOnWriteArraySet();

  public ConsumerGroupControl(int id, int size, int numDequeueRuns) {
    this.id = id;
    this.size = size;
    this.numDequeueRuns = numDequeueRuns;
    this.configureLatch = new CountDownLatch(size);
  }

  public int getId() {
    return id;
  }

  public int getSize() {
    return size;
  }

  public int getNumDequeueRuns() {
    return numDequeueRuns;
  }

  public void setConsumersAtQueueEnd(int consumerId) {
    consumersAtQueueEnd.add(consumerId);
  }

  public Set<Integer> getConsumersAtQueueEnd() {
    return consumersAtQueueEnd;
  }

  public void doneSingleConfigure() {
    configureLatch.countDown();
  }

  public void waitForGroupConfigure() throws Exception {
    configureLatch.await();
  }
}
