package com.continuuity.data.operation.ttqueue.randomized;

import com.google.common.collect.Sets;

import java.util.Set;

/**
*
*/
public class ConsumerControl {
  private final int id;
  private final int size;
  private final int numDequeueRuns;
  private final Set<Integer> consumersAtQueueEnd = Sets.newCopyOnWriteArraySet();

  public ConsumerControl(int id, int size, int numDequeueRuns) {
    this.id = id;
    this.size = size;
    this.numDequeueRuns = numDequeueRuns;
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
}
