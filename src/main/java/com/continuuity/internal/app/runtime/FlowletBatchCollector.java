package com.continuuity.internal.app.runtime;

import com.continuuity.data.operation.WriteOperation;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
final class FlowletBatchCollector implements ManagedBatchCollector {

  private final Queue<WriteOperation> writeOperations;
  private final AtomicReference<Queue<WriteOperation>> queueRef;

  public FlowletBatchCollector() {
    writeOperations = new ConcurrentLinkedQueue<WriteOperation>();
    queueRef = new AtomicReference<Queue<WriteOperation>>(writeOperations);
  }

  @Override
  public void add(WriteOperation write) {
    Queue<WriteOperation> queue = queueRef.get();
    Preconditions.checkState(queue != null, "FlowletBatchCollector is captured. No output is allowed until reset.");
    queue.add(write);
  }

  @Override
  public List<WriteOperation> capture() {
    return ImmutableList.copyOf(queueRef.getAndSet(null));
  }

  @Override
  public void reset() {
    writeOperations.clear();
    queueRef.set(writeOperations);
  }
}
