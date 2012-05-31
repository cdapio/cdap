package com.continuuity.data.operation.queue;

import com.google.common.base.Objects;

@Deprecated
public class QueueConfig {

  private final QueuePartitioner partitioner;
  private final ExecutionMode execMode;
  
  QueueConfig(QueuePartitioner partitioner, ExecutionMode execMode) {
    this.partitioner = partitioner;
    this.execMode = execMode;
  }

  public QueueConfig(QueuePartitioner partitioner, boolean sync) {
    this(partitioner, sync ? ExecutionMode.SYNC : ExecutionMode.ASYNC);
  }

  public QueuePartitioner getPartitioner() {
    return partitioner;
  }
  
  public boolean isSyncMode() {
    return execMode == ExecutionMode.SYNC;
  }
  
  public boolean isAsyncMode() {
    return execMode == ExecutionMode.ASYNC;
  }
  
  public enum ExecutionMode {
    SYNC, ASYNC
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("execmode", execMode.name())
        .add("partitioner", partitioner)
        .toString();
  }
}
