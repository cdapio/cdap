package com.continuuity.data.operation.executor;

import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Delete;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.OrderedWrite;
import com.continuuity.data.operation.ReadModifyWrite;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueInvalidate;

public interface WriteOperationExecutor {

  /**
   * Performs a {@link Write} operation.
   */
  public boolean execute(Write write);

  public boolean execute(Delete delete);

  public boolean execute(OrderedWrite write);

  public boolean execute(ReadModifyWrite rmw);

  public boolean execute(Increment inc);

  public boolean execute(CompareAndSwap cas);

  // TTQueues

  public boolean execute(QueueEnqueue enqueue);

  public boolean execute(QueueAck ack);

  public boolean execute(QueueInvalidate invalidate);
}
