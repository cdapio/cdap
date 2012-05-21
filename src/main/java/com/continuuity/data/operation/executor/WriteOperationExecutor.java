package com.continuuity.data.operation.executor;

import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.OrderedWrite;
import com.continuuity.data.operation.ReadModifyWrite;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.queue.QueueAck;
import com.continuuity.data.operation.queue.QueuePush;

public interface WriteOperationExecutor {

  /**
   * Performs a {@link Write} operation.
   */
  public boolean execute(Write write);

  public boolean execute(OrderedWrite write);

  public boolean execute(ReadModifyWrite rmw);

  public boolean execute(Increment inc);

  public boolean execute(CompareAndSwap cas);

  // Queues

  public boolean execute(QueuePush push);

  public boolean execute(QueueAck ack);
}
