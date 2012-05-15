package com.continuuity.fabric.operations;

import com.continuuity.fabric.operations.impl.CompareAndSwap;
import com.continuuity.fabric.operations.impl.Increment;
import com.continuuity.fabric.operations.impl.OrderedWrite;
import com.continuuity.fabric.operations.impl.ReadModifyWrite;
import com.continuuity.fabric.operations.impl.Write;
import com.continuuity.fabric.operations.queues.QueueAck;
import com.continuuity.fabric.operations.queues.QueuePush;

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
