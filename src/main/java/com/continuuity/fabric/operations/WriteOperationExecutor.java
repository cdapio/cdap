package com.continuuity.fabric.operations;

import com.continuuity.fabric.operations.impl.CompareAndSwap;
import com.continuuity.fabric.operations.impl.Increment;
import com.continuuity.fabric.operations.impl.OrderedWrite;
import com.continuuity.fabric.operations.impl.QueuePush;
import com.continuuity.fabric.operations.impl.ReadModifyWrite;
import com.continuuity.fabric.operations.impl.Write;

public interface WriteOperationExecutor {

  /**
   * @see SimpleOperationExecutor#execute(WriteOperation[])
   * @see TransactionalOperationExecutor#execute(WriteOperation[])
   */
  public boolean execute(WriteOperation [] writes);

  /**
   * Performs a {@link Write} operation.
   */
  public boolean execute(Write write);

  public boolean execute(OrderedWrite write);
  
  public boolean execute(ReadModifyWrite rmw);

  public boolean execute(QueuePush push);

  public boolean execute(Increment inc);

  public boolean execute(CompareAndSwap cas);
}
