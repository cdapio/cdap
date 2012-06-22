package com.continuuity.data.operation.executor;

import com.continuuity.api.data.CompareAndSwap;
import com.continuuity.api.data.Delete;
import com.continuuity.api.data.Increment;
import com.continuuity.api.data.Write;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;

public interface WriteOperationExecutor {

  // Key-Values

  /**
   * Performs a {@link Write} operation.
   * @param write
   * @return true if success, false if failure
   */
  public boolean execute(Write write);

  /**
   * Performs a {@link Delete} operation.
   * @param delete
   * @return true if success, false if failure
   */
  public boolean execute(Delete delete);

  /**
   * Performs an {@link Increment} operation.
   * @param inc
   * @return true if success, false if failure
   */
  public boolean execute(Increment inc);

  /**
   * Performs a {@link CompareAndSwap} operation.
   * @param cas
   * @return true if success, false if failure
   */
  public boolean execute(CompareAndSwap cas);
  
  // TTQueues

  /**
   * Performs a {@link QueueEnqueue} operation.
   * @param enqueue
   * @return true if success, false if failure
   */
  public boolean execute(QueueEnqueue enqueue);

  /**
   * Performs a {@link QueueAck} operation.
   * @param ack
   * @return true if success, false if failure
   */
  public boolean execute(QueueAck ack);
}
