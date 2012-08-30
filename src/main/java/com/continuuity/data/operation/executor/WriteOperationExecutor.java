package com.continuuity.data.operation.executor;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;

public interface WriteOperationExecutor {

  /**
   * Performs a {@link Write} operation.
   * @param write the operation
   * @throws OperationException if execution failed
   */
  public void execute(Write write) throws OperationException;

  /**
   * Performs a {@link Delete} operation.
   * @param delete the operation
   * @throws OperationException if execution failed
   */
  public void execute(Delete delete) throws OperationException;

  /**
   * Performs an {@link Increment} operation.
   * @param inc the operation
   * @throws OperationException if execution failed
   */
  public void execute(Increment inc) throws OperationException;

  /**
   * Performs a {@link CompareAndSwap} operation.
   * @param cas the operation
   * @throws OperationException if execution failed
   */
  public void execute(CompareAndSwap cas) throws OperationException;
  
  /**
   * Performs a {@link QueueEnqueue} operation.
   * @param enqueue the operation
   * @throws OperationException if execution failed
   */
  public void execute(QueueEnqueue enqueue) throws OperationException;

  /**
   * Performs a {@link QueueAck} operation.
   * @param ack the operation
   * @throws OperationException if execution failed
   */
  public void execute(QueueAck ack) throws OperationException;
}
